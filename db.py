"""
Асинхронный слой доступа к SQLite (aiosqlite).
Таблицы: users, meetings, raw_messages, summary_jobs, summaries.
"""
import aiosqlite
import logging
from pathlib import Path

DB_PATH = Path(__file__).parent / "bot.db"
logger = logging.getLogger(__name__)

_db: aiosqlite.Connection | None = None


async def init():
    """Открывает соединение и создаёт таблицы, если их нет."""
    global _db
    _db = await aiosqlite.connect(DB_PATH)
    _db.row_factory = aiosqlite.Row  # результат как dict-like

    await _db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id         INTEGER PRIMARY KEY,
            username        TEXT    NOT NULL DEFAULT '',
            first_name      TEXT    NOT NULL DEFAULT '',
            private_chat_id INTEGER,
            tracking_enabled INTEGER NOT NULL DEFAULT 0,
            sla              INTEGER NOT NULL DEFAULT 60,
            repeat_interval  INTEGER NOT NULL DEFAULT 120,
            zoom_remind_before INTEGER NOT NULL DEFAULT 900
        );

        CREATE TABLE IF NOT EXISTS meetings (
            id                      TEXT    PRIMARY KEY,
            user_id                 INTEGER NOT NULL,
            topic                   TEXT    NOT NULL DEFAULT 'Zoom-конференция',
            datetime                TEXT,
            datetime_str            TEXT,
            zoom_link               TEXT,
            meeting_id              TEXT,
            passcode                TEXT,
            remind_before           INTEGER NOT NULL DEFAULT 900,
            reminded                INTEGER NOT NULL DEFAULT 0,
            started_notified        INTEGER NOT NULL DEFAULT 0,
            status                  TEXT    NOT NULL DEFAULT 'scheduled',
            transcribe_enabled      INTEGER NOT NULL DEFAULT 0,
            duration                INTEGER,
            external_transcript_id  TEXT,
            transcript_url          TEXT,
            audio_url               TEXT,
            video_url               TEXT,
            created_at              TEXT    NOT NULL DEFAULT '',
            updated_at              TEXT    NOT NULL DEFAULT '',
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_meetings_user   ON meetings(user_id);
        CREATE INDEX IF NOT EXISTS idx_meetings_dt     ON meetings(datetime);
        CREATE INDEX IF NOT EXISTS idx_meetings_status ON meetings(status);
        CREATE INDEX IF NOT EXISTS idx_meetings_ext_id ON meetings(external_transcript_id);

        -- ============================================================
        --  ТРАНСКРИПТЫ И САММАРИ ВСТРЕЧ (Fireflies)
        -- ============================================================
        CREATE TABLE IF NOT EXISTS meeting_transcripts (
            id              TEXT PRIMARY KEY,
            meeting_id      TEXT NOT NULL UNIQUE,
            transcript_text TEXT NOT NULL DEFAULT '',
            sentences_json  TEXT NOT NULL DEFAULT '[]',
            speakers_json   TEXT NOT NULL DEFAULT '[]',
            attendees_json  TEXT NOT NULL DEFAULT '[]',
            created_at      TEXT NOT NULL,
            FOREIGN KEY (meeting_id) REFERENCES meetings(id)
        );

        CREATE TABLE IF NOT EXISTS meeting_summaries (
            id                  TEXT PRIMARY KEY,
            meeting_id          TEXT NOT NULL UNIQUE,
            overview            TEXT NOT NULL DEFAULT '',
            short_summary       TEXT NOT NULL DEFAULT '',
            action_items_json   TEXT NOT NULL DEFAULT '[]',
            outline_json        TEXT NOT NULL DEFAULT '[]',
            topics_json         TEXT NOT NULL DEFAULT '[]',
            keywords_json       TEXT NOT NULL DEFAULT '[]',
            created_at          TEXT NOT NULL,
            FOREIGN KEY (meeting_id) REFERENCES meetings(id)
        );

        -- ============================================================
        --  САММАРИЗАЦИЯ
        -- ============================================================

        CREATE TABLE IF NOT EXISTS raw_messages (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id             INTEGER NOT NULL,
            message_id          INTEGER NOT NULL,
            message_thread_id   INTEGER,
            from_user_id        INTEGER NOT NULL,
            username            TEXT    NOT NULL DEFAULT '',
            first_name          TEXT    NOT NULL DEFAULT '',
            date                TEXT    NOT NULL,
            text                TEXT    NOT NULL DEFAULT '',
            reply_to_message_id INTEGER,
            has_media           INTEGER NOT NULL DEFAULT 0,
            UNIQUE(chat_id, message_id)
        );

        CREATE INDEX IF NOT EXISTS idx_raw_chat_date
            ON raw_messages(chat_id, date);
        CREATE INDEX IF NOT EXISTS idx_raw_chat_thread
            ON raw_messages(chat_id, message_thread_id);

        CREATE TABLE IF NOT EXISTS summary_jobs (
            id                  TEXT    PRIMARY KEY,
            type                TEXT    NOT NULL,
            chat_id             INTEGER NOT NULL,
            message_thread_id   INTEGER,
            initiator_user_id   INTEGER NOT NULL,
            source_message_id   INTEGER,
            window_from         TEXT,
            window_to           TEXT,
            msg_count           INTEGER,
            status              TEXT    NOT NULL DEFAULT 'pending',
            error               TEXT,
            created_at          TEXT    NOT NULL,
            updated_at          TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS summaries (
            id                  TEXT PRIMARY KEY,
            job_id              TEXT NOT NULL,
            chat_id             INTEGER NOT NULL,
            message_thread_id   INTEGER,
            summary_json        TEXT    NOT NULL DEFAULT '{}',
            raw_text            TEXT    NOT NULL DEFAULT '',
            language            TEXT    NOT NULL DEFAULT 'ru',
            created_at          TEXT    NOT NULL,
            FOREIGN KEY (job_id) REFERENCES summary_jobs(id)
        );

        CREATE INDEX IF NOT EXISTS idx_summaries_job  ON summaries(job_id);
        CREATE INDEX IF NOT EXISTS idx_summaries_chat ON summaries(chat_id, created_at);

        -- ============================================================
        --  КЭШ ГРУПП (для меню в ЛС)
        -- ============================================================
        CREATE TABLE IF NOT EXISTS chats (
            chat_id     INTEGER PRIMARY KEY,
            title       TEXT    NOT NULL DEFAULT '',
            username    TEXT,
            updated_at  TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_topics (
            chat_id     INTEGER NOT NULL,
            thread_id   INTEGER NOT NULL,
            name        TEXT    NOT NULL DEFAULT '',
            updated_at  TEXT    NOT NULL,
            PRIMARY KEY (chat_id, thread_id)
        );

        -- ============================================================
        --  ОТСЛЕЖИВАНИЕ УЧАСТНИКОВ И КОНФЕРЕНЦ-ССЫЛОК
        -- ============================================================
        CREATE TABLE IF NOT EXISTS chat_memberships (
            chat_id      INTEGER NOT NULL,
            user_id      INTEGER NOT NULL,
            username     TEXT    NOT NULL DEFAULT '',
            first_name   TEXT    NOT NULL DEFAULT '',
            last_seen_at TEXT    NOT NULL,
            PRIMARY KEY (chat_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS conference_offers (
            id                 TEXT    PRIMARY KEY,
            chat_id            INTEGER NOT NULL,
            source_message_id  INTEGER NOT NULL,
            sender_user_id     INTEGER NOT NULL,
            user_id            INTEGER NOT NULL,
            provider           TEXT    NOT NULL,
            normalized_url     TEXT    NOT NULL,
            original_url       TEXT    NOT NULL,
            status             TEXT    NOT NULL DEFAULT 'pending',
            offered_at         TEXT    NOT NULL,
            responded_at       TEXT,
            created_meeting_id TEXT,
            UNIQUE(normalized_url, user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_conf_offers_user
            ON conference_offers(user_id, status);
    """)
    await _db.commit()
    logger.info(f"БД инициализирована: {DB_PATH}")


async def close():
    global _db
    if _db:
        await _db.close()
        _db = None


# ------------------------------------------------------------------
#  USERS
# ------------------------------------------------------------------
def _row_to_user(row: aiosqlite.Row) -> dict:
    return {
        "user_id": row["user_id"],
        "username": row["username"],
        "first_name": row["first_name"],
        "private_chat_id": row["private_chat_id"],
        "tracking_enabled": bool(row["tracking_enabled"]),
        "sla": row["sla"],
        "repeat_interval": row["repeat_interval"],
        "zoom_remind_before": row["zoom_remind_before"],
    }


async def get_user_by_id(user_id: int) -> dict | None:
    async with _db.execute(
        "SELECT * FROM users WHERE user_id = ?", (user_id,)
    ) as cur:
        row = await cur.fetchone()
        return _row_to_user(row) if row else None


async def get_user_by_username(username: str) -> dict | None:
    async with _db.execute(
        "SELECT * FROM users WHERE LOWER(username) = LOWER(?)", (username,)
    ) as cur:
        row = await cur.fetchone()
        return _row_to_user(row) if row else None


async def upsert_user(
    user_id: int,
    username: str = "",
    first_name: str = "",
    private_chat_id: int | None = None,
    tracking_enabled: bool = False,
    sla: int = 60,
    repeat_interval: int = 120,
    zoom_remind_before: int = 900,
):
    await _db.execute(
        """
        INSERT INTO users
            (user_id, username, first_name, private_chat_id,
             tracking_enabled, sla, repeat_interval, zoom_remind_before)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username         = excluded.username,
            first_name       = excluded.first_name,
            private_chat_id  = excluded.private_chat_id,
            tracking_enabled = excluded.tracking_enabled,
            sla              = excluded.sla,
            repeat_interval  = excluded.repeat_interval,
            zoom_remind_before = excluded.zoom_remind_before
        """,
        (user_id, username, first_name, private_chat_id,
         int(tracking_enabled), sla, repeat_interval, zoom_remind_before),
    )
    await _db.commit()


async def update_user_field(user_id: int, field: str, value):
    allowed = {
        "tracking_enabled", "sla", "repeat_interval",
        "zoom_remind_before", "private_chat_id", "username", "first_name",
    }
    if field not in allowed:
        raise ValueError(f"Недопустимое поле: {field}")
    if field == "tracking_enabled":
        value = int(value)
    await _db.execute(
        f"UPDATE users SET {field} = ? WHERE user_id = ?", (value, user_id)
    )
    await _db.commit()


# ------------------------------------------------------------------
#  MEETINGS
# ------------------------------------------------------------------
def _row_to_meeting(row: aiosqlite.Row) -> dict:
    return {
        "id": row["id"],
        "user_id": row["user_id"],
        "topic": row["topic"],
        "datetime": row["datetime"],
        "datetime_str": row["datetime_str"],
        "zoom_link": row["zoom_link"],
        "meeting_id": row["meeting_id"],
        "passcode": row["passcode"],
        "remind_before": row["remind_before"],
        "reminded": bool(row["reminded"]),
        "started_notified": bool(row["started_notified"]),
        "status": row["status"],
        "transcribe_enabled": bool(row["transcribe_enabled"]),
        "duration": row["duration"],
        "external_transcript_id": row["external_transcript_id"],
        "transcript_url": row["transcript_url"],
        "audio_url": row["audio_url"],
        "video_url": row["video_url"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


async def add_meeting(
    meeting_id: str,
    user_id: int,
    topic: str = "Zoom-конференция",
    dt: str | None = None,
    datetime_str: str | None = None,
    zoom_link: str | None = None,
    mid: str | None = None,
    passcode: str | None = None,
    remind_before: int = 900,
    transcribe_enabled: bool = False,
):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """
        INSERT INTO meetings
            (id, user_id, topic, datetime, datetime_str,
             zoom_link, meeting_id, passcode, remind_before,
             transcribe_enabled, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'scheduled', ?, ?)
        """,
        (meeting_id, user_id, topic, dt, datetime_str,
         zoom_link, mid, passcode, remind_before,
         int(transcribe_enabled), now, now),
    )
    await _db.commit()


async def get_user_meetings(user_id: int) -> list[dict]:
    async with _db.execute(
        "SELECT * FROM meetings WHERE user_id = ? ORDER BY datetime", (user_id,)
    ) as cur:
        rows = await cur.fetchall()
        return [_row_to_meeting(r) for r in rows]


async def get_all_pending_meetings() -> list[dict]:
    """Созвоны, по которым ещё не отправлены все уведомления."""
    async with _db.execute(
        """
        SELECT * FROM meetings
        WHERE datetime IS NOT NULL
          AND (reminded = 0 OR started_notified = 0)
        """
    ) as cur:
        rows = await cur.fetchall()
        return [_row_to_meeting(r) for r in rows]


async def update_meeting_flags(meeting_id: str, reminded: bool, started_notified: bool):
    await _db.execute(
        "UPDATE meetings SET reminded = ?, started_notified = ? WHERE id = ?",
        (int(reminded), int(started_notified), meeting_id),
    )
    await _db.commit()


async def delete_meeting(meeting_id: str):
    await _db.execute("DELETE FROM meetings WHERE id = ?", (meeting_id,))
    await _db.commit()


async def delete_old_meetings(cutoff_iso: str):
    """Удаляет завершённые встречи старше cutoff (не удаляет с транскриптами)."""
    await _db.execute(
        """DELETE FROM meetings
        WHERE datetime IS NOT NULL AND datetime < ?
          AND status IN ('scheduled', 'reminded', 'completed')
          AND transcribe_enabled = 0""",
        (cutoff_iso,),
    )
    await _db.commit()


async def update_meeting_status(meeting_id: str, status: str):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        "UPDATE meetings SET status = ?, updated_at = ? WHERE id = ?",
        (status, now, meeting_id),
    )
    await _db.commit()


async def set_meeting_transcribe(meeting_id: str, enabled: bool):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        "UPDATE meetings SET transcribe_enabled = ?, updated_at = ? WHERE id = ?",
        (int(enabled), now, meeting_id),
    )
    await _db.commit()


async def get_meetings_to_transcribe() -> list[dict]:
    """Встречи с включённой транскрибацией, которые прошли, но ещё без транскрипта."""
    async with _db.execute(
        """
        SELECT * FROM meetings
        WHERE transcribe_enabled = 1
          AND external_transcript_id IS NULL
          AND status IN ('completed', 'reminded', 'scheduled')
        """
    ) as cur:
        rows = await cur.fetchall()
    return [_row_to_meeting(r) for r in rows]


async def link_transcript_to_meeting(
    meeting_id: str,
    external_transcript_id: str,
    duration: int | None = None,
    transcript_url: str | None = None,
    audio_url: str | None = None,
    video_url: str | None = None,
):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """UPDATE meetings SET
            external_transcript_id = ?, duration = ?,
            transcript_url = ?, audio_url = ?, video_url = ?,
            status = 'transcribed', updated_at = ?
        WHERE id = ?""",
        (external_transcript_id, duration,
         transcript_url, audio_url, video_url, now, meeting_id),
    )
    await _db.commit()


# ------------------------------------------------------------------
#  MEETING TRANSCRIPTS & SUMMARIES
# ------------------------------------------------------------------
async def save_meeting_transcript(
    transcript_id: str,
    meeting_id: str,
    transcript_text: str = "",
    sentences_json: str = "[]",
    speakers_json: str = "[]",
    attendees_json: str = "[]",
):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """INSERT OR REPLACE INTO meeting_transcripts
            (id, meeting_id, transcript_text, sentences_json,
             speakers_json, attendees_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (transcript_id, meeting_id, transcript_text,
         sentences_json, speakers_json, attendees_json, now),
    )
    await _db.commit()


async def get_meeting_transcript(meeting_id: str) -> dict | None:
    async with _db.execute(
        "SELECT * FROM meeting_transcripts WHERE meeting_id = ?", (meeting_id,)
    ) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


async def save_meeting_summary(
    summary_id: str,
    meeting_id: str,
    overview: str = "",
    short_summary: str = "",
    action_items_json: str = "[]",
    outline_json: str = "[]",
    topics_json: str = "[]",
    keywords_json: str = "[]",
):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """INSERT OR REPLACE INTO meeting_summaries
            (id, meeting_id, overview, short_summary,
             action_items_json, outline_json, topics_json, keywords_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (summary_id, meeting_id, overview, short_summary,
         action_items_json, outline_json, topics_json, keywords_json, now),
    )
    await _db.commit()


async def get_meeting_summary(meeting_id: str) -> dict | None:
    async with _db.execute(
        "SELECT * FROM meeting_summaries WHERE meeting_id = ?", (meeting_id,)
    ) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


# ------------------------------------------------------------------
#  RAW MESSAGES
# ------------------------------------------------------------------
async def save_raw_message(
    chat_id: int,
    message_id: int,
    from_user_id: int,
    username: str,
    first_name: str,
    date: str,
    text: str,
    message_thread_id: int | None = None,
    reply_to_message_id: int | None = None,
    has_media: bool = False,
):
    """Сохраняет сообщение. Дубли по (chat_id, message_id) игнорируются."""
    await _db.execute(
        """
        INSERT OR IGNORE INTO raw_messages
            (chat_id, message_id, message_thread_id, from_user_id,
             username, first_name, date, text,
             reply_to_message_id, has_media)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (chat_id, message_id, message_thread_id, from_user_id,
         username, first_name, date, text,
         reply_to_message_id, int(has_media)),
    )
    await _db.commit()


async def get_chat_topics(chat_id: int) -> list[dict]:
    """
    Возвращает список тем/тредов в группе с кол-вом сообщений.
    Названия берутся из таблицы chat_topics.
    """
    async with _db.execute(
        """
        SELECT
            rm.message_thread_id,
            COUNT(*) as msg_count,
            MAX(rm.date) as last_date,
            ct.name as topic_name
        FROM raw_messages rm
        LEFT JOIN chat_topics ct
            ON ct.chat_id = rm.chat_id AND ct.thread_id = rm.message_thread_id
        WHERE rm.chat_id = ? AND rm.message_thread_id IS NOT NULL
        GROUP BY rm.message_thread_id
        ORDER BY MAX(rm.date) DESC
        """,
        (chat_id,),
    ) as cur:
        rows = await cur.fetchall()

    topics = []
    for row in rows:
        title = row["topic_name"] or f"Тема #{row['message_thread_id']}"
        topics.append({
            "thread_id": row["message_thread_id"],
            "title": title,
            "msg_count": row["msg_count"],
            "last_date": row["last_date"],
        })

    return topics


async def get_last_messages(
    chat_id: int, limit: int = 50, message_thread_id: int | None = None
) -> list[dict]:
    """Последние N сообщений чата (или конкретной темы), от старых к новым."""
    if message_thread_id is not None:
        query = """
            SELECT * FROM raw_messages
            WHERE chat_id = ? AND message_thread_id = ?
            ORDER BY date DESC, message_id DESC
            LIMIT ?
        """
        params = (chat_id, message_thread_id, limit)
    else:
        query = """
            SELECT * FROM raw_messages
            WHERE chat_id = ?
            ORDER BY date DESC, message_id DESC
            LIMIT ?
        """
        params = (chat_id, limit)
    async with _db.execute(query, params) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in reversed(rows)]


async def get_thread_messages(
    chat_id: int, message_thread_id: int, limit: int = 100
) -> list[dict]:
    """Сообщения из конкретного треда/топика."""
    async with _db.execute(
        """
        SELECT * FROM raw_messages
        WHERE chat_id = ? AND message_thread_id = ?
        ORDER BY date DESC, message_id DESC
        LIMIT ?
        """,
        (chat_id, message_thread_id, limit),
    ) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in reversed(rows)]


async def get_reply_chain(chat_id: int, message_id: int, max_depth: int = 30) -> list[dict]:
    """Рекурсивно собирает цепочку reply-сообщений."""
    chain = []
    current_id = message_id
    for _ in range(max_depth):
        async with _db.execute(
            "SELECT * FROM raw_messages WHERE chat_id = ? AND message_id = ?",
            (chat_id, current_id),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            break
        chain.append(dict(row))
        if not row["reply_to_message_id"]:
            break
        current_id = row["reply_to_message_id"]
    chain.reverse()
    return chain


async def get_messages_around(
    chat_id: int, message_id: int, window: int = 10
) -> list[dict]:
    """Сообщения вокруг конкретного message_id (window до + window после)."""
    async with _db.execute(
        """
        SELECT * FROM raw_messages
        WHERE chat_id = ? AND message_id BETWEEN ? AND ?
        ORDER BY message_id
        """,
        (chat_id, message_id - window, message_id + window),
    ) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def get_messages_today(
    chat_id: int, message_thread_id: int | None = None
) -> list[dict]:
    """Все сообщения чата (или конкретной темы) за сегодня."""
    from datetime import date as dt_date
    today = dt_date.today().isoformat()
    if message_thread_id is not None:
        query = """
            SELECT * FROM raw_messages
            WHERE chat_id = ? AND message_thread_id = ? AND date >= ?
            ORDER BY date, message_id
        """
        params = (chat_id, message_thread_id, today)
    else:
        query = """
            SELECT * FROM raw_messages
            WHERE chat_id = ? AND date >= ?
            ORDER BY date, message_id
        """
        params = (chat_id, today)
    async with _db.execute(query, params) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def delete_old_messages(days: int = 30):
    """Удаляет сообщения старше N дней."""
    from datetime import date as dt_date, timedelta
    cutoff = (dt_date.today() - timedelta(days=days)).isoformat()
    await _db.execute(
        "DELETE FROM raw_messages WHERE date < ?", (cutoff,)
    )
    await _db.commit()


# ------------------------------------------------------------------
#  SUMMARY JOBS & SUMMARIES
# ------------------------------------------------------------------
async def create_summary_job(
    job_id: str,
    job_type: str,
    chat_id: int,
    initiator_user_id: int,
    msg_count: int = 0,
    message_thread_id: int | None = None,
    source_message_id: int | None = None,
    window_from: str | None = None,
    window_to: str | None = None,
) -> dict:
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """
        INSERT INTO summary_jobs
            (id, type, chat_id, message_thread_id, initiator_user_id,
             source_message_id, window_from, window_to, msg_count,
             status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        """,
        (job_id, job_type, chat_id, message_thread_id,
         initiator_user_id, source_message_id,
         window_from, window_to, msg_count, now, now),
    )
    await _db.commit()
    return {"id": job_id, "status": "pending"}


async def update_job_status(job_id: str, status: str, error: str | None = None):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        "UPDATE summary_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ?",
        (status, error, now, job_id),
    )
    await _db.commit()


# ------------------------------------------------------------------
#  CHATS (кэш названий групп)
# ------------------------------------------------------------------
async def upsert_chat(chat_id: int, title: str, username: str | None = None):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """
        INSERT INTO chats (chat_id, title, username, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET
            title = excluded.title,
            username = excluded.username,
            updated_at = excluded.updated_at
        """,
        (chat_id, title, username, now),
    )
    await _db.commit()


async def get_user_chats(user_id: int) -> list[dict]:
    """Группы, в которых пользователь состоит (по membership) И есть сообщения."""
    async with _db.execute(
        """
        SELECT DISTINCT c.chat_id, c.title, c.username
        FROM chat_memberships cm
        JOIN chats c ON c.chat_id = cm.chat_id
        WHERE cm.user_id = ?
          AND EXISTS (
              SELECT 1 FROM raw_messages rm WHERE rm.chat_id = cm.chat_id
          )
        ORDER BY c.title
        """,
        (user_id,),
    ) as cur:
        rows = await cur.fetchall()
    return [{"chat_id": r["chat_id"], "title": r["title"], "username": r["username"]} for r in rows]


async def upsert_topic(chat_id: int, thread_id: int, name: str):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """
        INSERT INTO chat_topics (chat_id, thread_id, name, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(chat_id, thread_id) DO UPDATE SET
            name = excluded.name,
            updated_at = excluded.updated_at
        """,
        (chat_id, thread_id, name, now),
    )
    await _db.commit()


async def get_topic_name(chat_id: int, thread_id: int) -> str | None:
    async with _db.execute(
        "SELECT name FROM chat_topics WHERE chat_id = ? AND thread_id = ?",
        (chat_id, thread_id),
    ) as cur:
        row = await cur.fetchone()
    return row["name"] if row else None


async def get_chat_title(chat_id: int) -> str:
    async with _db.execute(
        "SELECT title FROM chats WHERE chat_id = ?", (chat_id,)
    ) as cur:
        row = await cur.fetchone()
    return row["title"] if row else f"Чат {chat_id}"


# ------------------------------------------------------------------
#  CHAT MEMBERSHIPS
# ------------------------------------------------------------------
async def upsert_membership(chat_id: int, user_id: int, username: str = "", first_name: str = ""):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """
        INSERT INTO chat_memberships (chat_id, user_id, username, first_name, last_seen_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(chat_id, user_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_seen_at = excluded.last_seen_at
        """,
        (chat_id, user_id, username, first_name, now),
    )
    await _db.commit()


async def get_chat_members_with_private_chat(chat_id: int) -> list[dict]:
    """Зарегистрированные пользователи в этом чате, у которых есть private_chat_id."""
    async with _db.execute(
        """
        SELECT cm.user_id, cm.username, cm.first_name, u.private_chat_id
        FROM chat_memberships cm
        JOIN users u ON u.user_id = cm.user_id
        WHERE cm.chat_id = ? AND u.private_chat_id IS NOT NULL
        """,
        (chat_id,),
    ) as cur:
        rows = await cur.fetchall()
    return [
        {"user_id": r["user_id"], "username": r["username"],
         "first_name": r["first_name"], "private_chat_id": r["private_chat_id"]}
        for r in rows
    ]


# ------------------------------------------------------------------
#  CONFERENCE OFFERS
# ------------------------------------------------------------------
async def create_conference_offer(
    offer_id: str,
    chat_id: int,
    source_message_id: int,
    sender_user_id: int,
    user_id: int,
    provider: str,
    normalized_url: str,
    original_url: str,
) -> bool:
    """Создаёт предложение. Возвращает True если создано, False если дубль."""
    now = __import__("datetime").datetime.now().isoformat()
    cursor = await _db.execute(
        """
        INSERT OR IGNORE INTO conference_offers
            (id, chat_id, source_message_id, sender_user_id, user_id,
             provider, normalized_url, original_url, status, offered_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (offer_id, chat_id, source_message_id, sender_user_id,
         user_id, provider, normalized_url, original_url, now),
    )
    await _db.commit()
    return cursor.rowcount > 0


async def get_conference_offer(offer_id: str) -> dict | None:
    async with _db.execute(
        "SELECT * FROM conference_offers WHERE id = ?", (offer_id,)
    ) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


async def update_conference_offer_status(
    offer_id: str, status: str, meeting_id: str | None = None
):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """UPDATE conference_offers
        SET status = ?, responded_at = ?, created_meeting_id = ?
        WHERE id = ?""",
        (status, now, meeting_id, offer_id),
    )
    await _db.commit()


async def save_summary(
    summary_id: str,
    job_id: str,
    chat_id: int,
    summary_json: str,
    raw_text: str = "",
    message_thread_id: int | None = None,
    language: str = "ru",
):
    now = __import__("datetime").datetime.now().isoformat()
    await _db.execute(
        """
        INSERT INTO summaries
            (id, job_id, chat_id, message_thread_id,
             summary_json, raw_text, language, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (summary_id, job_id, chat_id, message_thread_id,
         summary_json, raw_text, language, now),
    )
    await _db.commit()
