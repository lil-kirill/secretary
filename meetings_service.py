"""
Бизнес-логика встреч: синхронизация транскриптов Fireflies, форматирование.
"""
import json
import logging
import uuid
from datetime import datetime
from html import escape

import db
import fireflies_client

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
#  СИНХРОНИЗАЦИЯ С FIREFLIES
# ------------------------------------------------------------------
async def sync_transcripts_for_meetings() -> list[dict]:
    """
    Ищет транскрипты в Fireflies для встреч с transcribe_enabled=1.
    Сопоставляет по названию/времени. Возвращает список обновлённых встреч.
    """
    if not fireflies_client.is_configured():
        return []

    # Встречи, ожидающие транскрипт
    waiting = await db.get_meetings_to_transcribe()
    if not waiting:
        return []

    # Получаем список транскриптов из Fireflies
    remote = await fireflies_client.get_recent_transcripts(limit=30)
    if not remote:
        return []

    updated = []

    for meeting in waiting:
        match = _find_matching_transcript(meeting, remote)
        if not match:
            continue

        ext_id = match.get("id")
        if not ext_id:
            continue

        # Получаем полные данные транскрипта
        details = await fireflies_client.get_transcript(ext_id)
        if not details:
            continue

        # Сохраняем транскрипт
        t_id = uuid.uuid4().hex[:12]
        await db.save_meeting_transcript(
            transcript_id=t_id,
            meeting_id=meeting["id"],
            transcript_text=details.get("full_text", ""),
            sentences_json=json.dumps(details.get("sentences", []), ensure_ascii=False),
            speakers_json=json.dumps(details.get("speakers", []), ensure_ascii=False),
            attendees_json=json.dumps(details.get("attendees", []), ensure_ascii=False),
        )

        # Сохраняем саммари
        summary = details.get("summary", {})
        s_id = uuid.uuid4().hex[:12]
        await db.save_meeting_summary(
            summary_id=s_id,
            meeting_id=meeting["id"],
            overview=summary.get("overview", ""),
            short_summary=summary.get("overview", "")[:200],
            action_items_json=json.dumps(summary.get("action_items", []), ensure_ascii=False),
            outline_json=json.dumps(summary.get("outline", []), ensure_ascii=False),
            topics_json=json.dumps(summary.get("topics_discussed", []), ensure_ascii=False),
            keywords_json=json.dumps(summary.get("keywords", []), ensure_ascii=False),
        )

        # Обновляем встречу
        await db.link_transcript_to_meeting(
            meeting_id=meeting["id"],
            external_transcript_id=ext_id,
            duration=details.get("duration"),
            transcript_url=details.get("transcript_url"),
            audio_url=details.get("audio_url"),
            video_url=details.get("video_url"),
        )

        meeting["status"] = "transcribed"
        meeting["overview"] = summary.get("overview", "")
        meeting["action_items"] = summary.get("action_items", [])
        updated.append(meeting)
        logger.info(f"Транскрипт привязан к встрече {meeting['id']} ← {ext_id}")

    return updated


def _find_matching_transcript(meeting: dict, remote_transcripts: list[dict]) -> dict | None:
    """Сопоставляет встречу с транскриптом по названию (fuzzy)."""
    meeting_topic = (meeting.get("topic") or "").lower().strip()
    if not meeting_topic:
        return None

    for t in remote_transcripts:
        t_title = (t.get("title") or "").lower().strip()
        # Точное совпадение или одно содержит другое
        if meeting_topic in t_title or t_title in meeting_topic:
            return t
        # Совпадение по ключевым словам (>50% слов)
        m_words = set(meeting_topic.split())
        t_words = set(t_title.split())
        if m_words and t_words:
            overlap = len(m_words & t_words) / max(len(m_words), len(t_words))
            if overlap > 0.5:
                return t

    return None


# ------------------------------------------------------------------
#  ФОРМАТИРОВАНИЕ
# ------------------------------------------------------------------

STATUS_ICONS = {
    "scheduled": "🟢",
    "reminded": "🔔",
    "completed": "⏳" ,
    "transcribed": "✅",
}


def format_meeting_list_item(m: dict, index: int) -> str:
    """Одна строка в списке встреч."""
    icon = STATUS_ICONS.get(m.get("status", "scheduled"), "⚪")
    title = escape(m.get("topic", "Без названия"))

    date_str = ""
    if m.get("datetime"):
        try:
            dt = datetime.fromisoformat(m["datetime"])
            date_str = dt.strftime("%d.%m, %H:%M")
        except (ValueError, TypeError):
            pass

    duration_str = ""
    if m.get("duration"):
        mins = m["duration"] // 60
        duration_str = f" · {mins} мин"

    status_hint = ""
    if m.get("status") == "completed" and m.get("transcribe_enabled"):
        status_hint = " (ждём транскрипт)"
    elif m.get("status") == "transcribed":
        status_hint = " (транскрипт готов)"

    return f"{index}. {icon} <b>{title}</b> · {date_str}{duration_str}{status_hint}"


def format_meeting_detail(m: dict, summary: dict | None, transcript: dict | None) -> str:
    """Полная карточка встречи."""
    title = escape(m.get("topic", "Без названия"))
    icon = STATUS_ICONS.get(m.get("status", "scheduled"), "⚪")
    lines = [f"📅 <b>{title}</b>", f"{icon} {_status_label(m.get('status', 'scheduled'))}"]

    # Дата + длительность
    if m.get("datetime"):
        try:
            dt = datetime.fromisoformat(m["datetime"])
            d_str = dt.strftime("%d.%m.%Y, %H:%M")
        except (ValueError, TypeError):
            d_str = m.get("datetime_str", "")
        dur = f" · {m['duration'] // 60} мин" if m.get("duration") else ""
        lines.append(f"📆 {d_str}{dur}")

    # Zoom-данные
    zoom_parts = []
    if m.get("zoom_link"):
        zoom_parts.append(f'<a href="{m["zoom_link"]}">🔗 Zoom</a>')
    if m.get("meeting_id"):
        zoom_parts.append(f"🆔 {m['meeting_id']}")
    if m.get("passcode"):
        zoom_parts.append(f"🔑 {m['passcode']}")
    if zoom_parts:
        lines.append(" · ".join(zoom_parts))

    # Транскрибация
    if m.get("transcribe_enabled"):
        lines.append("\n📹 Транскрибация: включена")

    # --- Данные от Fireflies ---
    if summary and summary.get("overview"):
        overview = escape(summary["overview"][:500])
        lines.append(f"\n📝 <b>Обзор:</b>\n{overview}")

    if summary and summary.get("action_items_json"):
        try:
            items = json.loads(summary["action_items_json"])
            if items:
                lines.append("\n📋 <b>Действия:</b>")
                for item in items[:10]:
                    lines.append(f"  • {escape(str(item))[:150]}")
        except (json.JSONDecodeError, TypeError):
            pass

    if summary and summary.get("topics_json"):
        try:
            topics = json.loads(summary["topics_json"])
            if topics:
                lines.append(f"\n🏷 <b>Темы:</b> {', '.join(escape(str(t))[:50] for t in topics[:5])}")
        except (json.JSONDecodeError, TypeError):
            pass

    if transcript and transcript.get("speakers_json"):
        try:
            speakers = json.loads(transcript["speakers_json"])
            if speakers:
                names = ", ".join(escape(s.get("name", "?")) for s in speakers[:10])
                lines.append(f"\n🗣 <b>Участники:</b> {names}")
        except (json.JSONDecodeError, TypeError):
            pass

    if m.get("transcript_url"):
        lines.append(f'\n🔗 <a href="{m["transcript_url"]}">Полный транскрипт на Fireflies</a>')

    return "\n".join(lines)


def format_transcript_notification(m: dict) -> str:
    """Уведомление о готовом транскрипте."""
    title = escape(m.get("topic", "Без названия"))
    lines = [f"✅ <b>Транскрипт готов!</b>\n\n<b>{title}</b>"]

    if m.get("duration"):
        lines.append(f"⏱ {m['duration'] // 60} мин")

    if m.get("overview"):
        lines.append(f"\n📝 {escape(m['overview'][:300])}")

    if m.get("action_items"):
        items = m["action_items"]
        if items:
            lines.append("\n📋 <b>Действия:</b>")
            for item in items[:5]:
                lines.append(f"  • {escape(str(item))[:100]}")

    return "\n".join(lines)


def _status_label(status: str) -> str:
    labels = {
        "scheduled": "Запланирована",
        "reminded": "Напомнил",
        "completed": "Завершена",
        "transcribed": "Транскрипт готов",
    }
    return labels.get(status, status)
