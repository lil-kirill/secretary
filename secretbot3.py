import asyncio
import logging
import os
import re
from html import escape as h
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
)
from aiogram.enums import ChatType
from aiogram.filters import Command

import db
import summary_service
from summary_formatter import format_summary_html, format_context_html
from conference_detector import detect_conference_links, format_meeting_id

# ==== НАСТРОЙКИ ====
TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬТЕ_ТОКЕН_СЮДА")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
APPROVAL_REQUIRED = os.getenv("APPROVAL_REQUIRED", "true").lower() in ("true", "1", "yes")
DEFAULT_SLA = 60
DEFAULT_INTERVAL = 120
MIN_SLA = 10
MAX_SLA = 3600
MIN_INTERVAL = 30
MAX_INTERVAL = 3600

SLA_OPTIONS = [30, 60, 120, 300, 600]
INTERVAL_OPTIONS = [60, 120, 300, 600]

# Zoom
DEFAULT_ZOOM_REMIND = 900
ZOOM_REMIND_OPTIONS = [300, 600, 900, 1800, 3600]

MONTHS_MAP = {
    "янв": 1, "фев": 2, "мар": 3, "апр": 4, "мая": 5, "май": 5,
    "июн": 6, "июл": 7, "авг": 8, "сен": 9, "окт": 10, "ноя": 11, "дек": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ====================================================================
#  ПАРСИНГ ZOOM-ИНВАЙТА
# ====================================================================
def _parse_zoom_datetime(raw: str) -> datetime | None:
    raw = raw.strip()
    raw = re.sub(r"\s+[A-Za-zА-Яа-яЁё/()]+$", "", raw)

    day = month = year = hour = minute = None
    ampm = ""

    m = re.match(
        r"(\d{1,2})\s+(\w+)\.?\s+(\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)?",
        raw, re.IGNORECASE,
    )
    if m:
        day = int(m.group(1))
        month_str = m.group(2).lower().rstrip(".")
        year = int(m.group(3))
        hour = int(m.group(4))
        minute = int(m.group(5))
        ampm = (m.group(6) or "").upper()
        month = MONTHS_MAP.get(month_str[:3])

    if month is None:
        m = re.match(
            r"(\w+)\s+(\d{1,2}),?\s+(\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)?",
            raw, re.IGNORECASE,
        )
        if m:
            month_str = m.group(1).lower().rstrip(".")
            day = int(m.group(2))
            year = int(m.group(3))
            hour = int(m.group(4))
            minute = int(m.group(5))
            ampm = (m.group(6) or "").upper()
            month = MONTHS_MAP.get(month_str[:3])

    if not all(v is not None for v in (day, month, year, hour, minute)):
        return None

    if ampm == "PM" and hour != 12:
        hour += 12
    elif ampm == "AM" and hour == 12:
        hour = 0

    try:
        return datetime(year, month, day, hour, minute)
    except ValueError:
        return None


def _parse_zoom_invite(text: str) -> dict | None:
    link_match = re.search(r"https?://[\w.]*zoom\.us/j/[\w?=&./-]+", text)
    if not link_match:
        return None

    result = {"zoom_link": link_match.group(0)}

    topic_m = re.search(r"(?:Тема|Topic):\s*(.+)", text, re.IGNORECASE)
    result["topic"] = topic_m.group(1).strip() if topic_m else "Zoom-конференция"

    time_m = re.search(r"(?:Время|Time):\s*(.+)", text, re.IGNORECASE)
    if time_m:
        result["datetime_str"] = time_m.group(1).strip()
        dt = _parse_zoom_datetime(time_m.group(1))
        if dt:
            result["datetime"] = dt.isoformat()

    id_m = re.search(
        r"(?:Идентификатор\s*конференции|Meeting\s*ID|ID)[:\s]*([\d\s]+)",
        text, re.IGNORECASE,
    )
    if id_m:
        result["meeting_id"] = id_m.group(1).strip()

    pass_m = re.search(
        r"(?:Код доступа|Passcode|Password)[:\s]*(\S+)",
        text, re.IGNORECASE,
    )
    if pass_m:
        result["passcode"] = pass_m.group(1).strip()

    return result


def _format_meeting(m: dict) -> str:
    lines = [f"<b>{m.get('topic', 'Zoom-конференция')}</b>"]
    if m.get("datetime"):
        dt = datetime.fromisoformat(m["datetime"])
        lines.append(f"🕐 {dt.strftime('%d.%m.%Y %H:%M')}")
    elif m.get("datetime_str"):
        lines.append(f"🕐 {m['datetime_str']}")
    if m.get("zoom_link"):
        lines.append(f'🔗 <a href="{m["zoom_link"]}">Подключиться</a>')
    if m.get("meeting_id"):
        lines.append(f"🆔 ID: {m['meeting_id']}")
    if m.get("passcode"):
        lines.append(f"🔑 Код: {m['passcode']}")
    return "\n".join(lines)


def _format_seconds(sec: int) -> str:
    if sec < 60:
        return f"{sec} сек"
    m, s = divmod(sec, 60)
    return f"{m} мин" if s == 0 else f"{m} мин {s} сек"


# ====================================================================
#  АКТИВНЫЕ ТАЙМЕРЫ (in-memory, не в БД)
# ====================================================================
pending: dict[tuple[int, int], dict] = {}
invite_link_cache: dict[int, str] = {}
user_state: dict[int, dict] = {}


def _get_state(user_id: int) -> dict:
    if user_id not in user_state:
        user_state[user_id] = {"screen": "main", "menu_msg_id": None}
    return user_state[user_id]


def _cancel_pending(chat_id: int, user_id: int):
    key = (chat_id, user_id)
    if key not in pending:
        return
    task = pending[key].get("task")
    if task and not task.done():
        task.cancel()
    del pending[key]


def _cancel_all_user_pending(user_id: int):
    for key in [k for k in pending if k[1] == user_id]:
        task = pending[key].get("task")
        if task and not task.done():
            task.cancel()
        del pending[key]


async def _restart_user_timers(user_id: int, bot: Bot):
    u = await db.get_user_by_id(user_id)
    if not u:
        return
    for key in [k for k in pending if k[1] == user_id]:
        chat_id = key[0]
        old = pending[key]
        task = old.get("task")
        if task and not task.done():
            task.cancel()
        new_task = asyncio.create_task(
            _reminder_loop(
                chat_id=chat_id,
                user_id=user_id,
                private_chat_id=u.get("private_chat_id"),
                target_username=u.get("username"),
                sla=0,
                repeat_interval=u.get("repeat_interval", DEFAULT_INTERVAL),
                msg_link=old.get("msg_link"),
                chat_title=old.get("chat_title", ""),
                group_link=old.get("group_link"),
                sender_name=old.get("sender_name", ""),
                text_preview=old.get("text_preview", ""),
                bot=bot,
            )
        )
        pending[key] = {**old, "task": new_task}


# ====================================================================
#  ХЕЛПЕРЫ
# ====================================================================
async def _extract_mentioned_users(msg: Message) -> list[dict]:
    pairs: list[tuple[str, list]] = []
    if msg.text and msg.entities:
        pairs.append((msg.text, msg.entities))
    if msg.caption and msg.caption_entities:
        pairs.append((msg.caption, msg.caption_entities))

    found: dict[int, dict] = {}
    for source_text, entities in pairs:
        for ent in entities:
            if ent.type == "mention":
                mentioned = source_text[ent.offset : ent.offset + ent.length].lstrip("@")
                u = await db.get_user_by_username(mentioned)
                if u and u["tracking_enabled"]:
                    found[u["user_id"]] = u
            elif ent.type == "text_mention" and ent.user:
                u = await db.get_user_by_id(ent.user.id)
                if u and u["tracking_enabled"]:
                    found[u["user_id"]] = u
    return list(found.values())


def _build_message_link(chat, message_id: int) -> str | None:
    if chat.username:
        return f"https://t.me/{chat.username}/{message_id}"
    raw_id = str(chat.id)
    if raw_id.startswith("-100"):
        short_id = raw_id[4:]
    else:
        short_id = raw_id.lstrip("-")
    return f"https://t.me/c/{short_id}/{message_id}"


async def _get_group_link(chat_id: int, bot: Bot) -> str | None:
    if chat_id in invite_link_cache:
        return invite_link_cache[chat_id]
    try:
        chat_info = await bot.get_chat(chat_id)
        if chat_info.username:
            link = f"https://t.me/{chat_info.username}"
            invite_link_cache[chat_id] = link
            return link
        if chat_info.invite_link:
            invite_link_cache[chat_id] = chat_info.invite_link
            return chat_info.invite_link
        link = await bot.export_chat_invite_link(chat_id)
        invite_link_cache[chat_id] = link
        return link
    except Exception as e:
        logger.warning(f"Не удалось получить ссылку на группу {chat_id}: {e}")
        return None


def _sender_display_name(user) -> str:
    parts = []
    if user.first_name:
        parts.append(h(user.first_name))
    if user.last_name:
        parts.append(h(user.last_name))
    name = " ".join(parts) if parts else "Неизвестный"
    if user.username:
        name += f" (@{h(user.username)})"
    return name


def _build_reminder_text(
    chat_title: str, group_link: str | None,
    sender_name: str, text_preview: str, msg_link: str | None,
    context_summary: str = "",
) -> str:
    group_display = f'<a href="{group_link}">{chat_title}</a>' if group_link else chat_title
    lines = [
        "<b>Напоминание о сообщении</b>",
        f"В группе {group_display} ожидается Ваш ответ на сообщение от {sender_name}: {text_preview}",
    ]
    if context_summary:
        lines.append(format_context_html(context_summary))
    if msg_link:
        lines.append(f'<a href="{msg_link}">Открыть сообщение</a>')
    return "\n".join(lines)


def _make_text_preview(msg: Message) -> str:
    raw_text = msg.text or msg.caption or ""
    first_line = raw_text.split("\n", 1)[0].strip()
    return first_line[:100] + ("…" if len(first_line) > 100 else "")


# ====================================================================
#  INLINE-КЛАВИАТУРЫ
# ====================================================================
def _ikb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    """Строит InlineKeyboardMarkup из [[(label, callback_data), ...], ...]"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=data) for label, data in row]
            for row in rows
        ]
    )


def _main_menu_kb(u: dict | None) -> InlineKeyboardMarkup:
    is_tracked = u and u["tracking_enabled"]
    toggle = ("⏹ Выключить", "untrack") if is_tracked else ("✅ Включить", "track")
    return _ikb([
        [toggle],
        [("⚙ Настройки", "settings"), ("📊 Статус", "status")],
        [("📅 Встречи", "meetings"), ("📝 Саммари", "summary")],
    ])


def _settings_kb(u: dict) -> InlineKeyboardMarkup:
    sla = u.get("sla", DEFAULT_SLA)
    interval = u.get("repeat_interval", DEFAULT_INTERVAL)
    zoom_remind = u.get("zoom_remind_before", DEFAULT_ZOOM_REMIND)
    return _ikb([
        [(f"⏱ Напоминание: {_format_seconds(sla)}", "set_sla")],
        [(f"🔁 Повторы: {_format_seconds(interval)}", "set_int")],
        [(f"📹 Созвон: {_format_seconds(zoom_remind)}", "set_zr")],
        [("◀ Назад", "back_main")],
    ])


def _picker_kb(options: list[int], current: int, prefix: str) -> InlineKeyboardMarkup:
    buttons = []
    for val in options:
        label = _format_seconds(val)
        if val == current:
            label = f"• {label} •"
        buttons.append((label, f"{prefix}_{val}"))
    rows = [buttons[i : i + 3] for i in range(0, len(buttons), 3)]
    rows.append([("✏ Своё значение", f"{prefix}_cust")])
    rows.append([("◀ Назад", "back_settings")])
    return _ikb(rows)


def _summary_groups_kb(chats: list[dict]) -> InlineKeyboardMarkup:
    rows = [[(f"💬 {c['title']}", f"sg_{i}")] for i, c in enumerate(chats)]
    rows.append([("◀ Назад", "back_main")])
    return _ikb(rows)


def _summary_topics_kb(topics: list[dict]) -> InlineKeyboardMarkup:
    rows = [[("📂 Все сообщения", "st_all")]]
    for i, t in enumerate(topics):
        label = f"📂 {i+1}. {t['title'][:40]}"
        if t.get("msg_count"):
            label += f" ({t['msg_count']})"
        rows.append([(label, f"st_{i}")])
    rows.append([("◀ Назад", "back_sg")])
    return _ikb(rows)


def _summary_mode_kb() -> InlineKeyboardMarkup:
    return _ikb([
        [("📝 30 сообщ.", "sm_30"), ("📝 50 сообщ.", "sm_50")],
        [("📝 100 сообщ.", "sm_100"), ("📅 За день", "sm_day")],
        [("✏ Своё кол-во", "sm_cust")],
        [("◀ Назад", "back_st")],
    ])


def _meetings_kb(user_meetings: list[dict]) -> InlineKeyboardMarkup:
    rows = [[(f"📅 {i+1}. {m.get('topic','Zoom')[:30]}", f"mt_{i}")] for i, m in enumerate(user_meetings)]
    rows.append([("◀ Назад", "back_main")])
    return _ikb(rows)


def _meeting_detail_kb(meeting_id: str) -> InlineKeyboardMarkup:
    return _ikb([
        [("🗑 Удалить", f"md_del_{meeting_id}")],
        [("◀ Назад", "back_mt")],
    ])


def _cancel_kb() -> InlineKeyboardMarkup:
    return _ikb([[("✖ Отмена", "cancel")]])


# ====================================================================
#  EDIT-MENU HELPER
# ====================================================================
async def _edit_menu(
    bot: Bot, state: dict, chat_id: int,
    text: str, kb: InlineKeyboardMarkup,
):
    """Edit the menu message in place; ignore if content unchanged."""
    from aiogram.exceptions import TelegramBadRequest

    menu_msg_id = state.get("menu_msg_id")
    if menu_msg_id:
        try:
            await bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=menu_msg_id,
                reply_markup=kb,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                return  # текст не изменился — ничего не делаем
            # Другая ошибка (удалено, старое) — пересоздаём ниже
        except Exception:
            pass
    # Fallback: send new message
    sent = await bot.send_message(
        chat_id, text,
        reply_markup=kb,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    state["menu_msg_id"] = sent.message_id


async def _send_then_menu(
    bot: Bot, state: dict, chat_id: int,
    parts: list[str], menu_text: str, menu_kb: InlineKeyboardMarkup,
):
    """
    Отправляет контент (саммари), потом пересоздаёт меню внизу.
    1. Удаляет старое меню-сообщение
    2. Отправляет parts как обычные сообщения
    3. Отправляет НОВОЕ меню-сообщение (оно внизу)
    """
    # Удаляем старое меню
    menu_msg_id = state.get("menu_msg_id")
    if menu_msg_id:
        try:
            await bot.delete_message(chat_id, menu_msg_id)
        except Exception:
            pass
        state["menu_msg_id"] = None

    # Отправляем контент
    for part in parts:
        await bot.send_message(
            chat_id, part,
            parse_mode="HTML", disable_web_page_preview=True,
        )

    # Новое меню — будет последним сообщением
    sent = await bot.send_message(
        chat_id, menu_text,
        reply_markup=menu_kb,
        parse_mode="HTML", disable_web_page_preview=True,
    )
    state["menu_msg_id"] = sent.message_id


async def _recreate_menu_below(bot: Bot, user_id: int, private_chat_id: int):
    """Пересоздаёт меню-сообщение внизу чата после отправки уведомления."""
    state = _get_state(user_id)
    menu_msg_id = state.get("menu_msg_id")
    if not menu_msg_id:
        return
    # Удаляем старое меню
    try:
        await bot.delete_message(private_chat_id, menu_msg_id)
    except Exception:
        pass
    # Создаём новое внизу
    u = await db.get_user_by_id(user_id)
    status_line = "✅ Отслеживание включено" if (u and u["tracking_enabled"]) else "⏹ Отслеживание выключено"
    try:
        sent = await bot.send_message(
            private_chat_id,
            f"{status_line}\n\nВыберите действие:",
            reply_markup=_main_menu_kb(u),
            parse_mode="HTML",
        )
        state["menu_msg_id"] = sent.message_id
        state["screen"] = "main"
    except Exception:
        state["menu_msg_id"] = None


# ====================================================================
#  ЦИКЛ НАПОМИНАНИЙ
# ====================================================================
async def _reminder_loop(
    chat_id: int, user_id: int, private_chat_id: int | None,
    target_username: str | None, sla: int, repeat_interval: int,
    msg_link: str | None, chat_title: str, group_link: str | None,
    sender_name: str, text_preview: str, bot: Bot,
):
    key = (chat_id, user_id)
    try:
        await asyncio.sleep(sla)
        if key not in pending:
            return

        # Генерируем контекст, если LLM доступна (не блокирует при ошибке)
        context = ""
        if private_chat_id and os.getenv("LLM_API_KEY"):
            try:
                context = await summary_service.get_mention_context(chat_id, pending[key].get("source_msg_id", 0))
            except Exception:
                pass

        reminder = _build_reminder_text(chat_title, group_link, sender_name, text_preview, msg_link, context)

        async def send_reminder():
            if private_chat_id is None:
                tag = f"@{target_username}" if target_username else "Пользователь"
                await bot.send_message(
                    chat_id,
                    f"⏳ {tag}, вам написали — ответьте, пожалуйста.\n"
                    "(Напишите мне /start в ЛС, чтобы получать уведомления лично.)",
                )
            else:
                # Кнопка «Прочитано» — отключает напоминание
                read_kb = _ikb([[
                    ("✅ Прочитано", f"read_{chat_id}_{user_id}"),
                ]])
                await bot.send_message(
                    private_chat_id, reminder,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=read_kb,
                )
                # Пересоздаём меню внизу, чтобы оно не терялось
                await _recreate_menu_below(bot, user_id, private_chat_id)

        await send_reminder()
        logger.info(f"Первое напоминание → user_id={user_id}")

        while True:
            await asyncio.sleep(repeat_interval)
            if key not in pending:
                break
            await send_reminder()
            logger.info(f"Повторное напоминание → user_id={user_id}")

    except asyncio.CancelledError:
        logger.debug(f"Таймер ({chat_id}, {user_id}) отменён")
    except Exception as e:
        logger.error(f"Ошибка в _reminder_loop ({chat_id}, {user_id}): {e}")


# ====================================================================
#  РОУТЕРЫ
# ====================================================================
private_router = Router()
group_router = Router()


# ---- ГРУППОВЫЕ СООБЩЕНИЯ ----
@group_router.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def handle_group_message(msg: Message, bot: Bot):
    if not msg.from_user:
        return

    chat_id = msg.chat.id
    author_id = msg.from_user.id

    # ---- Сохраняем сообщение для саммаризации ----
    msg_text = msg.text or msg.caption or ""
    # Не сохраняем: команды бота, пустые сервисные сообщения
    if msg_text and not msg_text.startswith("/"):
        try:
            await db.save_raw_message(
                chat_id=chat_id,
                message_id=msg.message_id,
                from_user_id=author_id,
                username=msg.from_user.username or "",
                first_name=msg.from_user.first_name or "",
                date=msg.date.isoformat() if msg.date else "",
                text=msg_text,
                message_thread_id=msg.message_thread_id,
                reply_to_message_id=(
                    msg.reply_to_message.message_id if msg.reply_to_message else None
                ),
                has_media=bool(msg.photo or msg.video or msg.document or msg.voice or msg.sticker),
            )
        except Exception as e:
            logger.warning(f"Не удалось сохранить сообщение: {e}")

    # ---- Сохраняем название темы (форум-группы) ----
    if msg.message_thread_id:
        try:
            existing_name = await db.get_topic_name(chat_id, msg.message_thread_id)
            if not existing_name:
                topic_name = None
                # Способ 1: само сообщение — создание темы
                if msg.forum_topic_created:
                    topic_name = msg.forum_topic_created.name
                # Способ 2: reply_to содержит forum_topic_created
                elif msg.reply_to_message and msg.reply_to_message.forum_topic_created:
                    topic_name = msg.reply_to_message.forum_topic_created.name
                # Способ 3: reply_to — заголовочное сообщение темы (forum_topic_edited)
                elif msg.reply_to_message and msg.reply_to_message.forum_topic_edited:
                    topic_name = msg.reply_to_message.forum_topic_edited.name
                if topic_name:
                    await db.upsert_topic(chat_id, msg.message_thread_id, topic_name)
        except Exception:
            pass

    # Кэшируем название группы для меню саммари в ЛС
    try:
        await db.upsert_chat(
            chat_id=chat_id,
            title=msg.chat.title or f"Чат {chat_id}",
            username=msg.chat.username,
        )
    except Exception:
        pass

    # ---- Membership tracking + conference link detection ----
    author_data = await db.get_user_by_id(author_id)

    if author_data:
        try:
            await db.upsert_membership(
                chat_id, author_id,
                msg.from_user.username or "",
                msg.from_user.first_name or "",
            )
        except Exception:
            pass

    # Детектим ссылки на конференции
    try:
        conf_links = detect_conference_links(msg)
        if conf_links:
            chat_title = msg.chat.title or f"Чат {chat_id}"
            sender_name = _sender_display_name(msg.from_user)
            members = await db.get_chat_members_with_private_chat(chat_id)

            for link_info in conf_links:
                for member in members:
                    if member["user_id"] == author_id:
                        continue  # автор и так знает
                    offer_id = uuid.uuid4().hex[:12]
                    created = await db.create_conference_offer(
                        offer_id=offer_id,
                        chat_id=chat_id,
                        source_message_id=msg.message_id,
                        sender_user_id=author_id,
                        user_id=member["user_id"],
                        provider=link_info["provider"],
                        normalized_url=link_info["normalized_url"],
                        original_url=link_info["url"],
                    )
                    if not created:
                        continue  # дубль — уже предлагали

                    # Формируем карточку с деталями
                    card_lines = [
                        f"🔗 Конференция в чате «{chat_title}»\n",
                        f"📹 {link_info['provider']}",
                        f"🔗 {link_info['url']}",
                    ]
                    if link_info.get("meeting_code"):
                        card_lines.append(f"🆔 ID: {format_meeting_id(link_info['meeting_code'])}")
                    if link_info.get("passcode"):
                        card_lines.append(f"🔑 Код: {link_info['passcode']}")
                    card_lines.append(f"\nОтправил: {sender_name}")
                    card_lines.append("\nДобавить в отслеживание?")
                    card = "\n".join(card_lines)

                    kb = _ikb([
                        [("✅ Добавить", f"cy_{offer_id}"),
                         ("❌ Не добавлять", f"cn_{offer_id}")],
                    ])
                    try:
                        private_cid = member["private_chat_id"]

                        # Удаляем старое меню → карточка → новое меню внизу
                        m_state = _get_state(member["user_id"])
                        old_menu = m_state.get("menu_msg_id")
                        if old_menu:
                            try:
                                await bot.delete_message(private_cid, old_menu)
                            except Exception:
                                pass
                            m_state["menu_msg_id"] = None

                        await bot.send_message(
                            private_cid, card,
                            reply_markup=kb,
                            disable_web_page_preview=True,
                        )

                        # Пересоздаём меню внизу
                        u_data = await db.get_user_by_id(member["user_id"])
                        status_line = "✅ Отслеживание включено" if (u_data and u_data["tracking_enabled"]) else "⏹ Отслеживание выключено"
                        sent = await bot.send_message(
                            private_cid,
                            f"{status_line}\n\nВыберите действие:",
                            reply_markup=_main_menu_kb(u_data),
                            parse_mode="HTML",
                        )
                        m_state["menu_msg_id"] = sent.message_id
                        m_state["screen"] = "main"

                    except Exception as e:
                        logger.warning(f"Не удалось отправить offer user_id={member['user_id']}: {e}")
    except Exception as e:
        logger.warning(f"Ошибка conference detection: {e}")

    if author_data and author_data["tracking_enabled"]:
        key = (chat_id, author_id)
        if key in pending:
            _cancel_pending(chat_id, author_id)
            logger.info(f"user_id={author_id} ответил в чате {chat_id}, таймер снят")

    mentioned_users = await _extract_mentioned_users(msg)
    mentioned_users = [u for u in mentioned_users if u["user_id"] != author_id]
    if not mentioned_users:
        return

    msg_link = _build_message_link(msg.chat, msg.message_id)
    chat_title = msg.chat.title or f"Чат {chat_id}"
    sender_name = _sender_display_name(msg.from_user)
    group_link = await _get_group_link(chat_id, bot)

    for target in mentioned_users:
        target_uid = target["user_id"]
        logger.info(f"Упоминание user_id={target_uid} в чате {chat_id}")
        _cancel_pending(chat_id, target_uid)

        text_preview = _make_text_preview(msg)
        task = asyncio.create_task(
            _reminder_loop(
                chat_id=chat_id, user_id=target_uid,
                private_chat_id=target.get("private_chat_id"),
                target_username=target.get("username"),
                sla=target.get("sla", DEFAULT_SLA),
                repeat_interval=target.get("repeat_interval", DEFAULT_INTERVAL),
                msg_link=msg_link, chat_title=chat_title,
                group_link=group_link, sender_name=sender_name,
                text_preview=text_preview, bot=bot,
            )
        )
        pending[(chat_id, target_uid)] = {
            "task": task, "msg_link": msg_link, "chat_title": chat_title,
            "group_link": group_link, "sender_name": sender_name,
            "text_preview": text_preview, "source_msg_id": msg.message_id,
        }


# ---- /start В ЛС ----
@private_router.message(Command("start"), F.chat.type == ChatType.PRIVATE)
async def cmd_start(msg: Message, bot: Bot):
    user = msg.from_user
    state = _get_state(user.id)
    state["screen"] = "main"

    u = await db.get_user_by_id(user.id)
    is_new = u is None
    is_admin = user.id == ADMIN_USER_ID

    # Регистрация нового пользователя
    if is_new:
        auto_approve = is_admin or not APPROVAL_REQUIRED
        await db.upsert_user(
            user_id=user.id,
            username=user.username or "",
            first_name=user.first_name or "",
            private_chat_id=msg.chat.id,
            is_approved=auto_approve,
        )
        u = await db.get_user_by_id(user.id)
    else:
        # Обновляем private_chat_id
        await db.update_user_field(user.id, "private_chat_id", msg.chat.id)
        # Если режим без одобрения — автоматически одобряем старых
        if not APPROVAL_REQUIRED and not u.get("is_approved"):
            await db.approve_user(user.id, True)
            u = await db.get_user_by_id(user.id)

    # Удаляем старое меню
    old_id = state.get("menu_msg_id")
    if old_id:
        try:
            await bot.delete_message(msg.chat.id, old_id)
        except Exception:
            pass
        state["menu_msg_id"] = None

    # Если нужно одобрение и не одобрен — заявка админу
    if APPROVAL_REQUIRED and not u.get("is_approved") and not is_admin:
        # Отправляем заявку админу (и при первом /start, и при повторном)
        if ADMIN_USER_ID:
            name = h(user.first_name or "")
            uname = f" (@{h(user.username)})" if user.username else ""
            label = "Повторная заявка" if not is_new else "Новая заявка"
            kb = _ikb([
                [("✅ Одобрить", f"adm_approve_{user.id}"),
                 ("❌ Отклонить", f"adm_reject_{user.id}")],
            ])
            try:
                # Отправляем заявку админу, не сбивая его меню
                admin_state = _get_state(ADMIN_USER_ID)
                admin_menu = admin_state.get("menu_msg_id")
                admin_chat = (await db.get_user_by_id(ADMIN_USER_ID) or {}).get("private_chat_id")
                if admin_chat:
                    # Удаляем старое меню админа
                    if admin_menu:
                        try:
                            await bot.delete_message(admin_chat, admin_menu)
                        except Exception:
                            pass

                    # Отправляем заявку
                    await bot.send_message(
                        admin_chat,
                        f"📋 <b>{label}</b>\n\n"
                        f"Пользователь: {name}{uname}\n"
                        f"ID: <code>{user.id}</code>",
                        reply_markup=kb,
                        parse_mode="HTML",
                    )

                    # Пересоздаём меню админа внизу
                    await _recreate_menu_below(bot, ADMIN_USER_ID, admin_chat)

            except Exception as e:
                logger.warning(f"Не удалось уведомить админа: {e}")

        # Сообщение пользователю (без меню — он не одобрен)
        await bot.send_message(
            msg.chat.id,
            "👋 Привет! Я бот-секретарь.\n\n"
            "⏳ Ваша заявка отправлена администратору.\n"
            "Вы получите уведомление, когда доступ будет открыт.",
            reply_markup=ReplyKeyboardRemove(),
        )
        logger.info(f"Заявка: user_id={user.id} @{user.username} (new={is_new})")
        return

    # Одобренный пользователь — показываем меню
    status_line = "✅ Отслеживание включено" if (u and u["tracking_enabled"]) else "⏹ Отслеживание выключено"

    await bot.send_message(
        msg.chat.id,
        "👋 Привет! Я бот-секретарь.\n\n"
        "Если вас упомянут в групповом чате, а вы не ответите — "
        "я пришлю напоминание в личные сообщения.",
        reply_markup=ReplyKeyboardRemove(),
    )

    sent = await bot.send_message(
        msg.chat.id,
        f"{status_line}\n\nВыберите действие:",
        reply_markup=_main_menu_kb(u),
        parse_mode="HTML",
    )
    state["menu_msg_id"] = sent.message_id


# ====================================================================
#  CALLBACK QUERY HANDLER
# ====================================================================
@private_router.callback_query(F.message.chat.type == ChatType.PRIVATE)
async def handle_callback(query: CallbackQuery, bot: Bot):
    if not query.message:
        await query.answer()
        return
    user = query.from_user
    uid = user.id
    chat_id = query.message.chat.id
    data = query.data or ""
    state = _get_state(uid)

    # ---- Кнопка «Прочитано» на напоминании ----
    if data.startswith("read_"):
        try:
            parts = data.split("_")
            r_chat_id = int(parts[1])
            r_user_id = int(parts[2])
        except (ValueError, IndexError):
            await query.answer("Некорректные данные.")
            return

        # Проверяем что это тот же пользователь, кому было напоминание
        if uid != r_user_id:
            await query.answer("Это не ваше напоминание.")
            return

        # Отменяем таймер
        key = (r_chat_id, r_user_id)
        if key in pending:
            _cancel_pending(r_chat_id, r_user_id)
            logger.info(f"user_id={r_user_id} нажал «Прочитано» в чате {r_chat_id}, таймер снят")

        # Убираем кнопку и отмечаем как прочитанное
        try:
            new_text = (query.message.html_text or query.message.text or "") + "\n\n✅ <i>Прочитано</i>"
            await query.message.edit_text(
                new_text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass

        await query.answer("Напоминание отключено")
        return

    # ---- Админ: одобрение/отклонение заявок ----
    if data.startswith("adm_approve_") or data.startswith("adm_reject_"):
        if uid != ADMIN_USER_ID:
            await query.answer("Нет прав.")
            return
        try:
            target_uid = int(data.split("_")[-1])
        except (ValueError, IndexError):
            await query.answer("Некорректные данные.")
            return
        if data.startswith("adm_approve_"):
            await db.approve_user(target_uid, True)
            await query.message.edit_text(
                query.message.text + "\n\n✅ Одобрен",
                parse_mode="HTML",
            )
            # Уведомляем пользователя
            target_user = await db.get_user_by_id(target_uid)
            if target_user and target_user.get("private_chat_id"):
                try:
                    await bot.send_message(
                        target_user["private_chat_id"],
                        "✅ <b>Доступ одобрен!</b>\n\nНапишите /start, чтобы начать.",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            logger.info(f"Пользователь {target_uid} одобрен админом")
        else:
            await db.approve_user(target_uid, False)
            await query.message.edit_text(
                query.message.text + "\n\n❌ Отклонён",
                parse_mode="HTML",
            )
            logger.info(f"Пользователь {target_uid} отклонён админом")
        await query.answer()
        return

    # ---- Проверка is_approved для всех остальных действий ----
    u = await db.get_user_by_id(uid)
    if u and not u.get("is_approved"):
        await query.answer("⏳ Ваша заявка ещё не одобрена.", show_alert=True)
        return

    # Ensure menu_msg_id is tracked
    if query.message and query.message.message_id:
        state["menu_msg_id"] = query.message.message_id

    async def menu(text: str, kb: InlineKeyboardMarkup):
        await _edit_menu(bot, state, chat_id, text, kb)

    # ============================================================
    #  MAIN MENU ACTIONS
    # ============================================================
    if data == "track":
        existing = await db.get_user_by_id(uid)
        await db.upsert_user(
            user_id=uid,
            username=user.username or "",
            first_name=user.first_name or "",
            private_chat_id=chat_id,
            tracking_enabled=True,
            sla=existing["sla"] if existing else DEFAULT_SLA,
            repeat_interval=existing["repeat_interval"] if existing else DEFAULT_INTERVAL,
            zoom_remind_before=existing["zoom_remind_before"] if existing else DEFAULT_ZOOM_REMIND,
        )
        u = await db.get_user_by_id(uid)
        logger.info(f"track от user_id={uid} (@{user.username})")
        state["screen"] = "main"
        await menu(
            "✅ Отслеживание включено!\n"
            "Я буду напоминать вам в ЛС, если вас упомянут в группе и вы не ответите.",
            _main_menu_kb(u),
        )

    elif data == "untrack":
        await db.update_user_field(uid, "tracking_enabled", False)
        _cancel_all_user_pending(uid)
        logger.info(f"untrack от user_id={uid}")
        u = await db.get_user_by_id(uid)
        state["screen"] = "main"
        await menu("⏹ Отслеживание выключено. Все таймеры сняты.", _main_menu_kb(u))

    elif data == "settings":
        u = await db.get_user_by_id(uid)
        if not u:
            await menu("Сначала включите отслеживание.", _main_menu_kb(None))
            await query.answer()
            return
        state["screen"] = "settings"
        await menu("⚙ Настройки\n\nВыберите параметр для изменения:", _settings_kb(u))

    elif data == "status":
        u = await db.get_user_by_id(uid)
        if not u:
            await menu("Вы не зарегистрированы. Включите отслеживание.", _main_menu_kb(None))
            await query.answer()
            return
        status = "✅ Включено" if u["tracking_enabled"] else "⏹ Выключено"
        active_timers = sum(1 for k in pending if k[1] == uid)
        state["screen"] = "main"
        await menu(
            f"📊 <b>Ваши настройки</b>\n\n"
            f"Отслеживание: {status}\n"
            f"Username: @{u['username']}\n"
            f"Первое напоминание через: {_format_seconds(u['sla'])}\n"
            f"Интервал повторов: {_format_seconds(u['repeat_interval'])}\n"
            f"Активных таймеров: {active_timers}",
            _main_menu_kb(u),
        )

    elif data == "meetings":
        user_meetings = await db.get_user_meetings(uid)
        if not user_meetings:
            u = await db.get_user_by_id(uid)
            state["screen"] = "main"
            await menu("📅 Нет встреч.\nОтправьте Zoom-инвайт, чтобы добавить.", _main_menu_kb(u))
            await query.answer()
            return
        lines = ["📅 <b>Ваши встречи:</b>\n"]
        for i, m in enumerate(user_meetings, 1):
            lines.append(f"{i}. {_format_meeting(m)}")
        lines.append("\nНажмите на встречу для деталей.")
        state["screen"] = "meetings_list"
        state["user_meetings"] = user_meetings
        await menu("\n".join(lines), _meetings_kb(user_meetings))

    elif data == "summary":
        chats = await db.get_user_chats(uid)
        if not chats:
            u = await db.get_user_by_id(uid)
            state["screen"] = "main"
            await menu(
                "📝 Нет данных для саммари.\n\n"
                "Бот начнёт собирать сообщения, когда его добавят в группу. "
                "Подождите, пока в группе появится переписка.",
                _main_menu_kb(u),
            )
            await query.answer()
            return
        state["screen"] = "summary_groups"
        state["available_chats"] = chats
        await menu("📝 Выберите группу для саммари:", _summary_groups_kb(chats))

    # ============================================================
    #  BACK NAVIGATION
    # ============================================================
    elif data == "back_main":
        state["screen"] = "main"
        u = await db.get_user_by_id(uid)
        status_line = "✅ Отслеживание включено" if (u and u["tracking_enabled"]) else "⏹ Отслеживание выключено"
        await menu(f"Бот-секретарь\n\n{status_line}", _main_menu_kb(u))

    elif data == "back_settings":
        state["screen"] = "settings"
        u = await db.get_user_by_id(uid)
        await menu("⚙ Настройки", _settings_kb(u))

    elif data == "back_mt":
        user_meetings = await db.get_user_meetings(uid)
        if not user_meetings:
            state["screen"] = "main"
            u = await db.get_user_by_id(uid)
            await menu("📅 Нет встреч.", _main_menu_kb(u))
        else:
            lines = ["📅 <b>Ваши встречи:</b>\n"]
            for i, m in enumerate(user_meetings, 1):
                lines.append(f"{i}. {_format_meeting(m)}")
            state["screen"] = "meetings_list"
            state["user_meetings"] = user_meetings
            await menu("\n".join(lines), _meetings_kb(user_meetings))

    elif data == "back_sg":
        chats = await db.get_user_chats(uid)
        state["screen"] = "summary_groups"
        state["available_chats"] = chats
        await menu("📝 Выберите группу для саммари:", _summary_groups_kb(chats))

    elif data == "back_st":
        # Back from summary_mode: to topics if they exist, else to groups
        target_chat_id = state.get("summary_chat_id")
        if target_chat_id:
            topics = await db.get_chat_topics(target_chat_id)
            if topics:
                state["screen"] = "summary_topics"
                state["summary_topics"] = topics
                await menu(
                    f"📝 Группа: <b>{state.get('summary_chat_title', '')}</b>\n\nВыберите тему:",
                    _summary_topics_kb(topics),
                )
            else:
                chats = await db.get_user_chats(uid)
                state["screen"] = "summary_groups"
                state["available_chats"] = chats
                await menu("📝 Выберите группу для саммари:", _summary_groups_kb(chats))
        else:
            chats = await db.get_user_chats(uid)
            state["screen"] = "summary_groups"
            state["available_chats"] = chats
            await menu("📝 Выберите группу для саммари:", _summary_groups_kb(chats))

    # ============================================================
    #  SETTINGS: PICKER ENTRY
    # ============================================================
    elif data == "set_sla":
        u = await db.get_user_by_id(uid)
        if not u:
            await query.answer()
            return
        state["screen"] = "pick_sla"
        await menu(
            "⏱ Первое напоминание через:\n\nВыберите время до первого напоминания:",
            _picker_kb(SLA_OPTIONS, u["sla"], "sla"),
        )

    elif data == "set_int":
        u = await db.get_user_by_id(uid)
        if not u:
            await query.answer()
            return
        state["screen"] = "pick_interval"
        await menu(
            "🔁 Интервал повторных напоминаний:\n\nВыберите как часто повторять:",
            _picker_kb(INTERVAL_OPTIONS, u["repeat_interval"], "int"),
        )

    elif data == "set_zr":
        u = await db.get_user_by_id(uid)
        if not u:
            await query.answer()
            return
        state["screen"] = "pick_zoom_remind"
        await menu(
            "📹 Напоминание о созвоне:\n\nЗа сколько до начала напоминать?",
            _picker_kb(ZOOM_REMIND_OPTIONS, u["zoom_remind_before"], "zr"),
        )

    # ============================================================
    #  PICKERS: SLA values
    # ============================================================
    elif data.startswith("sla_"):
        suffix = data[4:]
        if suffix == "cust":
            state["screen"] = "custom_sla"
            await menu(
                f"Введите время в секундах ({MIN_SLA}–{MAX_SLA}):\n\nНапример: 90",
                _cancel_kb(),
            )
        else:
            try:
                value = int(suffix)
            except ValueError:
                await query.answer()
                return
            await db.update_user_field(uid, "sla", value)
            await _restart_user_timers(uid, bot)
            state["screen"] = "settings"
            u = await db.get_user_by_id(uid)
            await menu(f"✅ Первое напоминание: {_format_seconds(value)}", _settings_kb(u))

    # ============================================================
    #  PICKERS: INTERVAL values
    # ============================================================
    elif data.startswith("int_"):
        suffix = data[4:]
        if suffix == "cust":
            state["screen"] = "custom_interval"
            await menu(
                f"Введите время в секундах ({MIN_INTERVAL}–{MAX_INTERVAL}):\n\nНапример: 90",
                _cancel_kb(),
            )
        else:
            try:
                value = int(suffix)
            except ValueError:
                await query.answer()
                return
            await db.update_user_field(uid, "repeat_interval", value)
            await _restart_user_timers(uid, bot)
            state["screen"] = "settings"
            u = await db.get_user_by_id(uid)
            await menu(f"✅ Интервал повторов: {_format_seconds(value)}", _settings_kb(u))

    # ============================================================
    #  PICKERS: ZOOM REMIND values
    # ============================================================
    elif data.startswith("zr_"):
        suffix = data[3:]
        if suffix == "cust":
            state["screen"] = "custom_zoom_remind"
            await menu(
                f"Введите время в секундах ({MIN_INTERVAL}–{MAX_INTERVAL}):\n\nНапример: 600",
                _cancel_kb(),
            )
        else:
            try:
                value = int(suffix)
            except ValueError:
                await query.answer()
                return
            await db.update_user_field(uid, "zoom_remind_before", value)
            state["screen"] = "settings"
            u = await db.get_user_by_id(uid)
            await menu(f"✅ Напоминание о созвоне: {_format_seconds(value)}", _settings_kb(u))

    # ============================================================
    #  MEETINGS: select meeting by index
    # ============================================================
    elif data.startswith("mt_"):
        try:
            idx = int(data[3:])
        except ValueError:
            await query.answer()
            return
        user_meetings = state.get("user_meetings", [])
        if 0 <= idx < len(user_meetings):
            m = user_meetings[idx]
            state["screen"] = "meeting_detail"
            state["current_meeting_id"] = m["id"]
            await menu(_format_meeting(m), _meeting_detail_kb(m["id"]))

    # ============================================================
    #  MEETINGS: delete
    # ============================================================
    elif data.startswith("md_del_"):
        mid = data[7:]
        if mid:
            await db.delete_meeting(mid)
            logger.info(f"Встреча {mid} удалена user_id={uid}")
        user_meetings = await db.get_user_meetings(uid)
        if not user_meetings:
            state["screen"] = "main"
            u = await db.get_user_by_id(uid)
            await menu("📅 Все встречи удалены.", _main_menu_kb(u))
        else:
            lines = ["📅 <b>Ваши встречи:</b>\n"]
            for i, m in enumerate(user_meetings, 1):
                lines.append(f"{i}. {_format_meeting(m)}")
            state["screen"] = "meetings_list"
            state["user_meetings"] = user_meetings
            await menu("\n".join(lines), _meetings_kb(user_meetings))

    # ============================================================
    #  SUMMARY: select group by index
    # ============================================================
    elif data.startswith("sg_"):
        try:
            idx = int(data[3:])
        except ValueError:
            await query.answer()
            return
        chats = state.get("available_chats", [])
        if 0 <= idx < len(chats):
            selected = chats[idx]
            state["summary_chat_id"] = selected["chat_id"]
            state["summary_chat_title"] = selected["title"]

            topics = await db.get_chat_topics(selected["chat_id"])
            if topics:
                state["screen"] = "summary_topics"
                state["summary_topics"] = topics
                state["summary_thread_id"] = None
                lines = [f"📝 Группа: <b>{selected['title']}</b>\n"]
                lines.append("Выберите тему или «Все сообщения»:")
                await menu("\n".join(lines), _summary_topics_kb(topics))
            else:
                state["screen"] = "summary_mode"
                state["summary_thread_id"] = None
                await menu(
                    f"📝 Группа: <b>{selected['title']}</b>\n\nВыберите режим саммари:",
                    _summary_mode_kb(),
                )

    # ============================================================
    #  SUMMARY: select topic
    # ============================================================
    elif data == "st_all":
        state["screen"] = "summary_mode"
        state["summary_thread_id"] = None
        await menu(
            f"📝 Группа: <b>{state.get('summary_chat_title', '')}</b>\n"
            f"📂 Все сообщения\n\nВыберите режим саммари:",
            _summary_mode_kb(),
        )

    elif data.startswith("st_"):
        try:
            idx = int(data[3:])
        except ValueError:
            await query.answer()
            return
        topics = state.get("summary_topics", [])
        if 0 <= idx < len(topics):
            topic = topics[idx]
            state["screen"] = "summary_mode"
            state["summary_thread_id"] = topic["thread_id"]
            await menu(
                f"📝 Группа: <b>{state.get('summary_chat_title', '')}</b>\n"
                f"📂 Тема: <b>{topic['title']}</b> ({topic['msg_count']} сообщ.)\n\n"
                f"Выберите режим саммари:",
                _summary_mode_kb(),
            )

    # ============================================================
    #  SUMMARY: mode selection
    # ============================================================
    elif data in ("sm_30", "sm_50", "sm_100"):
        n = int(data[3:])
        target_chat_id = state.get("summary_chat_id")
        thread_id = state.get("summary_thread_id")
        if not target_chat_id:
            state["screen"] = "main"
            u = await db.get_user_by_id(uid)
            await menu("Ошибка. Попробуйте снова.", _main_menu_kb(u))
            await query.answer()
            return

        await menu(f"⏳ Генерирую саммари ({n} сообщ.)...", _cancel_kb())
        await query.answer()
        result = await summary_service.summarize_last_n(
            target_chat_id, n, uid, message_thread_id=thread_id
        )
        parts = format_summary_html(result)
        state["screen"] = "summary_mode"
        await _send_then_menu(
            bot, state, chat_id, parts,
            f"📝 Группа: <b>{state.get('summary_chat_title', '')}</b>\n\nВыберите ещё режим или вернитесь:",
            _summary_mode_kb(),
        )
        return

    elif data == "sm_day":
        target_chat_id = state.get("summary_chat_id")
        thread_id = state.get("summary_thread_id")
        if not target_chat_id:
            state["screen"] = "main"
            u = await db.get_user_by_id(uid)
            await menu("Ошибка. Попробуйте снова.", _main_menu_kb(u))
            await query.answer()
            return

        await menu("⏳ Генерирую дайджест за день...", _cancel_kb())
        await query.answer()
        result = await summary_service.summarize_today(
            target_chat_id, uid, message_thread_id=thread_id
        )
        parts = format_summary_html(result)
        state["screen"] = "summary_mode"
        await _send_then_menu(
            bot, state, chat_id, parts,
            f"📝 Группа: <b>{state.get('summary_chat_title', '')}</b>\n\nВыберите ещё режим или вернитесь:",
            _summary_mode_kb(),
        )
        return

    elif data == "sm_cust":
        state["screen"] = "summary_custom"
        await menu(
            "Введите количество сообщений (3–200):\n\nНапример: 75",
            _cancel_kb(),
        )

    # ============================================================
    #  CANCEL (from custom input screens)
    # ============================================================
    elif data == "cancel":
        screen = state.get("screen", "main")
        if screen == "custom_sla":
            u = await db.get_user_by_id(uid)
            state["screen"] = "pick_sla"
            await menu("⏱ Первое напоминание:", _picker_kb(SLA_OPTIONS, u["sla"] if u else DEFAULT_SLA, "sla"))
        elif screen == "custom_interval":
            u = await db.get_user_by_id(uid)
            state["screen"] = "pick_interval"
            await menu("🔁 Интервал повторов:", _picker_kb(INTERVAL_OPTIONS, u["repeat_interval"] if u else DEFAULT_INTERVAL, "int"))
        elif screen == "custom_zoom_remind":
            u = await db.get_user_by_id(uid)
            state["screen"] = "pick_zoom_remind"
            await menu("📹 Напоминание о созвоне:", _picker_kb(ZOOM_REMIND_OPTIONS, u["zoom_remind_before"] if u else DEFAULT_ZOOM_REMIND, "zr"))
        elif screen == "summary_custom":
            state["screen"] = "summary_mode"
            await menu(
                f"📝 Группа: <b>{state.get('summary_chat_title', '')}</b>\n\nВыберите режим саммари:",
                _summary_mode_kb(),
            )
        else:
            state["screen"] = "main"
            u = await db.get_user_by_id(uid)
            await menu("Главное меню", _main_menu_kb(u))

    # ============================================================
    #  CONFERENCE OFFERS: accept / decline
    # ============================================================
    elif data.startswith("cy_"):
        offer_id = data[3:]
        offer = await db.get_conference_offer(offer_id)
        if not offer or offer["status"] != "pending":
            await query.answer("Предложение уже обработано.")
            return
        u = await db.get_user_by_id(uid)
        remind_before = u["zoom_remind_before"] if u else DEFAULT_ZOOM_REMIND
        meeting_id = uuid.uuid4().hex[:8]
        await db.add_meeting(
            meeting_id=meeting_id,
            user_id=uid,
            topic=f"{offer['provider']}-конференция",
            zoom_link=offer["original_url"],
            remind_before=remind_before,
        )
        await db.update_conference_offer_status(offer_id, "accepted", meeting_id=meeting_id)
        try:
            await query.message.edit_text(
                f"✅ Конференция добавлена!\n\n"
                f"📹 {offer['provider']}\n"
                f"Ссылка: {offer['original_url']}\n"
                f"🔔 Напоминание: за {_format_seconds(remind_before)}",
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        await query.answer("Добавлено!")
        return

    elif data.startswith("cn_"):
        offer_id = data[3:]
        offer = await db.get_conference_offer(offer_id)
        if not offer or offer["status"] != "pending":
            await query.answer("Предложение уже обработано.")
            return
        await db.update_conference_offer_status(offer_id, "declined")
        try:
            await query.message.edit_text("❌ Конференция не добавлена.")
        except Exception:
            pass
        await query.answer()
        return

    await query.answer()


# ====================================================================
#  TEXT HANDLER: Zoom invites + custom number input
# ====================================================================
@private_router.message(F.chat.type == ChatType.PRIVATE, F.text)
async def handle_private_text(msg: Message, bot: Bot):
    user = msg.from_user
    uid = user.id
    text = msg.text.strip()
    chat_id = msg.chat.id
    state = _get_state(uid)
    screen = state.get("screen", "main")

    async def menu(content: str, kb: InlineKeyboardMarkup):
        await _edit_menu(bot, state, chat_id, content, kb)

    # ---- Zoom invite (on main screen) ----
    if screen == "main" and "zoom.us/j/" in text.lower():
        parsed = _parse_zoom_invite(text)
        if parsed:
            u = await db.get_user_by_id(uid)
            remind_before = u["zoom_remind_before"] if u else DEFAULT_ZOOM_REMIND

            mid = uuid.uuid4().hex[:8]
            await db.add_meeting(
                meeting_id=mid,
                user_id=uid,
                topic=parsed.get("topic", "Zoom-конференция"),
                dt=parsed.get("datetime"),
                datetime_str=parsed.get("datetime_str"),
                zoom_link=parsed.get("zoom_link"),
                mid=parsed.get("meeting_id"),
                passcode=parsed.get("passcode"),
                remind_before=remind_before,
            )
            logger.info(f"Встреча сохранена: {mid} от user_id={uid}")

            confirm_lines = ["📅 <b>Встреча сохранена!</b>\n", _format_meeting(parsed)]
            if parsed.get("datetime"):
                confirm_lines.append(f"\n🔔 Напоминание: за {_format_seconds(remind_before)} до начала")

            # Try to delete the user's paste message
            try:
                await msg.delete()
            except Exception:
                pass

            await menu("\n".join(confirm_lines), _main_menu_kb(u))
        else:
            u = await db.get_user_by_id(uid)
            try:
                await msg.delete()
            except Exception:
                pass
            await menu("Не удалось распознать Zoom-инвайт.", _main_menu_kb(u))
        return

    # ---- Custom number input for SLA / Interval / Zoom remind ----
    if screen in ("custom_sla", "custom_interval", "custom_zoom_remind"):
        field_map = {
            "custom_sla": ("sla", MIN_SLA, MAX_SLA, "sla", SLA_OPTIONS, "Первое напоминание"),
            "custom_interval": ("repeat_interval", MIN_INTERVAL, MAX_INTERVAL, "int", INTERVAL_OPTIONS, "Интервал повторов"),
            "custom_zoom_remind": ("zoom_remind_before", MIN_INTERVAL, MAX_INTERVAL, "zr", ZOOM_REMIND_OPTIONS, "Напоминание о созвоне"),
        }
        field, min_val, max_val, prefix, options, label = field_map[screen]

        try:
            value = int(text)
        except ValueError:
            try:
                await msg.delete()
            except Exception:
                pass
            hint = await bot.send_message(chat_id, "Введите целое число секунд.")
            await asyncio.sleep(3)
            try:
                await hint.delete()
            except Exception:
                pass
            return

        if not (min_val <= value <= max_val):
            try:
                await msg.delete()
            except Exception:
                pass
            hint = await bot.send_message(chat_id, f"Допустимый диапазон: {min_val}–{max_val} сек.")
            await asyncio.sleep(3)
            try:
                await hint.delete()
            except Exception:
                pass
            return

        # Delete user's number message
        try:
            await msg.delete()
        except Exception:
            pass

        await db.update_user_field(uid, field, value)
        if field in ("sla", "repeat_interval"):
            await _restart_user_timers(uid, bot)
        state["screen"] = "settings"
        u = await db.get_user_by_id(uid)
        await menu(f"✅ {label}: {_format_seconds(value)}", _settings_kb(u))
        return

    # ---- Custom number input for summary ----
    if screen == "summary_custom":
        target_chat_id = state.get("summary_chat_id")
        thread_id = state.get("summary_thread_id")

        try:
            n = int(text)
        except ValueError:
            try:
                await msg.delete()
            except Exception:
                pass
            hint = await bot.send_message(chat_id, "Введите целое число.")
            await asyncio.sleep(3)
            try:
                await hint.delete()
            except Exception:
                pass
            return

        if not (3 <= n <= 200):
            try:
                await msg.delete()
            except Exception:
                pass
            hint = await bot.send_message(chat_id, "Допустимый диапазон: 3–200.")
            await asyncio.sleep(3)
            try:
                await hint.delete()
            except Exception:
                pass
            return

        # Delete user's number message
        try:
            await msg.delete()
        except Exception:
            pass

        await menu(f"⏳ Генерирую саммари ({n} сообщ.)...", _cancel_kb())
        result = await summary_service.summarize_last_n(
            target_chat_id, n, uid, message_thread_id=thread_id
        )
        parts = format_summary_html(result)
        state["screen"] = "summary_mode"
        await _send_then_menu(
            bot, state, chat_id, parts,
            f"📝 Группа: <b>{state.get('summary_chat_title', '')}</b>\n\nВыберите ещё режим или вернитесь:",
            _summary_mode_kb(),
        )
        return


# ====================================================================
#  КОМАНДЫ САММАРИЗАЦИИ (групповой чат)
# ====================================================================
summary_router = Router()


@summary_router.message(Command("sum"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_sum(msg: Message, bot: Bot):
    """
    /sum N — саммари последних N сообщений.
    По умолчанию N=30.
    """
    if not os.getenv("LLM_API_KEY"):
        await msg.reply("⚠️ Саммаризация не настроена (нет LLM_API_KEY).")
        return

    # Парсим аргумент
    parts = (msg.text or "").split()
    n = 30
    if len(parts) > 1:
        try:
            n = int(parts[1])
        except ValueError:
            await msg.reply("Использование: /sum 30")
            return

    processing = await msg.reply("⏳ Генерирую саммари...")

    result = await summary_service.summarize_last_n(
        chat_id=msg.chat.id,
        n=n,
        initiator_user_id=msg.from_user.id,
        message_thread_id=msg.message_thread_id,
    )

    parts_html = format_summary_html(result)
    try:
        await processing.delete()
    except Exception:
        pass

    for part in parts_html:
        await bot.send_message(
            msg.chat.id, part,
            parse_mode="HTML", disable_web_page_preview=True,
        )


@summary_router.message(Command("sum_thread"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_sum_thread(msg: Message, bot: Bot):
    """
    /sum_thread — саммари текущего треда / reply-цепочки.
    Нужно отправить как reply на сообщение в треде.
    """
    if not os.getenv("LLM_API_KEY"):
        await msg.reply("⚠️ Саммаризация не настроена (нет LLM_API_KEY).")
        return

    # Определяем reply или thread
    reply_msg_id = msg.reply_to_message.message_id if msg.reply_to_message else msg.message_id
    thread_id = msg.message_thread_id

    processing = await msg.reply("⏳ Генерирую саммари треда...")

    result = await summary_service.summarize_thread(
        chat_id=msg.chat.id,
        message_id=reply_msg_id,
        message_thread_id=thread_id,
        initiator_user_id=msg.from_user.id,
    )

    parts_html = format_summary_html(result)
    try:
        await processing.delete()
    except Exception:
        pass

    for part in parts_html:
        await bot.send_message(
            msg.chat.id, part,
            parse_mode="HTML", disable_web_page_preview=True,
        )


@summary_router.message(Command("digest"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_digest(msg: Message, bot: Bot):
    """
    /digest today — дайджест чата за сегодня.
    """
    if not os.getenv("LLM_API_KEY"):
        await msg.reply("⚠️ Саммаризация не настроена (нет LLM_API_KEY).")
        return

    processing = await msg.reply("⏳ Генерирую дайджест за сегодня...")

    result = await summary_service.summarize_today(
        chat_id=msg.chat.id,
        initiator_user_id=msg.from_user.id,
        message_thread_id=msg.message_thread_id,
    )

    parts_html = format_summary_html(result)
    try:
        await processing.delete()
    except Exception:
        pass

    for part in parts_html:
        await bot.send_message(
            msg.chat.id, part,
            parse_mode="HTML", disable_web_page_preview=True,
        )


# ====================================================================
#  ФОНОВАЯ ПРОВЕРКА СОЗВОНОВ
# ====================================================================
async def _check_meetings_loop(bot: Bot):
    while True:
        await asyncio.sleep(30)
        now = datetime.now()

        pending_meetings = await db.get_all_pending_meetings()
        for m in pending_meetings:
            dt_str = m.get("datetime")
            if not dt_str:
                continue
            try:
                dt = datetime.fromisoformat(dt_str)
            except ValueError:
                continue

            user_id = m["user_id"]
            u = await db.get_user_by_id(user_id)
            private_chat_id = u.get("private_chat_id") if u else None
            if not private_chat_id:
                continue

            remind_before = m.get("remind_before", DEFAULT_ZOOM_REMIND)
            reminded = m["reminded"]
            started = m["started_notified"]

            # Напоминание за N минут
            if not reminded and now >= dt - timedelta(seconds=remind_before) and now < dt:
                delta = dt - now
                minutes_left = max(1, int(delta.total_seconds() / 60))
                try:
                    await bot.send_message(
                        private_chat_id,
                        f"🔔 <b>Через {minutes_left} мин. созвон!</b>\n\n{_format_meeting(m)}",
                        parse_mode="HTML", disable_web_page_preview=True,
                    )
                    reminded = True
                    logger.info(f"Zoom-напоминание {m['id']} → user_id={user_id}")
                except Exception as e:
                    logger.error(f"Ошибка Zoom-напоминания {m['id']}: {e}")

            # В момент начала
            if not started and now >= dt:
                try:
                    await bot.send_message(
                        private_chat_id,
                        f"📹 <b>Созвон начинается!</b>\n\n{_format_meeting(m)}",
                        parse_mode="HTML", disable_web_page_preview=True,
                    )
                    started = True
                    logger.info(f"Zoom-старт {m['id']} → user_id={user_id}")
                except Exception as e:
                    logger.error(f"Ошибка Zoom-старта {m['id']}: {e}")

            if reminded != m["reminded"] or started != m["started_notified"]:
                await db.update_meeting_flags(m["id"], reminded, started)

        # Удалить прошедшие (через 1 час)
        cutoff = (now - timedelta(hours=1)).isoformat()
        await db.delete_old_meetings(cutoff)


# ====================================================================
#  ФОНОВАЯ ОЧИСТКА СТАРЫХ СООБЩЕНИЙ
# ====================================================================
async def _cleanup_old_messages_loop():
    """Раз в 6 часов удаляет сообщения старше 30 дней."""
    while True:
        await asyncio.sleep(6 * 3600)
        try:
            await db.delete_old_messages(days=30)
            logger.info("Старые сообщения очищены")
        except Exception as e:
            logger.error(f"Ошибка очистки сообщений: {e}")


# ====================================================================
#  ЗАПУСК
# ====================================================================
async def main():
    await db.init()

    bot = Bot(token=TOKEN)
    dp = Dispatcher()

    dp.include_router(private_router)
    dp.include_router(summary_router)  # до group_router (команды приоритетнее)
    dp.include_router(group_router)

    asyncio.create_task(_check_meetings_loop(bot))
    asyncio.create_task(_cleanup_old_messages_loop())
    logger.info("Бот запущен (aiogram + SQLite + summarization)")
    try:
        await dp.start_polling(bot)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
