"""
Форматирование SummaryResult в HTML для Telegram.
Учитывает лимит 4096 символов на сообщение.
"""
from html import escape
from llm_client import SummaryResult

TG_MSG_LIMIT = 4096


def format_summary_html(result: SummaryResult) -> list[str]:
    """
    Форматирует SummaryResult в список HTML-сообщений для Telegram.
    Если текст не влезает в одно сообщение — разбивает на части.
    """
    sections: list[str] = []

    # Brief
    if result.brief:
        sections.append(f"📝 <b>Саммари</b>\n{escape(result.brief)}")

    # Decisions
    if result.decisions:
        lines = ["✅ <b>Решения</b>"]
        for d in result.decisions:
            owner = f" ({escape(d.owner)})" if d.owner else ""
            lines.append(f"  • {escape(d.text)}{owner}")
        sections.append("\n".join(lines))

    # Tasks
    if result.tasks:
        lines = ["📋 <b>Задачи</b>"]
        for t in result.tasks:
            prio = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(t.priority, "⚪")
            owner = f" → {escape(t.owner)}" if t.owner else ""
            lines.append(f"  {prio} {escape(t.text)}{owner}")
        sections.append("\n".join(lines))

    # Questions
    if result.questions:
        lines = ["❓ <b>Открытые вопросы</b>"]
        for q in result.questions:
            owner = f" ({escape(q.owner)})" if q.owner else ""
            lines.append(f"  • {escape(q.text)}{owner}")
        sections.append("\n".join(lines))

    if not sections:
        sections.append("Нет данных для отображения.")

    # Собираем в сообщения с учётом лимита
    full_text = "\n\n".join(sections)
    return _split_message(full_text)


def format_context_html(context: str) -> str:
    """Форматирует контекст для вставки в напоминание."""
    if not context:
        return ""
    return f"\n\n💬 <b>Контекст:</b> {escape(context)}"


def _split_message(text: str, limit: int = TG_MSG_LIMIT) -> list[str]:
    """
    Разбивает длинный текст на части по лимиту.
    Разбивает по абзацам (\n\n), не по символам.
    """
    if len(text) <= limit:
        return [text]

    parts: list[str] = []
    current = ""

    for paragraph in text.split("\n\n"):
        if len(current) + len(paragraph) + 2 <= limit:
            current = f"{current}\n\n{paragraph}" if current else paragraph
        else:
            if current:
                parts.append(current)
            # Если один абзац > limit — режем по строкам
            if len(paragraph) > limit:
                for line in paragraph.split("\n"):
                    if len(current) + len(line) + 1 <= limit:
                        current = f"{current}\n{line}" if current else line
                    else:
                        if current:
                            parts.append(current)
                        current = line[:limit]
            else:
                current = paragraph

    if current:
        parts.append(current)

    return parts if parts else [text[:limit]]
