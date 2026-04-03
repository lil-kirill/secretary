"""
Бизнес-логика саммаризации.
Оркестрирует: сбор сообщений → вызов LLM → сохранение результата.
"""
import logging
import uuid

import db
from llm_client import generate_chat_summary, SummaryResult

logger = logging.getLogger(__name__)

MIN_MESSAGES_FOR_SUMMARY = 3
MAX_MESSAGES_FOR_SUMMARY = 200


async def summarize_last_n(
    chat_id: int,
    n: int,
    initiator_user_id: int,
    message_thread_id: int | None = None,
) -> SummaryResult:
    """
    /sum N — саммари последних N сообщений чата или конкретной темы/ветки.
    """
    n = max(MIN_MESSAGES_FOR_SUMMARY, min(n, MAX_MESSAGES_FOR_SUMMARY))
    messages = await db.get_last_messages(chat_id, limit=n, message_thread_id=message_thread_id)

    if len(messages) < MIN_MESSAGES_FOR_SUMMARY:
        return SummaryResult(brief="Недостаточно сообщений для анализа (минимум 3).")

    job_id = uuid.uuid4().hex[:12]
    await db.create_summary_job(
        job_id=job_id,
        job_type="last_n",
        chat_id=chat_id,
        initiator_user_id=initiator_user_id,
        msg_count=len(messages),
        message_thread_id=message_thread_id,
        window_from=messages[0].get("date"),
        window_to=messages[-1].get("date"),
    )

    try:
        result = await generate_chat_summary(messages, mode="full")
        await db.update_job_status(job_id, "done")
        await db.save_summary(
            summary_id=uuid.uuid4().hex[:12],
            job_id=job_id,
            chat_id=chat_id,
            summary_json=result.to_json(),
            raw_text=result.brief,
        )
        return result
    except Exception as e:
        logger.error(f"summarize_last_n failed: {e}")
        await db.update_job_status(job_id, "error", str(e))
        return SummaryResult(brief=f"⚠️ Ошибка при генерации саммари: {e}")


async def summarize_thread(
    chat_id: int,
    message_id: int,
    message_thread_id: int | None,
    initiator_user_id: int,
) -> SummaryResult:
    """
    /sum_thread — саммари текущего треда или reply-цепочки.
    Приоритет: message_thread_id → reply chain.
    """
    messages = []

    # 1. Если есть thread_id (супергруппа с топиками)
    if message_thread_id:
        messages = await db.get_thread_messages(chat_id, message_thread_id, limit=100)

    # 2. Fallback: reply-цепочка
    if not messages:
        messages = await db.get_reply_chain(chat_id, message_id, max_depth=30)

    # 3. Fallback: контекст вокруг сообщения
    if len(messages) < MIN_MESSAGES_FOR_SUMMARY:
        messages = await db.get_messages_around(chat_id, message_id, window=15)

    if len(messages) < MIN_MESSAGES_FOR_SUMMARY:
        return SummaryResult(
            brief="Недостаточно сообщений в треде/цепочке для анализа.\n"
                  "Попробуйте /sum N для саммари последних сообщений."
        )

    job_id = uuid.uuid4().hex[:12]
    await db.create_summary_job(
        job_id=job_id,
        job_type="thread",
        chat_id=chat_id,
        initiator_user_id=initiator_user_id,
        msg_count=len(messages),
        message_thread_id=message_thread_id,
        source_message_id=message_id,
        window_from=messages[0].get("date"),
        window_to=messages[-1].get("date"),
    )

    try:
        result = await generate_chat_summary(messages, mode="full")
        await db.update_job_status(job_id, "done")
        await db.save_summary(
            summary_id=uuid.uuid4().hex[:12],
            job_id=job_id,
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            summary_json=result.to_json(),
            raw_text=result.brief,
        )
        return result
    except Exception as e:
        logger.error(f"summarize_thread failed: {e}")
        await db.update_job_status(job_id, "error", str(e))
        return SummaryResult(brief=f"⚠️ Ошибка при генерации саммари: {e}")


async def summarize_today(
    chat_id: int,
    initiator_user_id: int,
    message_thread_id: int | None = None,
) -> SummaryResult:
    """
    /digest today — саммари всех сообщений за сегодня (или конкретной темы).
    """
    messages = await db.get_messages_today(chat_id, message_thread_id=message_thread_id)

    if len(messages) < MIN_MESSAGES_FOR_SUMMARY:
        return SummaryResult(brief="За сегодня недостаточно сообщений для дайджеста (минимум 3).")

    # Ограничиваем для LLM
    if len(messages) > MAX_MESSAGES_FOR_SUMMARY:
        messages = messages[-MAX_MESSAGES_FOR_SUMMARY:]

    job_id = uuid.uuid4().hex[:12]
    await db.create_summary_job(
        job_id=job_id,
        job_type="digest_today",
        chat_id=chat_id,
        initiator_user_id=initiator_user_id,
        msg_count=len(messages),
        message_thread_id=message_thread_id,
        window_from=messages[0].get("date"),
        window_to=messages[-1].get("date"),
    )

    try:
        result = await generate_chat_summary(messages, mode="full")
        await db.update_job_status(job_id, "done")
        await db.save_summary(
            summary_id=uuid.uuid4().hex[:12],
            job_id=job_id,
            chat_id=chat_id,
            summary_json=result.to_json(),
            raw_text=result.brief,
        )
        return result
    except Exception as e:
        logger.error(f"summarize_today failed: {e}")
        await db.update_job_status(job_id, "error", str(e))
        return SummaryResult(brief=f"⚠️ Ошибка при генерации дайджеста: {e}")


async def get_mention_context(
    chat_id: int,
    message_id: int,
) -> str:
    """
    Короткое контекстное саммари для вставки в напоминание.
    Берёт ~10 сообщений вокруг упоминания → генерирует 1-2 предложения.
    При ошибке возвращает пустую строку (не ломает reminder flow).
    """
    try:
        messages = await db.get_messages_around(chat_id, message_id, window=10)
        if len(messages) < 2:
            return ""
        result = await generate_chat_summary(messages, mode="context")
        return result.brief if result.brief else ""
    except Exception as e:
        logger.warning(f"get_mention_context failed: {e}")
        return ""
