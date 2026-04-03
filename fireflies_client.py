"""
GraphQL-клиент для Fireflies.ai API.
Получение списка транскриптов и детальных данных.
"""
import json
import logging
import os

import httpx

logger = logging.getLogger(__name__)

FIREFLIES_API_URL = "https://api.fireflies.ai/graphql"
FIREFLIES_API_KEY = os.getenv("FIREFLIES_API_KEY", "")
FIREFLIES_TIMEOUT = int(os.getenv("FIREFLIES_TIMEOUT", "30"))


def is_configured() -> bool:
    """True, если API-ключ задан."""
    return bool(FIREFLIES_API_KEY)


async def _graphql(query: str, variables: dict | None = None) -> dict:
    """Выполняет GraphQL-запрос к Fireflies."""
    if not FIREFLIES_API_KEY:
        raise RuntimeError("FIREFLIES_API_KEY не задан")

    headers = {
        "Authorization": f"Bearer {FIREFLIES_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    async with httpx.AsyncClient(timeout=FIREFLIES_TIMEOUT) as client:
        resp = await client.post(FIREFLIES_API_URL, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    if "errors" in data:
        errors = data["errors"]
        msg = errors[0].get("message", str(errors)) if errors else "Unknown GraphQL error"
        raise RuntimeError(f"Fireflies GraphQL error: {msg}")

    return data.get("data", {})


# ------------------------------------------------------------------
#  ЗАПРОСЫ
# ------------------------------------------------------------------

TRANSCRIPTS_LIST_QUERY = """
query {
  transcripts {
    id
    title
    date
    duration
  }
}
"""

TRANSCRIPT_DETAIL_QUERY = """
query($id: String!) {
  transcript(id: $id) {
    id
    title
    date
    duration
    transcript_url
    audio_url
    video_url
    sentences {
      speaker_name
      text
      raw_text
      start_time
      end_time
    }
    speakers {
      id
      name
    }
    meeting_attendees {
      displayName
      email
    }
    summary {
      overview
      action_items
      outline
      keywords
      topics_discussed
    }
  }
}
"""


async def get_recent_transcripts(limit: int = 20) -> list[dict]:
    """
    Получает список недавних транскриптов (id, title, date, duration).
    """
    try:
        data = await _graphql(TRANSCRIPTS_LIST_QUERY)
        transcripts = data.get("transcripts") or []
        return transcripts[:limit]
    except Exception as e:
        logger.error(f"Ошибка получения списка транскриптов: {e}")
        return []


async def get_transcript(transcript_id: str) -> dict | None:
    """
    Получает полные данные транскрипта: реплики, спикеры, саммари.
    Возвращает нормализованный dict.
    """
    try:
        data = await _graphql(TRANSCRIPT_DETAIL_QUERY, {"id": transcript_id})
        raw = data.get("transcript")
        if not raw:
            logger.warning(f"Транскрипт {transcript_id} не найден")
            return None
        return _normalize_transcript(raw)
    except Exception as e:
        logger.error(f"Ошибка получения транскрипта {transcript_id}: {e}")
        return None


def _normalize_transcript(raw: dict) -> dict:
    """Нормализует ответ Fireflies в удобный формат."""
    summary_raw = raw.get("summary") or {}

    # Собираем полный текст из реплик
    sentences = raw.get("sentences") or []
    full_text = "\n".join(
        f"{s.get('speaker_name', '?')}: {s.get('text', '')}"
        for s in sentences
    )

    # Action items — Fireflies возвращает как список строк
    action_items = summary_raw.get("action_items") or []
    if isinstance(action_items, str):
        action_items = [line.strip("- •") for line in action_items.split("\n") if line.strip()]

    return {
        "id": raw.get("id"),
        "title": raw.get("title") or "Без названия",
        "date": raw.get("date"),
        "duration": raw.get("duration"),
        "transcript_url": raw.get("transcript_url"),
        "audio_url": raw.get("audio_url"),
        "video_url": raw.get("video_url"),
        "sentences": sentences,
        "speakers": raw.get("speakers") or [],
        "attendees": raw.get("meeting_attendees") or [],
        "full_text": full_text,
        "summary": {
            "overview": summary_raw.get("overview") or "",
            "action_items": action_items,
            "outline": summary_raw.get("outline") or [],
            "keywords": summary_raw.get("keywords") or [],
            "topics_discussed": summary_raw.get("topics_discussed") or [],
        },
    }
