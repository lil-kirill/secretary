"""
Детекция ссылок на конференции в Telegram-сообщениях.
Поддерживает: Zoom, Google Meet, Microsoft Teams.
Расширяемый список провайдеров.
"""
import re
from urllib.parse import urlparse, urlencode, parse_qs

from aiogram.types import Message


# ====================================================================
#  ПРОВАЙДЕРЫ
# ====================================================================

PROVIDERS = [
    {
        "name": "Zoom",
        "patterns": [
            re.compile(r"https?://[\w.]*zoom\.us/j/[\d]+[\w?=&./-]*", re.IGNORECASE),
            re.compile(r"https?://[\w.]*zoom\.us/my/[\w./-]+", re.IGNORECASE),
        ],
    },
    {
        "name": "Google Meet",
        "patterns": [
            re.compile(r"https?://meet\.google\.com/[\w-]+", re.IGNORECASE),
        ],
    },
    {
        "name": "Microsoft Teams",
        "patterns": [
            re.compile(r"https?://teams\.microsoft\.com/l/meetup-join/\S+", re.IGNORECASE),
            re.compile(r"https?://teams\.live\.com/meet/\S+", re.IGNORECASE),
        ],
    },
]


# ====================================================================
#  НОРМАЛИЗАЦИЯ
# ====================================================================

def _normalize_url(url: str) -> str:
    """Убирает мусорные символы в конце и лишние query-параметры."""
    # Убираем trailing пунктуацию (часто прилипает из текста)
    url = url.rstrip(".,;:!?)>\"'")

    parsed = urlparse(url)

    # Для Zoom: оставляем только pwd
    if "zoom.us" in parsed.netloc:
        qs = parse_qs(parsed.query)
        keep = {}
        if "pwd" in qs:
            keep["pwd"] = qs["pwd"][0]
        clean_query = urlencode(keep) if keep else ""
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}" + (f"?{clean_query}" if clean_query else "")

    # Для остальных: оставляем как есть, убрав trailing мусор
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}" + (f"?{parsed.query}" if parsed.query else "")


def _extract_meeting_code(url: str, provider_name: str) -> str | None:
    """Извлекает код/ID встречи из URL."""
    parsed = urlparse(url)

    if provider_name == "Zoom":
        m = re.search(r"/j/(\d+)", parsed.path)
        return m.group(1) if m else None

    if provider_name == "Google Meet":
        parts = parsed.path.strip("/").split("/")
        return parts[0] if parts else None

    return None


def _extract_passcode(url: str, provider_name: str) -> str | None:
    """Извлекает код доступа из URL."""
    if provider_name == "Zoom":
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        if "pwd" in qs:
            return qs["pwd"][0]
    return None


def format_meeting_id(raw_id: str) -> str:
    """Форматирует ID встречи с пробелами: 86410579715 → 864 1057 9715."""
    digits = re.sub(r"\D", "", raw_id)
    if len(digits) <= 4:
        return digits
    # Разбиваем: 3 + по 4
    parts = [digits[:3]]
    rest = digits[3:]
    parts.extend([rest[i:i+4] for i in range(0, len(rest), 4)])
    return " ".join(parts)


# ====================================================================
#  ДЕТЕКЦИЯ
# ====================================================================

def _find_urls_in_text(text: str) -> list[str]:
    """Находит все URL в тексте через regex."""
    if not text:
        return []
    return re.findall(r"https?://\S+", text)


def _find_urls_in_entities(text: str | None, entities: list | None) -> list[str]:
    """Извлекает URL из entities (url и text_link типы)."""
    if not entities:
        return []
    urls = []
    for ent in entities:
        if ent.type == "url" and text:
            urls.append(text[ent.offset : ent.offset + ent.length])
        elif ent.type == "text_link" and ent.url:
            urls.append(ent.url)
    return urls


def _match_provider(url: str) -> dict | None:
    """Сопоставляет URL с провайдером. Возвращает dict или None."""
    for provider in PROVIDERS:
        for pattern in provider["patterns"]:
            if pattern.search(url):
                clean_url = url.rstrip(".,;:!?)>\"'")
                normalized = _normalize_url(clean_url)
                return {
                    "provider": provider["name"],
                    "url": clean_url,
                    "normalized_url": normalized,
                    "meeting_code": _extract_meeting_code(normalized, provider["name"]),
                    "passcode": _extract_passcode(clean_url, provider["name"]),
                }
    return None


def detect_conference_links(msg: Message) -> list[dict]:
    """
    Главная функция: находит все ссылки на конференции в сообщении.

    Проверяет:
    - msg.text
    - msg.caption
    - msg.entities (url, text_link)
    - msg.caption_entities (url, text_link)

    Возвращает список dict:
    [{"provider": "Zoom", "url": "...", "normalized_url": "...", "meeting_code": "..."}, ...]

    Дедупликация по normalized_url.
    """
    all_urls: list[str] = []

    # Текст и caption
    all_urls.extend(_find_urls_in_text(msg.text))
    all_urls.extend(_find_urls_in_text(msg.caption))

    # Entities
    all_urls.extend(_find_urls_in_entities(msg.text, msg.entities))
    all_urls.extend(_find_urls_in_entities(msg.caption, msg.caption_entities))

    # Сопоставляем с провайдерами и дедуплицируем
    seen: set[str] = set()
    results: list[dict] = []

    for url in all_urls:
        match = _match_provider(url)
        if match and match["normalized_url"] not in seen:
            seen.add(match["normalized_url"])
            results.append(match)

    return results
