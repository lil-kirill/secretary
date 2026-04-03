"""
LLM-абстракция для саммаризации.
По умолчанию — OpenAI-совместимый API.
Легко переключить на Claude, локальную модель или другой провайдер.
"""
import json
import logging
import os
from dataclasses import dataclass, field, asdict

import httpx

logger = logging.getLogger(__name__)

# ---- Настройки ----
LLM_API_URL = os.getenv("LLM_API_URL", "https://api.openai.com/v1/chat/completions")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "60"))


# ---- Структуры данных ----
@dataclass
class Decision:
    text: str
    owner: str | None = None
    due: str | None = None
    source_message_ids: list[int] = field(default_factory=list)


@dataclass
class Task:
    text: str
    owner: str | None = None
    due: str | None = None
    priority: str = "medium"
    source_message_ids: list[int] = field(default_factory=list)


@dataclass
class Question:
    text: str
    owner: str | None = None
    source_message_ids: list[int] = field(default_factory=list)


@dataclass
class SummaryResult:
    brief: str = ""
    decisions: list[Decision] = field(default_factory=list)
    tasks: list[Task] = field(default_factory=list)
    questions: list[Question] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "brief": self.brief,
            "decisions": [asdict(d) for d in self.decisions],
            "tasks": [asdict(t) for t in self.tasks],
            "questions": [asdict(q) for q in self.questions],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict) -> "SummaryResult":
        return cls(
            brief=data.get("brief", ""),
            decisions=[Decision(**d) for d in data.get("decisions", [])],
            tasks=[Task(**t) for t in data.get("tasks", [])],
            questions=[Question(**q) for q in data.get("questions", [])],
        )


# ---- Промпты ----
SYSTEM_PROMPT = """Ты — ассистент-аналитик корпоративных чатов.
Твоя задача — делать краткие структурированные саммари переписок на русском языке.

Правила:
- Всегда отвечай на русском языке, даже если переписка на английском.
- Выделяй принятые решения, задачи и открытые вопросы.
- Указывай кто за что отвечает, если это явно видно из переписки.
- Не выдумывай то, чего нет в переписке.
- Формат ответа — строго JSON.
"""

SUMMARY_PROMPT_TEMPLATE = """Проанализируй переписку и верни JSON строго в таком формате:

{{
  "brief": "1-3 предложения: о чём переписка, ключевые выводы",
  "decisions": [
    {{
      "text": "что решили",
      "owner": "кто отвечает или null",
      "due": "YYYY-MM-DDTHH:MM:SS или null",
      "source_message_ids": [1, 2]
    }}
  ],
  "tasks": [
    {{
      "text": "что нужно сделать",
      "owner": "кто отвечает или null",
      "due": "YYYY-MM-DDTHH:MM:SS или null",
      "priority": "low|medium|high",
      "source_message_ids": [3]
    }}
  ],
  "questions": [
    {{
      "text": "открытый вопрос, на который не ответили",
      "owner": "кто задал или null",
      "source_message_ids": [4]
    }}
  ]
}}

Если какой-то раздел пустой — верни пустой массив [].
Не добавляй комментариев или текста вне JSON.

Переписка:
{messages}"""

CONTEXT_PROMPT_TEMPLATE = """Дай ОЧЕНЬ краткое описание контекста переписки (1-2 предложения).
Формат ответа — простой текст (НЕ JSON).
Суть: о чём говорят и чего ждут от упомянутого пользователя.

Переписка:
{messages}"""


def _format_messages_for_prompt(messages: list[dict]) -> str:
    """Форматирует список сообщений в текст для промпта."""
    lines = []
    for m in messages:
        name = m.get("first_name") or m.get("username") or f"user_{m.get('from_user_id', '?')}"
        if m.get("username"):
            name += f" (@{m['username']})"
        date = m.get("date", "")
        text = m.get("text", "").strip()
        if not text:
            text = "[медиа]"
        msg_id = m.get("message_id", "")
        lines.append(f"[{msg_id}] {date} {name}: {text}")
    return "\n".join(lines)


async def generate_chat_summary(
    messages: list[dict],
    mode: str = "full",
    language: str = "ru",
) -> SummaryResult:
    """
    Генерирует саммари переписки.
    mode:
      "full"    — полный формат (brief + decisions + tasks + questions)
      "context" — короткий контекст для напоминания (только brief)
    """
    if not messages:
        return SummaryResult(brief="Нет сообщений для анализа.")

    formatted = _format_messages_for_prompt(messages)

    if mode == "context":
        user_prompt = CONTEXT_PROMPT_TEMPLATE.format(messages=formatted)
    else:
        user_prompt = SUMMARY_PROMPT_TEMPLATE.format(messages=formatted)

    try:
        raw = await _call_llm(user_prompt)
    except Exception as e:
        logger.error(f"LLM вызов не удался: {e}")
        return SummaryResult(brief=f"⚠️ Ошибка генерации: {e}")

    if mode == "context":
        return SummaryResult(brief=raw.strip())

    return _parse_summary_json(raw)


async def _call_llm(user_prompt: str) -> str:
    """Вызывает OpenAI-совместимый API."""
    if not LLM_API_KEY:
        raise RuntimeError("LLM_API_KEY не задан (переменная окружения)")

    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": LLM_MODEL,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    }

    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        resp = await client.post(LLM_API_URL, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    return data["choices"][0]["message"]["content"]


def _parse_summary_json(raw: str) -> SummaryResult:
    """Парсит JSON из ответа LLM с fallback на plain text."""
    # Убираем markdown-обёртку ```json ... ```
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        # Убираем первую и последнюю строки (```json и ```)
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()

    try:
        data = json.loads(cleaned)
        return SummaryResult.from_dict(data)
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        logger.warning(f"Не удалось распарсить JSON от LLM: {e}")
        # Fallback: используем весь ответ как brief
        return SummaryResult(brief=raw.strip()[:1000])
