# backend/pipeline/ai_extractor.py

import json
import logging
import requests

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Адрес Ollama (запущена локально)
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "mistral"

# Максимальное количество попыток при ошибке
MAX_RETRIES = 3

# Промпт — инструкция для модели
PROMPT_TEMPLATE = """Ты — система извлечения структурированных данных.

Задача: извлечь из текста список позиций (товары, материалы, объекты).

Верни ТОЛЬКО JSON. Без комментариев. Без объяснений. Без markdown.

Формат:
[
  {{
    "name": "название позиции",
    "quantity": 1,
    "unit": "шт"
  }}
]

Правила:
- Если данных нет — верни пустой список []
- Не выдумывай значения
- quantity → число (если "100 шт" → 100)
- unit → коротко ("шт", "кг", "м2", "л")
- name → очищенное название без мусора

Текст:
\"\"\"
{TEXT}
\"\"\"
"""


def safe_parse_json(response: str) -> list | None:
    """
    Пытается распарсить JSON из ответа модели.
    """
    # Попытка 1 — парсим как есть
    try:
        return json.loads(response)
    except json.JSONDecodeError:
        pass

    # Попытка 2 — ищем JSON массив внутри текста
    try:
        start = response.find("[")
        end = response.rfind("]") + 1
        if start != -1 and end > start:
            json_str = response[start:end]
            return json.loads(json_str)
    except json.JSONDecodeError:
        pass

    # Попытка 3 — убираем markdown блоки ```json ... ```
    try:
        cleaned = response.replace("```json", "").replace("```", "").strip()
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    return None


def call_ollama(prompt: str) -> str | None:
    """
    Отправляет запрос к Ollama и возвращает текст ответа.
    """
    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL_NAME,
                "prompt": prompt,
                "stream": False
            },
            timeout=120
        )
        response.raise_for_status()
        return response.json().get("response", "")

    except requests.exceptions.ConnectionError:
        logger.error("Ollama не запущена! Запусти приложение Ollama.")
        return None
    except requests.exceptions.Timeout:
        logger.error("Ollama не ответила за 120 секунд.")
        return None
    except Exception as e:
        logger.error(f"Ошибка запроса к Ollama: {e}")
        return None


def extract_items(text: str) -> list:
    """
    Главная функция — извлекает позиции из текста через AI.
    Возвращает список словарей или пустой список при ошибке.
    """
    # Если текст слишком короткий
    if not text or len(text.strip()) < 10:
        logger.warning("Текст слишком короткий для анализа.")
        return []

    # Обрезаем текст если он очень длинный
    max_text_length = 3000
    if len(text) > max_text_length:
        logger.info(f"Текст обрезан с {len(text)} до {max_text_length} символов.")
        text = text[:max_text_length]

    # Подставляем текст в промпт
    prompt = PROMPT_TEMPLATE.format(TEXT=text)

    # Пробуем MAX_RETRIES раз
    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"Попытка {attempt}/{MAX_RETRIES}...")

        raw_response = call_ollama(prompt)

        if raw_response is None:
            logger.warning(f"Попытка {attempt} — нет ответа от модели.")
            continue

        logger.info(f"Ответ модели: {raw_response[:200]}...")

        items = safe_parse_json(raw_response)

        if items is None:
            logger.warning(f"Попытка {attempt} — не удалось распарсить JSON.")
            continue

        if not isinstance(items, list):
            logger.warning(f"Попытка {attempt} — ответ не является списком.")
            continue

        logger.info(f"Успешно извлечено {len(items)} позиций.")
        return items

    logger.error("Все попытки исчерпаны. Возвращаем пустой список.")
    return []