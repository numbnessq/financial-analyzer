# backend/pipeline/ai_extractor.py

import json
import logging
import requests
import re
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Адрес Ollama (запущена локально)
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "mistral"

# Максимальное количество попыток при ошибке
MAX_RETRIES = 3

# Улучшенный промпт с контрагентами и датами
PROMPT_TEMPLATE = """Ты — система извлечения структурированных данных из документов закупок.

Задача: извлечь из текста товары, материалы, услуги, работы, контрагентов и даты.

Верни ТОЛЬКО JSON. Без комментариев. Без объяснений. Без markdown.

Формат:
[
  {{
    "name": "название позиции",
    "quantity": 1,
    "unit": "шт",
    "price": 0,
    "contractor": "поставщик или исполнитель",
    "date": "дата в формате YYYY-MM-DD"
  }}
]

Правила:
- Если данных нет — верни пустой список []
- Извлекай: товары, услуги, работы с ценами
- Извлекай: контрагенты (поставщики, подрядчики, исполнители)
- Извлекай: даты выполнения, поставки, оказания услуг
- quantity → число
- unit → коротко ("шт", "кг", "м2", "л")
- price → число (если есть)
- contractor → полное или краткое название организации
- date → в формате YYYY-MM-DD (если можно определить)
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


def extract_dates_from_text(text: str) -> list[str]:
    """Извлекает даты из текста простым regex'ом"""
    # Ищем даты в форматах: DD.MM.YYYY, DD/MM/YYYY, YYYY-MM-DD
    patterns = [
        r'\b(\d{1,2}[./\-]\d{1,2}[./\-]\d{4})\b',  # DD.MM.YYYY или DD/MM/YYYY
        r'\b(\d{4}[./\-]\d{1,2}[./\-]\d{1,2})\b',  # YYYY-MM-DD
    ]

    dates = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            try:
                # Пробуем распарсить дату
                if '.' in match:
                    dt = datetime.strptime(match, '%d.%m.%Y')
                elif '/' in match:
                    dt = datetime.strptime(match, '%d/%m/%Y')
                else:
                    dt = datetime.strptime(match, '%Y-%m-%d')
                dates.append(dt.strftime('%Y-%m-%d'))
            except:
                continue

    return list(set(dates))  # Уникальные даты


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
                "stream": False,
                "temperature": 0
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

        # Фильтруем мусорные позиции и добавляем даты
        filtered_items = []
        for item in items:
            if isinstance(item, dict):
                name = str(item.get("name", "")).strip()
                price = float(item.get("price", 0) or 0)

                # Если есть имя и цена, или есть контрагент/дата - сохраняем
                if (len(name) >= 3 and price > 0) or item.get("contractor") or item.get("date"):
                    # Добавляем даты из текста если их нет
                    if not item.get("date"):
                        dates = extract_dates_from_text(text)
                        if dates:
                            item["date"] = dates[0]  # Берем первую найденную дату

                    filtered_items.append(item)

        logger.info(f"Успешно извлечено {len(filtered_items)} позиций.")
        return filtered_items

    logger.error("Все попытки исчерпаны. Возвращаем пустой список.")
    return []
