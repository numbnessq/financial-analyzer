# backend/pipeline/normalizer.py

import re

# Словарь стандартизации единиц измерения
# Ключи — варианты написания, значения — стандартная форма
UNIT_SYNONYMS = {
    # Штуки
    "штук": "шт",
    "штука": "шт",
    "шт.": "шт",
    "piece": "шт",
    "pcs": "шт",
    "ед": "шт",
    "ед.": "шт",

    # Килограммы
    "килограмм": "кг",
    "килограммов": "кг",
    "кг.": "кг",
    "kg": "кг",
    "kilo": "кг",

    # Граммы
    "грамм": "г",
    "граммов": "г",
    "гр": "г",
    "gr": "г",
    "g": "г",

    # Литры
    "литр": "л",
    "литров": "л",
    "литра": "л",
    "liter": "л",
    "litre": "л",
    "lt": "л",

    # Метры
    "метр": "м",
    "метров": "м",
    "метра": "м",
    "meter": "м",
    "metre": "м",

    # Квадратные метры
    "м2": "м²",
    "м кв": "м²",
    "м.кв": "м²",
    "кв.м": "м²",
    "кв м": "м²",
    "m2": "м²",
    "sq m": "м²",

    # Кубические метры
    "м3": "м³",
    "куб": "м³",
    "куб.м": "м³",
    "м.куб": "м³",
    "кубометр": "м³",
    "m3": "м³",
    "cubic m": "м³",

    # Тонны
    "тонна": "т",
    "тонн": "т",
    "тонны": "т",
    "ton": "т",
    "tonne": "т",
}

# Мусор который нужно убрать из названий
NOISE_WORDS = [
    "итого", "всего", "сумма", "total", "sum",
    "в т.ч.", "в том числе", "including",
    "№", "n/a", "н/д",
]


def clean_name(name: str) -> str:
    """
    Очищает название позиции:
    - убирает лишние пробелы
    - убирает спецсимволы в начале/конце
    - убирает мусорные слова
    - приводит к нижнему регистру
    """
    if not name:
        return ""

    # Убираем лишние пробелы
    name = name.strip()

    # Убираем спецсимволы в начале и конце (но не внутри)
    name = re.sub(r'^[^\w]+|[^\w]+$', '', name)

    # Убираем двойные пробелы
    name = re.sub(r'\s+', ' ', name)

    # Приводим к нижнему регистру
    name = name.lower()

    # Убираем мусорные слова
    for noise in NOISE_WORDS:
        name = name.replace(noise.lower(), "").strip()

    # Убираем двойные пробелы снова (после удаления слов)
    name = re.sub(r'\s+', ' ', name).strip()

    return name


def normalize_unit(unit: str) -> str:
    """
    Стандартизирует единицу измерения.
    Например: "куб" → "м³", "кг." → "кг"
    """
    if not unit:
        return ""

    # Приводим к нижнему регистру и убираем пробелы
    unit = unit.strip().lower()

    # Ищем в словаре синонимов
    if unit in UNIT_SYNONYMS:
        return UNIT_SYNONYMS[unit]

    return unit


def normalize_quantity(quantity) -> float:
    """
    Приводит количество к числу с плавающей точкой.
    Обрабатывает строки вида "100 шт", "1,5" и т.д.
    """
    if isinstance(quantity, (int, float)):
        return max(0.0, float(quantity))

    if isinstance(quantity, str):
        # Убираем всё кроме цифр, точки и запятой
        quantity = quantity.strip()
        quantity = re.sub(r'[^\d.,]', '', quantity)

        # Заменяем запятую на точку (европейский формат)
        quantity = quantity.replace(',', '.')

        try:
            return max(0.0, float(quantity))
        except ValueError:
            return 0.0

    return 0.0


def normalize_item(item: dict) -> dict:
    """
    Нормализует одну позицию — применяет все функции очистки.
    Принимает словарь, возвращает нормализованный словарь.
    """
    return {
        "name": clean_name(item.get("name", "")),
        "quantity": normalize_quantity(item.get("quantity", 0)),
        "unit": normalize_unit(item.get("unit", "")),
        "price": normalize_quantity(item.get("price", 0)),
        "source": item.get("source", "").strip(),
    }


def normalize_items(items: list, source: str = "") -> list:
    """
    Нормализует список позиций.
    Добавляет source (имя файла) к каждой позиции.
    Пропускает позиции с пустым именем после очистки.
    """
    result = []

    for item in items:
        # Добавляем source если его нет
        if not item.get("source"):
            item["source"] = source

        normalized = normalize_item(item)

        # Пропускаем позиции с пустым названием
        if not normalized["name"]:
            continue

        result.append(normalized)

    return result