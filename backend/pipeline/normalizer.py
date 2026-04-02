# backend/pipeline/normalizer.py

import re

# ─────────────────────────────────────────────
# Нормализация единиц измерения
# ─────────────────────────────────────────────

UNIT_MAP = {
    "куб.м": "м3", "куб м": "м3", "кубометр": "м3", "m3": "м3",
    "кв.м":  "м2", "кв м":  "м2", "квадратный метр": "м2",
    "килограмм": "кг", "кило": "кг", "kg": "кг",
    "тонна": "т",  "тн": "т",
    "штука": "шт", "штук": "шт", "единица": "шт",
    "литр":  "л",  "л.":   "л",
    "метр":  "м",  "м.":   "м",
    "час":   "ч",  "часов": "ч",
}


def normalize_unit(unit: str) -> str:
    if not unit:
        return ""
    u = unit.lower().strip().rstrip(".")
    return UNIT_MAP.get(u, u)


# ─────────────────────────────────────────────
# Канонизация названий позиций
# ─────────────────────────────────────────────

def canonicalize(name: str) -> str:
    """
    Приводит название позиции к единому виду.

    Примеры:
        "бетон М-300"      → "бетон м300"
        "бетон марки М300" → "бетон м300"
        "Арматура А500С"   → "арматура а500с"
        "куб.м"            → "м3"
    """
    if not name:
        return ""

    name = name.lower().strip()

    # Убираем лишние слова
    noise_words = ["марки", "марка", "класса", "класс", "типа", "тип", "сорта", "сорт"]
    for word in noise_words:
        name = re.sub(rf'\b{word}\b', '', name)

    # Убираем дефисы между буквой/числом и числом: М-300 → м300
    name = re.sub(r'([а-яa-z])[-–](\d)', r'\1\2', name)

    # Убираем пробелы между буквой и числом: м 300 → м300
    name = re.sub(r'([а-яa-z])\s+(\d)', r'\1\2', name)

    # Схлопываем множественные пробелы
    name = re.sub(r'\s+', ' ', name).strip()

    return name


# ─────────────────────────────────────────────
# Нормализация списка позиций
# ─────────────────────────────────────────────

def normalize_items(items: list, source: str = "") -> list:
    """
    Нормализует список позиций:
    - canonical_name через canonicalize()
    - unit через normalize_unit()
    - добавляет source
    """
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue

        raw_name = str(item.get("name", "")).strip()
        if not raw_name:
            continue

        normalized = dict(item)
        normalized["name"]           = raw_name
        normalized["canonical_name"] = canonicalize(raw_name)
        normalized["unit"]           = normalize_unit(str(item.get("unit", "")))
        normalized["source"]         = source

        result.append(normalized)

    return result