# backend/pipeline/normalizer.py
import re

UNIT_MAP = {
    "куб.м": "м3", "куб м": "м3", "кубометр": "м3", "m3": "м3",
    "кв.м":  "м2", "кв м":  "м2", "квадратный метр": "м2",
    "килограмм": "кг", "кило": "кг", "kg": "кг",
    "тонна": "т",  "тн": "т",
    "штука": "шт", "штук": "шт", "единица": "шт",
    "литр":  "л",  "л.": "л",
    "метр":  "м",  "м.": "м",
    "час":   "ч",  "часов": "ч",
}

NOISE_WORDS = [
    "марки", "марка", "класса", "класс", "типа", "тип",
    "сорта", "сорт", "строительный", "строительная", "строительного",
    "фракции", "фракция", "размером", "размер",
    "рядовой", "крупный", "мелкий", "средний",
    "гравийный", "гранитный", "известняковый",
]


def normalize_unit(unit: str) -> str:
    if not unit:
        return ""
    u = unit.lower().strip().rstrip(".")
    return UNIT_MAP.get(u, u)


def canonicalize(name: str) -> str:
    if not name:
        return ""
    name = name.lower().strip()
    for word in NOISE_WORDS:
        name = re.sub(rf'\b{word}\b', '', name)
    name = re.sub(r'\b\d+\s*[-/×x]\s*\d+\b', '', name)
    name = re.sub(r'([а-яёa-z])[-–](\d)', r'\1\2', name)
    name = re.sub(r'([а-яёa-z])\s+(\d)', r'\1\2', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def normalize_items(items: list, source: str = "") -> list:
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_name = str(item.get("name", "")).strip()
        if not raw_name:
            continue
        result.append({
            "name":           raw_name,
            "canonical_name": canonicalize(raw_name),
            "price":          _to_float(item.get("price", 0)),
            "quantity":       _to_float(item.get("quantity", 1)),
            "unit":           normalize_unit(str(item.get("unit", ""))),
            "source":         source or item.get("source", ""),
            "department":     item.get("department", ""),
            "contractor":     item.get("contractor", ""),
            "date":           item.get("date", None),
        })
    return result


def _to_float(value) -> float:
    try:
        return float(str(value).replace(",", ".").strip() or 0)
    except (ValueError, TypeError):
        return 0.0