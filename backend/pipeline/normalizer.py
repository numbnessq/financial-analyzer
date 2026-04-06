# backend/pipeline/normalizer.py
"""
Нормализация позиций.
ВАЖНО: НЕ транслитерируем — оставляем русские названия.
"""
import re
import unicodedata
from typing import Any


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

JUNK_VALUES = {
    "не указан", "unknown", "неизвестно", "неизвестный контрагент",
    "неизвестный отдел", "-", "—", "none", "null", ""
}


def _coerce_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        s = str(value).replace(" ", "").replace(",", ".").strip()
        return float(s) if s else 0.0
    except (TypeError, ValueError):
        return 0.0


def _clean_label(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() in JUNK_VALUES:
        return ""
    return re.sub(r"\s+", " ", s)


def normalize_unit(unit: str) -> str:
    if not unit:
        return ""
    u = str(unit).lower().strip().rstrip(".")
    return UNIT_MAP.get(u, u)


def canonicalize(name: Any) -> str:
    """
    Канонизация названия для matcher.
    Оставляет кириллицу — НЕ транслитерирует.
    """
    if not name:
        return ""

    s = unicodedata.normalize("NFKC", str(name)).strip().lower()
    s = s.replace("ё", "е")

    # Убираем шумовые слова
    for word in NOISE_WORDS:
        s = re.sub(rf'\b{word}\b', '', s)

    # Убираем диапазоны: 20-40, 5/10
    s = re.sub(r'\b\d+\s*[-/×x]\s*\d+\b', ' ', s)

    # Убираем дефис между буквой и цифрой: м-300 → м300
    s = re.sub(r'([а-яёa-z])[-–](\d)', r'\1\2', s)

    # Убираем пробел между буквой и цифрой: м 300 → м300
    s = re.sub(r'([а-яёa-z])\s+(\d)', r'\1\2', s)

    # Схлопываем пробелы
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# normalize_text нужен только scorer'у для VAGUE_KEYWORDS — оставляем кириллицу
def normalize_text(text: Any) -> str:
    if not text:
        return ""
    s = unicodedata.normalize("NFKC", str(text)).strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def normalize_item(item: dict, source: str = "") -> dict:
    if not isinstance(item, dict):
        return {}

    raw_name = item.get("name") or item.get("item_name") or item.get("canonical_name") or ""
    name        = _clean_label(raw_name)
    department  = _clean_label(item.get("department", ""))
    contractor  = _clean_label(item.get("contractor", ""))
    source_file = _clean_label(item.get("source_file") or item.get("source") or source)

    normalized = {
        "name":           name,
        "canonical_name": canonicalize(name),
        "price":          _coerce_float(item.get("price", 0)),
        "quantity":       _coerce_float(item.get("quantity", 1) or 1),
        "department":     department,
        "contractor":     contractor,
        "source_file":    source_file,
        "source":         source_file,
        "unit":           normalize_unit(item.get("unit", "")),
    }

    for key in ("date", "currency", "description", "vat", "code"):
        if key in item:
            normalized[key] = item[key]

    return normalized


def normalize_items(items: list, source: str = "") -> list:
    result = []
    for item in items or []:
        n = normalize_item(item, source=source)
        if n.get("name"):
            result.append(n)
    return result