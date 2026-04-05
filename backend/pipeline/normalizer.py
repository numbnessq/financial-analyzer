# backend/pipeline/normalizer.py
import re
import unicodedata
from typing import Any


UNIT_MAP = {
    "куб.м": "м3", "куб м": "м3", "кубометр": "м3", "m3": "м3",
    "кв.м": "м2", "кв м": "м2", "квадратный метр": "м2",
    "килограмм": "кг", "кило": "кг", "kg": "кг",
    "тонна": "т", "тн": "т",
    "штука": "шт", "штук": "шт", "единица": "шт",
    "литр": "л", "л.": "л",
    "метр": "м", "м.": "м",
    "час": "ч", "часов": "ч",
}

NOISE_WORDS = [
    "марки", "марка", "класса", "класс", "типа", "тип",
    "сорта", "сорт", "строительный", "строительная", "строительного",
    "фракции", "фракция", "размером", "размер",
    "рядовой", "крупный", "мелкий", "средний",
    "гравийный", "гранитный", "известняковый",
]

CYR_TO_LAT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
    "ы": "y", "э": "e", "ю": "yu", "я": "ya", "ь": "", "ъ": "",
}

JUNK_VALUES = {
    "не указан", "unknown", "неизвестно", "неизвестный контрагент",
    "неизвестный отдел", "-", "—", "none", "null", ""
}


def _coerce_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        s = str(value).replace(" ", "").replace(",", ".").strip()
        if not s:
            return 0.0
        return float(s)
    except (TypeError, ValueError):
        return 0.0


def _clean_label(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if s.lower() in JUNK_VALUES:
        return ""
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_unit(unit: str) -> str:
    if not unit:
        return ""
    u = str(unit).lower().strip().rstrip(".")
    return UNIT_MAP.get(u, u)


def transliterate_ru_to_lat(text: str) -> str:
    if not text:
        return ""
    out = []
    for ch in text:
        out.append(CYR_TO_LAT.get(ch, ch))
    return "".join(out)


def normalize_text(text: Any) -> str:
    """
    Унифицированная нормализация для matching:
    - NFKC
    - lower
    - translit RU -> LAT
    - удаление спецсимволов
    - collapse whitespace
    """
    if text is None:
        return ""

    s = unicodedata.normalize("NFKC", str(text))
    s = s.strip().lower().replace("ё", "е")
    s = transliterate_ru_to_lat(s)

    # удаляем спецсимволы, оставляем буквы/цифры/пробелы
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def canonicalize(name: Any) -> str:
    """
    Канонизация позиции для matcher.
    Важно: vague-слова не удаляем, потому что они нужны scorer’у.
    """
    s = normalize_text(name)
    if not s:
        return ""

    # убираем дублирующиеся пробелы и простые размерности
    s = re.sub(r"\b\d+\s*[-/×x]\s*\d+\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_item(item: dict, source: str = "") -> dict:
    """
    Приводит item к обязательной схеме:
    {
      name, price, quantity, department, contractor, source_file
    }
    """
    if not isinstance(item, dict):
        return {}

    raw_name = item.get("name") or item.get("item_name") or item.get("canonical_name") or ""
    name = _clean_label(raw_name)
    department = _clean_label(item.get("department", ""))
    contractor = _clean_label(item.get("contractor", ""))
    source_file = _clean_label(item.get("source_file", "")) or _clean_label(item.get("source", "")) or _clean_label(source)

    normalized = {
        "name": name,
        "canonical_name": canonicalize(name),
        "price": _coerce_float(item.get("price", 0)),
        "quantity": _coerce_float(item.get("quantity", 1) or 1),
        "department": department,
        "contractor": contractor,
        "source_file": source_file,
        "unit": normalize_unit(item.get("unit", "")),
    }

    # совместимость со старым кодом
    normalized["source"] = source_file

    # сохраняем прочие поля, если они есть
    for key in ("date", "currency", "description", "vat", "code"):
        if key in item and key not in normalized:
            normalized[key] = item[key]

    return normalized


def normalize_items(items: list, source: str = "") -> list:
    result = []
    for item in items or []:
        normalized = normalize_item(item, source=source)
        if normalized.get("name"):
            result.append(normalized)
    return result