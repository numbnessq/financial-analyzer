# backend/pipeline/normalizer.py
"""
Нормализатор позиций.
Единый формат полей: name, canonical_name, quantity, unit,
unit_price, total_price, contractor, date, source_file.

НЕТ поля 'price' — только unit_price и total_price.
"""

import re
import unicodedata
from typing import Any
from datetime import datetime

UNIT_MAP = {
    "куб.м": "м3", "куб м": "м3", "кубометр": "м3", "m3": "м3", "куб": "м3",
    "кв.м":  "м2", "кв м":  "м2", "кв.метр": "м2",
    "килограмм": "кг", "kg": "кг",
    "тонна": "т", "тонн": "т",
    "штука": "шт", "штук": "шт", "шт.": "шт",
    "литр":  "л",
    "метр":  "м",
    "час":   "ч",
    "пог.м": "пм", "погонный метр": "пм",
    "компл": "компл.", "комплект": "компл.",
}

JUNK_VALUES = {"", "-", "—", "none", "null", "unknown", "н/д", "нет"}


# ─── Утилиты ──────────────────────────────────────────────────────

def _to_float(x: Any) -> float | None:
    if x is None:
        return None
    s = str(x).strip().replace(" ", "").replace("\xa0", "").replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        v = float(s)
        return v if v >= 0 else None
    except ValueError:
        return None


def _clean(x: Any) -> str:
    if not x:
        return ""
    s = str(x).strip()
    return "" if s.lower() in JUNK_VALUES else re.sub(r"\s+", " ", s)


def normalize_unit(unit: str) -> str:
    if not unit:
        return ""
    u = str(unit).lower().strip().rstrip(".")
    return UNIT_MAP.get(u, u)


def normalize_date(date_str: Any) -> str:
    if not date_str:
        return ""
    s = str(date_str).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def canonicalize(name: str) -> str:
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", name).lower()
    s = s.replace("ё", "е")
    s = re.sub(r'\d+([.,]\d+)?\s*(м2|м3|шт|кг|л|м)\b', '', s)
    s = re.sub(r'[^a-zа-я0-9 ]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


# ─── Ядро нормализации ────────────────────────────────────────────

def normalize_item(item: dict, source: str = "") -> dict:
    name       = _clean(item.get("name") or item.get("item_name"))
    quantity   = _to_float(item.get("quantity"))
    unit       = normalize_unit(item.get("unit", ""))
    contractor = _clean(item.get("contractor"))
    date       = normalize_date(item.get("date"))
    source_f   = item.get("source_file") or source

    # ── Цена: строгое разделение unit_price и total_price ─────────
    unit_price  = _to_float(item.get("unit_price"))
    total_price = _to_float(item.get("total_price"))

    # Поле "price" из AI — определяем его смысл по quantity
    legacy_price = _to_float(item.get("price"))
    if legacy_price is not None and unit_price is None and total_price is None:
        if quantity and quantity > 0:
            # Проверяем: это unit_price или total_price?
            # Если значение << ожидаемой итоговой суммы — это unit_price
            # Используем простую эвристику: если есть поле "amount"/"sum"/"total"
            alt_total = _to_float(item.get("amount") or item.get("sum") or item.get("total"))
            if alt_total and alt_total > legacy_price:
                unit_price  = legacy_price
                total_price = alt_total
            else:
                # Нет альтернативного поля — не угадываем, помечаем как unit_price
                unit_price = legacy_price
        else:
            total_price = legacy_price

    # ── Восстановление отсутствующих полей ────────────────────────
    if unit_price and quantity and quantity > 0 and total_price is None:
        total_price = round(unit_price * quantity, 2)

    if total_price and quantity and quantity > 0 and unit_price is None:
        unit_price = round(total_price / quantity, 2)

    if total_price and unit_price and unit_price > 0 and quantity is None:
        quantity = round(total_price / unit_price, 4)

    return {
        "name":         name,
        "canonical_name": canonicalize(name),
        "quantity":     quantity,
        "unit":         unit,
        "unit_price":   unit_price,
        "total_price":  total_price,
        "contractor":   contractor,
        "date":         date,
        "source_file":  source_f,
        # Совместимость со scorer.py (использует item.get("price"))
    }


def normalize_items(items: list[dict], source: str = "") -> list[dict]:
    result = []
    for item in items or []:
        n = normalize_item(item, source=source)
        if not n["name"] and not n["total_price"]:
            continue
        result.append(n)
    return result