# backend/pipeline/normalizer.py
"""
Нормализатор позиций v2.

Поля на выходе: name, canonical_name, quantity, unit,
unit_price, total_price, contractor, date, source_file.

v2:
  - confidence score (0.0–1.0) для каждой позиции
  - parse_warnings: список предупреждений по конкретным полям
  - улучшенная эвристика определения unit_price vs total_price
  - защита от отрицательных значений
  - normalize_items возвращает статистику качества парсинга
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

UNIT_MAP = {
    "куб.м": "м3", "куб м": "м3", "кубометр": "м3", "m3": "м3", "куб": "м3",
    "кв.м":  "м2", "кв м":  "м2", "кв.метр": "м2", "m2": "м2",
    "килограмм": "кг", "kg": "кг", "кило": "кг",
    "тонна": "т",  "тонн": "т",   "тн": "т",
    "штука": "шт", "штук": "шт",  "шт.": "шт",  "pcs": "шт",
    "литр":  "л",  "liter": "л",
    "метр":  "м",  "ml": "мл",
    "час":   "ч",  "hours": "ч",
    "пог.м": "пм", "погонный метр": "пм", "п.м": "пм",
    "компл": "компл.", "комплект": "компл.", "компл.": "компл.",
    "услуга": "усл.", "услуг": "усл.",
}

JUNK_VALUES = {
    "", "-", "—", "none", "null", "unknown",
    "н/д", "нет", "не указано", "не указан",
}

# Максимально правдоподобные значения для эвристики
_MAX_UNIT_PRICE   = 50_000_000   # 50 млн за единицу — предел
_MAX_QUANTITY     = 1_000_000    # 1 млн единиц
_MAX_TOTAL        = 500_000_000  # 500 млн итого


# ─── Confidence signals ──────────────────────────────────────────

@dataclass
class ParseWarning:
    field:   str
    code:    str
    message: str

    def to_dict(self) -> dict:
        return {"field": self.field, "code": self.code, "message": self.message}


def _compute_confidence(item: dict, warnings: list[ParseWarning]) -> float:
    """
    Оценивает достоверность извлечённой позиции (0.0–1.0).

    Штрафы за:
      - нет name                     → -0.40
      - нет цены совсем              → -0.25
      - восстановленные поля         → -0.10 каждое (max -0.20)
      - предупреждения парсинга      → -0.05 каждое (max -0.15)
      - имя очень короткое (<3 символа) → -0.15
      - нет quantity при наличии unit_price → -0.05
    """
    score = 1.0

    name = item.get("name") or ""
    if not name.strip():
        score -= 0.40
    elif len(name.strip()) < 3:
        score -= 0.15

    has_unit_price  = (item.get("unit_price") or 0) > 0
    has_total_price = (item.get("total_price") or 0) > 0

    if not has_unit_price and not has_total_price:
        score -= 0.25

    restored = sum(1 for w in warnings if w.code == "restored")
    score -= min(0.20, restored * 0.10)

    parse_warn = sum(1 for w in warnings if w.code != "restored")
    score -= min(0.15, parse_warn * 0.05)

    qty = item.get("quantity")
    if has_unit_price and (qty is None or qty == 0):
        score -= 0.05

    return round(max(0.0, min(1.0, score)), 2)


# ─── Утилиты ─────────────────────────────────────────────────────

def _to_float(x: Any) -> Optional[float]:
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
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y",
                "%d.%m.%y", "%Y.%m.%d"):
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


# ─── Эвристика unit_price vs total_price ────────────────────────

def _resolve_legacy_price(
    legacy_price: float,
    quantity:     Optional[float],
    item:         dict,
) -> tuple[Optional[float], Optional[float], list[ParseWarning]]:
    """
    Определяет является ли legacy_price (поле "price") unit_price или total_price.

    Логика:
      1. Если есть явное поле amount/sum/total → это total_price, legacy = unit_price
      2. Если quantity > 0:
           a. Если quantity == 1 → неоднозначно, ставим как total_price
           b. Если legacy_price / quantity < _MAX_UNIT_PRICE → скорее unit_price
           c. Иначе → скорее total_price
      3. Без quantity → ставим как total_price (безопаснее)

    Возвращает: (unit_price, total_price, warnings)
    """
    warnings: list[ParseWarning] = []

    alt_total = _to_float(
        item.get("amount") or item.get("sum") or item.get("total")
        or item.get("итого") or item.get("сумма")
    )

    if alt_total is not None and alt_total > legacy_price:
        return legacy_price, alt_total, warnings

    if quantity is not None and quantity > 0:
        if quantity == 1:
            # Неоднозначно: quantity=1 может быть и штукой и "одной услугой"
            # Безопаснее ставить как total_price
            return None, legacy_price, warnings

        implied_unit = legacy_price / quantity
        if 0 < implied_unit <= _MAX_UNIT_PRICE:
            return legacy_price, round(legacy_price * quantity, 2), warnings
        else:
            warnings.append(ParseWarning(
                field   = "price",
                code    = "ambiguous",
                message = (
                    f"Поле 'price'={legacy_price} неоднозначно при qty={quantity}: "
                    f"implied unit_price={implied_unit:.0f} превышает лимит. "
                    f"Интерпретировано как total_price."
                ),
            ))
            return None, legacy_price, warnings

    # Нет quantity → total_price
    return None, legacy_price, warnings


# ─── Ядро нормализации ───────────────────────────────────────────

def normalize_item(item: dict, source: str = "") -> dict:
    """
    Нормализует одну позицию.

    Добавляет поля:
      - confidence     (0.0–1.0)
      - parse_warnings (list[dict])
    """
    warnings: list[ParseWarning] = []

    name       = _clean(item.get("name") or item.get("item_name") or "")
    quantity   = _to_float(item.get("quantity"))
    unit       = normalize_unit(item.get("unit", ""))
    contractor = _clean(item.get("contractor") or "")
    date       = normalize_date(item.get("date") or "")
    source_f   = _clean(item.get("source_file") or "") or source

    # ── Защита от неправдоподобных значений ───────────────────
    if quantity is not None and quantity > _MAX_QUANTITY:
        warnings.append(ParseWarning(
            field="quantity", code="suspicious",
            message=f"Очень большое quantity={quantity:.0f}, возможна ошибка парсинга",
        ))

    # ── Цены ──────────────────────────────────────────────────
    unit_price  = _to_float(item.get("unit_price"))
    total_price = _to_float(item.get("total_price"))

    # Блок совместимости с AI-extractor: поле "price"
    legacy_price = _to_float(item.get("price"))

    if legacy_price is not None and unit_price is None and total_price is None:
        unit_price, total_price, lp_warnings = _resolve_legacy_price(
            legacy_price, quantity, item
        )
        warnings.extend(lp_warnings)

    # ── Восстановление отсутствующих полей ────────────────────
    if unit_price and quantity and quantity > 0 and total_price is None:
        total_price = round(unit_price * quantity, 2)
        warnings.append(ParseWarning(
            field="total_price", code="restored",
            message=f"total_price={total_price} восстановлен из unit_price×quantity",
        ))

    if total_price and quantity and quantity > 0 and unit_price is None:
        implied = round(total_price / quantity, 2)
        if implied <= _MAX_UNIT_PRICE:
            unit_price = implied
            warnings.append(ParseWarning(
                field="unit_price", code="restored",
                message=f"unit_price={unit_price} восстановлен из total_price/quantity",
            ))
        else:
            warnings.append(ParseWarning(
                field="unit_price", code="suspicious",
                message=f"Implied unit_price={implied} слишком велик, пропущено",
            ))

    if total_price and unit_price and unit_price > 0 and quantity is None:
        implied_qty = round(total_price / unit_price, 4)
        if 0 < implied_qty <= _MAX_QUANTITY:
            quantity = implied_qty
            warnings.append(ParseWarning(
                field="quantity", code="restored",
                message=f"quantity={quantity} восстановлено из total_price/unit_price",
            ))

    # ── Проверка согласованности ──────────────────────────────
    if unit_price and quantity and quantity > 0 and total_price:
        expected = round(unit_price * quantity, 2)
        if expected > 0 and abs(expected - total_price) / expected > 0.05:
            warnings.append(ParseWarning(
                field="total_price", code="mismatch",
                message=(
                    f"unit_price({unit_price}) × quantity({quantity}) = {expected}, "
                    f"но total_price={total_price} (расхождение "
                    f"{abs(expected - total_price) / expected * 100:.1f}%)"
                ),
            ))

    # ── Проверка диапазонов ───────────────────────────────────
    if total_price and total_price > _MAX_TOTAL:
        warnings.append(ParseWarning(
            field="total_price", code="suspicious",
            message=f"total_price={total_price:.0f} превышает лимит {_MAX_TOTAL:.0f}",
        ))

    # ── Нет имени ─────────────────────────────────────────────
    if not name:
        warnings.append(ParseWarning(
            field="name", code="missing",
            message="Название позиции не определено",
        ))

    # ── Сборка результата ─────────────────────────────────────
    result = {
        "name":           name,
        "canonical_name": canonicalize(name),
        "quantity":       quantity,
        "unit":           unit,
        "unit_price":     unit_price,
        "total_price":    total_price,
        "contractor":     contractor,
        "date":           date,
        "source_file":    source_f,
        "parse_warnings": [w.to_dict() for w in warnings],
    }

    result["confidence"] = _compute_confidence(result, warnings)
    return result


def normalize_items(
    items:  list[dict],
    source: str = "",
) -> list[dict]:
    """
    Нормализует список позиций.
    Фильтрует полностью пустые записи (нет name И нет total_price).
    """
    result = []
    for item in items or []:
        n = normalize_item(item, source=source)
        if not n["name"] and not n["total_price"]:
            continue
        result.append(n)
    return result


# ─── Статистика качества парсинга ────────────────────────────────

def parse_quality_report(items: list[dict]) -> dict:
    """
    Агрегирует confidence и предупреждения по набору позиций.
    Полезно для логирования и отображения в UI.

    Вызывается опционально из main.py после normalize_items.
    """
    if not items:
        return {
            "total":          0,
            "avg_confidence": 0.0,
            "low_confidence": 0,
            "warnings_total": 0,
            "by_code":        {},
        }

    confidences = [i.get("confidence", 1.0) for i in items]
    avg_conf    = round(sum(confidences) / len(confidences), 3)
    low_conf    = sum(1 for c in confidences if c < 0.5)

    by_code: dict[str, int] = {}
    total_warnings = 0
    for item in items:
        for w in item.get("parse_warnings", []):
            code = w.get("code", "unknown")
            by_code[code] = by_code.get(code, 0) + 1
            total_warnings += 1

    return {
        "total":          len(items),
        "avg_confidence": avg_conf,
        "low_confidence": low_conf,
        "low_confidence_pct": round(low_conf / len(items) * 100, 1),
        "warnings_total": total_warnings,
        "by_code":        by_code,
    }