# backend/pipeline/scorer.py
"""
Вероятностная модель риска.

risk = 1 - Π(1 - p_i)  где p_i — вероятность каждого фактора.

Объяснения — только факты из данных, никаких шаблонных фраз.
"""

import statistics
from typing import Any
from backend.pipeline.normalizer import normalize_text


# ─── Вероятности факторов ──────────────────────────────────────────
FACTOR_PROBS = {
    "duplicate_3_plus": 0.65,   # 3+ отдела — высокий риск
    "duplicate_2":      0.35,   # 2 отдела — средний риск
    "vague_item":       0.70,   # размытая формулировка
    "price_deviation_50": 0.60, # цена > 50% от среднего
    "price_deviation_20": 0.40, # цена > 20% от среднего
    "contractor_concentration": 0.50,  # один контрагент у многих
    "split_suspected":  0.45,   # разбиение закупки
    "single_occurrence": 0.10,  # мало данных
}

# Ключевые слова для размытых позиций
VAGUE_KEYWORDS = {
    normalize_text(w) for w in [
        "прочие", "дополнительные", "сопутствующие",
        "услуги", "работы", "расходы", "затраты", "материалы",
    ]
}


# ─── Утилиты ───────────────────────────────────────────────────────

def _to_float(value: Any) -> float:
    try:
        return float(str(value).replace(",", ".").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _get_name(item: dict) -> str:
    return str(item.get("name") or item.get("item_name") or item.get("canonical_name") or "").strip()


def _get_department(item: dict) -> str:
    return str(item.get("department") or "").strip()


def _get_contractor(item: dict) -> str:
    return str(item.get("contractor") or "").strip()


def _get_source_file(item: dict) -> str:
    return str(item.get("source_file") or item.get("source") or "").strip()


def _is_vague(item: dict) -> bool:
    name = normalize_text(_get_name(item))
    return any(kw in name for kw in VAGUE_KEYWORDS)


def _group_prices(group: dict) -> list[float]:
    return [
        _to_float(i.get("price", 0))
        for i in group.get("items", [])
        if _to_float(i.get("price", 0)) > 0
    ]


def _reference_price(item: dict, group: dict) -> float:
    """Средняя цена по всей группе."""
    prices = _group_prices(group)
    if not prices:
        return 0.0
    return round(statistics.mean(prices), 2)


def _deviation_pct(value: float, reference: float) -> float:
    if reference == 0:
        return 0.0
    return round(abs((value - reference) / reference * 100), 2)


def _unique_departments(group: dict) -> list[str]:
    seen = set()
    result = []
    for i in group.get("items", []):
        d = _get_department(i)
        if d and d not in seen:
            seen.add(d)
            result.append(d)
    return result


def _unique_contractors(group: dict) -> list[str]:
    seen = set()
    result = []
    for i in group.get("items", []):
        c = _get_contractor(i)
        if c and c not in seen:
            seen.add(c)
            result.append(c)
    return result


# ─── Флаги ─────────────────────────────────────────────────────────

def calculate_flags(item: dict, group: dict) -> list[str]:
    flags = []
    departments = _unique_departments(group)
    n_depts = len(departments)
    n_items = len(group.get("items", []))

    if n_depts >= 3:
        flags.append("duplicate_3_plus")
    elif n_depts == 2:
        flags.append("duplicate_2")
    elif n_items == 1:
        flags.append("single_occurrence")

    if _is_vague(item):
        flags.append("vague_item")

    price     = _to_float(item.get("price", 0))
    ref_price = _reference_price(item, group)

    if price > 0 and ref_price > 0:
        dev = _deviation_pct(price, ref_price)
        if dev > 50:
            flags.append("price_deviation_50")
        elif dev > 20:
            flags.append("price_deviation_20")

    # Split: >3 записей одной позиции в одном источнике
    source = _get_source_file(item)
    same_source = [
        i for i in group.get("items", [])
        if _get_source_file(i) == source
    ]
    if len(same_source) >= 3:
        flags.append("split_suspected")

    return flags


# ─── Вероятностная модель ──────────────────────────────────────────

def probabilistic_score(flags: list[str]) -> int:
    """
    risk = 1 - Π(1 - p_i)
    Возвращает 0–100.
    """
    complement = 1.0
    for flag in flags:
        p = FACTOR_PROBS.get(flag, 0.0)
        complement *= (1.0 - p)
    return min(round((1.0 - complement) * 100), 100)


def get_risk_level(score: int) -> str:
    if score >= 70: return "CRITICAL"
    if score >= 40: return "HIGH"
    if score >= 20: return "MEDIUM"
    return "LOW"


# ─── Фактические объяснения ────────────────────────────────────────

def build_explanation(flags: list[str], item: dict, group: dict) -> str:
    """
    Объяснение со ссылками на конкретные факты из данных.
    """
    parts = []
    departments  = _unique_departments(group)
    contractors  = _unique_contractors(group)
    price        = _to_float(item.get("price", 0))
    ref_price    = _reference_price(item, group)
    n_items      = len(group.get("items", []))

    if "duplicate_3_plus" in flags:
        depts_str = ", ".join(departments[:5])
        parts.append(f"Позиция закупается в {len(departments)} отделах: {depts_str}")

    if "duplicate_2" in flags:
        depts_str = ", ".join(departments)
        parts.append(f"Позиция закупается в 2 отделах: {depts_str}")

    if "vague_item" in flags:
        parts.append(f"Размытая формулировка — невозможно однозначно идентифицировать позицию")

    if "price_deviation_50" in flags and ref_price > 0:
        dev = _deviation_pct(price, ref_price)
        parts.append(f"Цена {price:,.0f} отклоняется от средней {ref_price:,.0f} на {dev:.0f}%")

    elif "price_deviation_20" in flags and ref_price > 0:
        dev = _deviation_pct(price, ref_price)
        parts.append(f"Цена {price:,.0f} отклоняется от средней {ref_price:,.0f} на {dev:.0f}%")

    if "split_suspected" in flags:
        parts.append(f"Возможное дробление — {n_items} записей одной позиции в одном документе")

    if contractors and len(contractors) == 1:
        parts.append(f"Единственный поставщик: {contractors[0]}")

    if "single_occurrence" in flags:
        parts.append("Недостаточно данных для сравнения — позиция встречается один раз")

    return " | ".join(parts) if parts else "Без явных аномалий"


# ─── Основная функция ──────────────────────────────────────────────

def score_item(item: dict, group: dict) -> dict:
    flags       = calculate_flags(item, group)
    score       = probabilistic_score(flags)
    risk_level  = get_risk_level(score)
    explanation = build_explanation(flags, item, group)
    ref_price   = _reference_price(item, group)
    price       = _to_float(item.get("price", 0))
    dev         = _deviation_pct(price, ref_price) if ref_price > 0 and price > 0 else 0.0

    return {
        "name":            _get_name(item),
        "item":            _get_name(item),
        "department":      _get_department(item),
        "contractor":      _get_contractor(item),
        "source_file":     _get_source_file(item),
        "price":           price,
        "reference_price": ref_price,
        "deviation_pct":   dev,
        "score":           score,
        "risk_level":      risk_level,
        "flags":           flags,
        "explanation":     explanation,
        "departments":     _unique_departments(group),
        "contractors":     _unique_contractors(group),
    }