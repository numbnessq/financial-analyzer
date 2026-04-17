# backend/pipeline/scorer.py
"""
Вероятностная модель риска.
risk = 1 - Π(1 - p_i)
"""

import statistics
from typing import Any, Dict
from datetime import datetime


FACTOR_PROBS = {
    "duplicate_3_plus":         0.65,
    "duplicate_2":              0.35,
    "vague_item":               0.70,
    "price_deviation_50":       0.60,
    "price_deviation_20":       0.40,
    "contractor_concentration": 0.50,
    "split_suspected":          0.45,
    "single_occurrence":        0.10,
    "contractor_blacklist":     0.80,
    "temporal_clustering":      0.35,
    "graph_central":            0.30,
    "quantity_deviation_50":    0.55,
    "quantity_deviation_20":    0.30,
    "total_mismatch":           0.60,
    "volume_without_price":     0.45,
    "price_without_volume":     0.35,
    "unit_mismatch":            0.50,
    "zero_quantity":            0.65,
    "round_number":             0.25,
    "total_price_deviation_40": 0.55,  # итоговая сумма расходится >40% между документами
    "total_price_deviation_15": 0.30,  # итоговая сумма расходится 15-40% между документами
}

VAGUE_KEYWORDS = {
    "прочие", "дополнительные", "сопутствующие",
    "услуги", "работы", "расходы", "затраты", "материалы",
    "прочее", "иные", "разные", "разное",
}

CONTRACTOR_BLACKLIST: set[str] = set()


# ─── Утилиты ───────────────────────────────────────────────────────

def _to_float(value: Any) -> float:
    try:
        return float(str(value).replace(",", ".").replace(" ", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _get(item: dict, *keys, default="") -> str:
    for k in keys:
        v = item.get(k)
        if v is not None:
            return str(v).strip()
    return default


def _get_name(item: dict)        -> str:   return _get(item, "name", "item_name", "canonical_name")
def _get_department(item: dict)  -> str:   return _get(item, "department")
def _get_contractor(item: dict)  -> str:   return _get(item, "contractor")
def _get_source_file(item: dict) -> str:   return _get(item, "source_file", "source")
def _get_date(item: dict)        -> str:   return _get(item, "date")
def _get_unit(item: dict)        -> str:   return _get(item, "unit").lower().strip()
def _get_unit_price(item: dict)  -> float: return _to_float(item.get("unit_price") or item.get("price") or 0)
def _get_total_price(item: dict) -> float: return _to_float(item.get("total_price") or 0)


def _is_vague(name: str) -> bool:
    n = name.lower().strip()
    return any(kw in n for kw in VAGUE_KEYWORDS)


def _group_unit_prices(group: dict) -> list[float]:
    return [_get_unit_price(i) for i in group.get("items", []) if _get_unit_price(i) > 0]


def _reference_price(item: dict, group: dict) -> float:
    prices = _group_unit_prices(group)
    return round(statistics.mean(prices), 2) if prices else 0.0


def _deviation_pct(value: float, ref: float) -> float:
    if ref == 0:
        return 0.0
    return round(abs((value - ref) / ref * 100), 2)


def _unique_list(group: dict, getter) -> list[str]:
    seen, result = set(), []
    for i in group.get("items", []):
        v = getter(i)
        if v and v not in seen:
            seen.add(v)
            result.append(v)
    return result


def _unique_departments(group: dict) -> list[str]: return _unique_list(group, _get_department)
def _unique_contractors(group: dict) -> list[str]: return _unique_list(group, _get_contractor)


def _is_round_number(value: float) -> bool:
    if value <= 0:
        return False
    for divisor in (10000, 5000, 1000):
        if value >= divisor and value % divisor == 0:
            return True
    return False


# ─── Проверки ─────────────────────────────────────────────────────

def _check_quantity_deviation(item: dict, group: dict) -> list[str]:
    """Расхождение объёмов одной позиции между документами."""
    flags = []
    quantities = [
        _to_float(i.get("quantity", 0))
        for i in group.get("items", [])
        if _to_float(i.get("quantity", 0)) > 0
    ]
    if len(quantities) < 2:
        return flags

    min_q = min(quantities)
    max_q = max(quantities)
    if min_q == 0:
        return flags

    # Считаем отклонение от минимума к максимуму — более чувствительно
    spread = (max_q - min_q) / min_q * 100

    item_qty = _to_float(item.get("quantity", 0))
    if item_qty <= 0:
        return flags

    if spread > 30:
        flags.append("quantity_deviation_50")
    elif spread > 10:
        flags.append("quantity_deviation_20")

    return flags

def _check_total_mismatch(item: dict) -> list[str]:
    if item.get("_has_detail") is False:
        return []
    unit_price  = _get_unit_price(item)
    quantity    = _to_float(item.get("quantity", 0))
    total_price = _get_total_price(item)
    if unit_price > 0 and quantity > 0 and total_price > 0:
        expected = unit_price * quantity
        if expected > 0 and abs(expected - total_price) / expected * 100 > 2:
            return ["total_mismatch"]
    return []


def _check_volume_price_integrity(item: dict) -> list[str]:
    if item.get("_has_detail") is False:
        return []
    unit_price = _get_unit_price(item)
    quantity   = _to_float(item.get("quantity", 0))
    if quantity > 0 and unit_price == 0:
        return ["volume_without_price"]
    if unit_price > 0 and quantity == 0:
        raw_qty = item.get("quantity")
        if raw_qty is not None and str(raw_qty).strip() not in ("", "None"):
            return ["zero_quantity"]
        return ["price_without_volume"]
    return []


def _check_unit_mismatch(item: dict, group: dict) -> list[str]:
    units = [_get_unit(i) for i in group.get("items", []) if _get_unit(i)]
    return ["unit_mismatch"] if len(set(units)) > 1 else []


def _check_round_number(item: dict) -> list[str]:
    for val in (_get_total_price(item), _get_unit_price(item)):
        if _is_round_number(val):
            return ["round_number"]
    return []

def _check_total_price_deviation(item: dict, group: dict) -> list[str]:
    """Сравнивает total_price одной позиции между документами."""
    flags = []
    totals = [
        _to_float(i.get("total_price") or i.get("amount") or i.get("sum") or 0)
        for i in group.get("items", [])
        if _to_float(i.get("total_price") or i.get("amount") or i.get("sum") or 0) > 0
    ]
    if len(totals) < 2:
        return flags
    mean_t = statistics.mean(totals)
    if mean_t == 0:
        return flags
    item_total = _to_float(item.get("total_price") or item.get("amount") or item.get("sum") or 0)
    if item_total <= 0:
        return flags
    dev = abs(item_total - mean_t) / mean_t * 100
    if dev > 40:
        flags.append("total_price_deviation_40")
    elif dev > 15:
        flags.append("total_price_deviation_15")
    return flags


# ─── Флаги ────────────────────────────────────────────────────────

def calculate_flags(item: dict, group: dict, graph_context: Dict = None) -> list[str]:
    flags       = []
    departments = _unique_departments(group)
    contractors = _unique_contractors(group)
    n_depts     = len(departments)
    n_items     = len(group.get("items", []))

    if n_depts >= 3:
        flags.append("duplicate_3_plus")
    elif n_depts == 2:
        flags.append("duplicate_2")
    elif n_items == 1:
        flags.append("single_occurrence")

    if _is_vague(_get_name(item)):
        flags.append("vague_item")

    unit_price = _get_unit_price(item)
    ref_price  = _reference_price(item, group)
    if unit_price > 0 and ref_price > 0:
        dev = _deviation_pct(unit_price, ref_price)
        if dev > 50:
            flags.append("price_deviation_50")
        elif dev > 20:
            flags.append("price_deviation_20")

    source      = _get_source_file(item)
    same_source = [i for i in group.get("items", []) if _get_source_file(i) == source]
    if len(same_source) >= 3:
        flags.append("split_suspected")

    if len(contractors) == 1 and n_items > 1:
        flags.append("contractor_concentration")

    if _get_contractor(item) in CONTRACTOR_BLACKLIST:
        flags.append("contractor_blacklist")

    dates = [_get_date(i) for i in group.get("items", []) if _get_date(i)]
    if len(dates) > 2:
        try:
            dt_list = sorted([datetime.strptime(d, "%Y-%m-%d") for d in dates])
            diffs   = [(dt_list[i + 1] - dt_list[i]).days for i in range(len(dt_list) - 1)]
            if any(d <= 3 for d in diffs):
                flags.append("temporal_clustering")
        except Exception:
            pass

    item_key = f"item:{_get_name(item)}"
    if graph_context and item_key in graph_context:
        ctx = graph_context[item_key]
        if isinstance(ctx, dict) and ctx.get("centrality", 0) > 0.1:
            flags.append("graph_central")

    flags.extend(_check_quantity_deviation(item, group))
    flags.extend(_check_total_mismatch(item))
    flags.extend(_check_volume_price_integrity(item))
    flags.extend(_check_unit_mismatch(item, group))
    flags.extend(_check_round_number(item))
    flags.extend(_check_total_price_deviation(item, group))

    seen, unique = set(), []
    for f in flags:
        if f not in seen:
            seen.add(f)
            unique.append(f)
    return unique


# ─── Вероятностная модель ──────────────────────────────────────────

def probabilistic_score(flags: list[str]) -> int:
    complement = 1.0
    for flag in flags:
        complement *= (1.0 - FACTOR_PROBS.get(flag, 0.0))
    return min(round((1.0 - complement) * 100), 100)


def get_risk_level(score: int) -> str:
    if score >= 70: return "CRITICAL"
    if score >= 40: return "HIGH"
    if score >= 20: return "MEDIUM"
    return "LOW"


# ─── Объяснения ────────────────────────────────────────────────────

def build_explanation(flags: list[str], item: dict, group: dict) -> str:
    parts       = []
    departments = _unique_departments(group)
    contractors = _unique_contractors(group)
    unit_price  = _get_unit_price(item)
    total_price = _get_total_price(item)
    ref_price   = _reference_price(item, group)
    n_items     = len(group.get("items", []))
    dates       = [_get_date(i) for i in group.get("items", []) if _get_date(i)]

    if "duplicate_3_plus" in flags:
        parts.append(f"Закупается в {len(departments)} отделах: {', '.join(departments[:5])}")
    elif "duplicate_2" in flags:
        parts.append(f"Закупается в 2 отделах: {', '.join(departments)}")

    if "vague_item" in flags:
        parts.append("Размытая формулировка позиции")

    if "price_deviation_50" in flags and ref_price > 0:
        dev = _deviation_pct(unit_price, ref_price)
        parts.append(f"Цена {unit_price:,.0f} отклоняется от средней {ref_price:,.0f} на {dev:.0f}%")
    elif "price_deviation_20" in flags and ref_price > 0:
        dev = _deviation_pct(unit_price, ref_price)
        parts.append(f"Цена {unit_price:,.0f} отклоняется от средней {ref_price:,.0f} на {dev:.0f}%")

    if "quantity_deviation_50" in flags or "quantity_deviation_20" in flags:
        qtys = [_to_float(i.get("quantity", 0)) for i in group.get("items", [])
                if _to_float(i.get("quantity", 0)) > 0]
        if qtys:
            spread = (max(qtys) - min(qtys)) / min(qtys) * 100
            label = ">30%" if "quantity_deviation_50" in flags else ">10%"
            parts.append(f"Объём расходится {label} между документами (мин {min(qtys):g} / макс {max(qtys):g})")
    elif "quantity_deviation_20" in flags:
        qtys = [_to_float(i.get("quantity", 0)) for i in group.get("items", [])
                if _to_float(i.get("quantity", 0)) > 0]
        if qtys:
            parts.append(f"Объём расходится >20% (мин {min(qtys):g} / макс {max(qtys):g})")

    if "total_mismatch" in flags:
        qty      = _to_float(item.get("quantity", 0))
        expected = unit_price * qty
        parts.append(f"Сумма {total_price:,.0f} не совпадает с ценой×кол-во ({expected:,.0f})")

    if "volume_without_price" in flags:
        qty = _to_float(item.get("quantity", 0))
        parts.append(f"Указан объём {qty:g}, но цена отсутствует")

    if "price_without_volume" in flags:
        parts.append(f"Указана цена {unit_price:,.0f}, но объём не задан")

    if "zero_quantity" in flags:
        parts.append(f"Количество = 0, но цена {unit_price:,.0f} указана")

    if "unit_mismatch" in flags:
        units = list({_get_unit(i) for i in group.get("items", []) if _get_unit(i)})
        parts.append(f"Разные единицы в документах: {', '.join(units)}")

    if "round_number" in flags:
        parts.append("Подозрительно круглая сумма — возможные приписки")
    
    if "total_price_deviation_40" in flags:
        totals = [_to_float(i.get("total_price") or 0) for i in group.get("items", [])
                  if _to_float(i.get("total_price") or 0) > 0]
        if totals:
            parts.append(f"Итоговая сумма расходится >40% между документами (мин {min(totals):,.0f} / макс {max(totals):,.0f})")
    elif "total_price_deviation_15" in flags:
        totals = [_to_float(i.get("total_price") or 0) for i in group.get("items", [])
                  if _to_float(i.get("total_price") or 0) > 0]
        if totals:
            parts.append(f"Итоговая сумма расходится >15% между документами (мин {min(totals):,.0f} / макс {max(totals):,.0f})")

    if "split_suspected" in flags:
        parts.append(f"Возможное дробление — {n_items} записей")

    if "contractor_concentration" in flags and contractors:
        parts.append(f"Единственный поставщик: {contractors[0]}")

    if "contractor_blacklist" in flags:
        parts.append(f"Подозрительный контрагент: {_get_contractor(item)}")

    if "temporal_clustering" in flags and dates:
        parts.append(f"Частые закупки в короткий срок ({len(dates)} дат)")

    if "graph_central" in flags:
        parts.append("Высокая центральность в сети закупок")

    if "single_occurrence" in flags:
        parts.append("Единственное упоминание")

    return " | ".join(parts) if parts else "Без явных аномалий"


# ─── Основная функция ──────────────────────────────────────────────

def score_item(item: dict, group: dict, graph_context: Dict = None) -> dict:
    flags       = calculate_flags(item, group, graph_context)
    score       = probabilistic_score(flags)
    risk_level  = get_risk_level(score)
    explanation = build_explanation(flags, item, group)
    unit_price  = _get_unit_price(item)
    total_price = _get_total_price(item)
    ref_price   = _reference_price(item, group)
    dev         = _deviation_pct(unit_price, ref_price) if ref_price > 0 and unit_price > 0 else 0.0

    return {
        "name":            _get_name(item),
        "item":            _get_name(item),
        "department":      _get_department(item),
        "contractor":      _get_contractor(item),
        "source_file":     _get_source_file(item),
        "date":            _get_date(item),
        "unit_price":      unit_price,
        "total_price":     total_price,
        "quantity":        _to_float(item.get("quantity", 0)),
        "unit":            _get_unit(item),
        "reference_price": ref_price,
        "deviation_pct":   dev,
        "score":           score,
        "risk_level":      risk_level,
        "flags":           flags,
        "explanation":     explanation,
        "departments":     _unique_departments(group),
        "contractors":     _unique_contractors(group),
    }