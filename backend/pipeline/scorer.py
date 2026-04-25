# backend/pipeline/scorer.py
"""
Rule-based + deviation-based скоринг.
Прозрачность > "умность".
Каждый флаг имеет фиксированный вес и понятную причину.

v2: интегрированы IQR-флаги из price_analyzer.
    При наличии достаточной статистики (n >= 2) старые
    price_deviation_* заменяются на iqr_strong_outlier / iqr_moderate_outlier.
"""

import statistics
from typing import Any, Dict, List, Optional


# ─── Веса флагов (аддитивные, не вероятности) ────────────────────
FLAG_WEIGHTS = {
    # Структурные аномалии
    "duplicate_3_plus":          25,
    "duplicate_2":               10,
    "single_occurrence":          5,
    "split_suspected":           15,

    # Ценовые отклонения — IQR-based (v2, приоритет над price_deviation_*)
    "iqr_strong_outlier":        35,   # за extreme fence (3×IQR)
    "iqr_moderate_outlier":      18,   # за Tukey fence (1.5×IQR)

    # Ценовые отклонения — legacy (используются только если IQR недоступен)
    "price_deviation_100":       30,
    "price_deviation_50":        20,
    "price_deviation_20":        10,
    "total_price_deviation_40":  20,
    "total_price_deviation_15":   8,

    # Целостность данных
    "total_mismatch":            20,
    "volume_without_price":      15,
    "price_without_volume":      10,
    "zero_quantity":             15,
    "unit_mismatch":             10,

    # Контрагент
    "contractor_concentration":   8,
    "contractor_blacklist":       35,

    # Прочие
    "vague_item":                10,
    "round_number":               5,
    "quantity_deviation_50":     15,
    "quantity_deviation_20":      8,
    "temporal_clustering":       10,
    "graph_central":              8,
}

VAGUE_KEYWORDS = {
    "прочие", "дополнительные", "сопутствующие",
    "услуги", "работы", "расходы", "затраты", "материалы",
    "прочее", "иные", "разные", "разное",
}

CONTRACTOR_BLACKLIST: set = set()


# ─── Утилиты ──────────────────────────────────────────────────────

def _to_float(value: Any) -> float:
    try:
        return float(str(value).replace(",", ".").replace(" ", "").replace("\xa0", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _get(item: dict, *keys, default="") -> str:
    for k in keys:
        v = item.get(k)
        if v is not None:
            return str(v).strip()
    return default


def _get_name(item)        -> str:   return _get(item, "name", "item_name", "canonical_name")
def _get_department(item)  -> str:   return _get(item, "department")
def _get_contractor(item)  -> str:   return _get(item, "contractor")
def _get_source_file(item) -> str:   return _get(item, "source_file", "source")
def _get_date(item)        -> str:   return _get(item, "date")
def _get_unit(item)        -> str:   return _get(item, "unit").lower().strip()
def _get_unit_price(item)  -> float: return _to_float(item.get("unit_price") or item.get("price") or 0)
def _get_total_price(item) -> float: return _to_float(item.get("total_price") or 0)


def _is_vague(name: str) -> bool:
    n     = name.lower().strip()
    words = set(n.split())
    non_vague = words - VAGUE_KEYWORDS
    if not non_vague:
        return True
    has_vague    = any(kw in n for kw in VAGUE_KEYWORDS)
    has_specific = any(len(w) > 5 and w not in VAGUE_KEYWORDS for w in words)
    return has_vague and not has_specific


def _unique_list(group: dict, getter) -> list:
    seen, result = set(), []
    for i in group.get("items", []):
        v = getter(i)
        if v and v not in seen:
            seen.add(v)
            result.append(v)
    return result


def _unique_departments(group) -> list: return _unique_list(group, _get_department)
def _unique_contractors(group) -> list: return _unique_list(group, _get_contractor)


def _is_round_number(value: float) -> bool:
    if value <= 0:
        return False
    for divisor in (1_000_000, 500_000, 100_000):
        if value >= divisor and value % divisor == 0:
            return True
    return False


# ─── Статистика группы ────────────────────────────────────────────

def _group_price_stats(group: dict) -> Optional[dict]:
    items = group.get("items", [])
    known_qtys = [_to_float(i.get("quantity", 0)) for i in items if _to_float(i.get("quantity", 0)) > 0]
    median_qty = statistics.median(known_qtys) if known_qtys else 0

    prices = []
    for i in items:
        up = _get_unit_price(i)
        if up > 0:
            prices.append(up)
        else:
            tp  = _get_total_price(i)
            qty = _to_float(i.get("quantity", 0)) or median_qty
            if tp > 0 and qty > 0:
                prices.append(round(tp / qty, 2))

    if not prices:
        return None

    return {
        "values":  prices,
        "n":       len(prices),
        "mean":    round(statistics.mean(prices), 2),
        "median":  round(statistics.median(prices), 2),
        "min":     round(min(prices), 2),
        "max":     round(max(prices), 2),
        "stdev":   round(statistics.stdev(prices), 2) if len(prices) > 1 else 0.0,
    }


def _effective_price(item: dict, group: dict) -> float:
    up = _get_unit_price(item)
    if up > 0:
        return up
    tp  = _get_total_price(item)
    qty = _to_float(item.get("quantity", 0))
    if qty == 0:
        known_qtys = [_to_float(i.get("quantity", 0)) for i in group.get("items", []) if _to_float(i.get("quantity", 0)) > 0]
        qty = statistics.median(known_qtys) if known_qtys else 0
    if tp > 0 and qty > 0:
        return round(tp / qty, 2)
    return 0.0


def _deviation_pct(value: float, ref: float) -> float:
    if ref == 0:
        return 0.0
    return round(abs((value - ref) / ref * 100), 2)


# ─── Проверки ─────────────────────────────────────────────────────

def _check_price_deviation_legacy(item: dict, group: dict) -> list:
    """Fallback: используется только если IQR-данных недостаточно."""
    stats = _group_price_stats(group)
    if not stats or stats["n"] < 2:
        return []
    price = _effective_price(item, group)
    if price <= 0:
        return []
    dev = _deviation_pct(price, stats["median"])
    if dev > 100: return ["price_deviation_100"]
    if dev > 50:  return ["price_deviation_50"]
    if dev > 20:  return ["price_deviation_20"]
    return []


def _check_iqr_price_deviation(item: dict, group: dict) -> list:
    """
    IQR-based ценовые флаги через price_analyzer.
    Возвращает [] если данных недостаточно — тогда используется legacy.
    """
    try:
        from backend.pipeline.price_analyzer import get_price_flags
        return get_price_flags(item, group.get("items", []))
    except ImportError:
        return []


def _check_quantity_deviation(item: dict, group: dict) -> list:
    quantities = [
        _to_float(i.get("quantity", 0))
        for i in group.get("items", [])
        if _to_float(i.get("quantity", 0)) > 0
    ]
    if len(quantities) < 2:
        return []
    min_q = min(quantities)
    max_q = max(quantities)
    if min_q == 0:
        return []
    spread = (max_q - min_q) / min_q * 100
    if spread > 30: return ["quantity_deviation_50"]
    if spread > 10: return ["quantity_deviation_20"]
    return []


def _check_total_mismatch(item: dict) -> list:
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


def _check_volume_price_integrity(item: dict) -> list:
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


def _check_unit_mismatch(item: dict, group: dict) -> list:
    units = [_get_unit(i) for i in group.get("items", []) if _get_unit(i)]
    return ["unit_mismatch"] if len(set(units)) > 1 else []


def _check_total_price_deviation(item: dict, group: dict) -> list:
    totals = [
        _to_float(i.get("total_price") or 0)
        for i in group.get("items", [])
        if _to_float(i.get("total_price") or 0) > 0
    ]
    if len(totals) < 2:
        return []
    median_t   = statistics.median(totals)
    item_total = _get_total_price(item)
    if median_t == 0 or item_total <= 0:
        return []
    dev = _deviation_pct(item_total, median_t)
    if dev > 40: return ["total_price_deviation_40"]
    if dev > 15: return ["total_price_deviation_15"]
    return []


# ─── Флаги ────────────────────────────────────────────────────────

def calculate_flags(item: dict, group: dict, graph_context: dict = None) -> list:
    flags       = []
    departments = _unique_departments(group)
    contractors = _unique_contractors(group)
    n_depts     = len(departments)
    n_items     = len(group.get("items", []))

    # Дубли
    if n_depts >= 3:
        flags.append("duplicate_3_plus")
    elif n_depts == 2:
        flags.append("duplicate_2")
    elif n_items == 1:
        flags.append("single_occurrence")

    # Размытая позиция
    if _is_vague(_get_name(item)):
        flags.append("vague_item")

    # Ценовые отклонения: IQR если доступно, иначе legacy
    iqr_flags = _check_iqr_price_deviation(item, group)
    if iqr_flags:
        flags.extend(iqr_flags)
    else:
        flags.extend(_check_price_deviation_legacy(item, group))

    # Дробление
    source      = _get_source_file(item)
    same_source = [i for i in group.get("items", []) if _get_source_file(i) == source]
    if len(same_source) >= 3:
        flags.append("split_suspected")

    # Один поставщик
    if len(contractors) == 1 and n_items > 1:
        flags.append("contractor_concentration")

    # Чёрный список
    if _get_contractor(item) in CONTRACTOR_BLACKLIST:
        flags.append("contractor_blacklist")

    # Временной кластер
    dates = [_get_date(i) for i in group.get("items", []) if _get_date(i)]
    if len(dates) > 2:
        try:
            from datetime import datetime
            dt_list = sorted([datetime.strptime(d, "%Y-%m-%d") for d in dates])
            diffs   = [(dt_list[i + 1] - dt_list[i]).days for i in range(len(dt_list) - 1)]
            if any(d <= 3 for d in diffs):
                flags.append("temporal_clustering")
        except Exception:
            pass

    # Граф
    item_key = f"item:{_get_name(item)}"
    if graph_context and item_key in graph_context:
        ctx = graph_context[item_key]
        if isinstance(ctx, dict) and ctx.get("centrality", 0) > 0.1:
            flags.append("graph_central")

    # Количественные проверки
    flags.extend(_check_quantity_deviation(item, group))
    flags.extend(_check_total_mismatch(item))
    flags.extend(_check_volume_price_integrity(item))
    flags.extend(_check_unit_mismatch(item, group))
    flags.extend(_check_total_price_deviation(item, group))

    # Круглая сумма
    price_flags = {
        "price_deviation_100", "price_deviation_50", "price_deviation_20",
        "total_price_deviation_40", "total_price_deviation_15",
        "iqr_strong_outlier", "iqr_moderate_outlier",
    }
    if not any(f in flags for f in price_flags):
        total = _get_total_price(item)
        up    = _get_unit_price(item)
        if _is_round_number(total) or _is_round_number(up):
            flags.append("round_number")

    # Дедупликация с сохранением порядка
    seen, unique = set(), []
    for f in flags:
        if f not in seen:
            seen.add(f)
            unique.append(f)
    return unique


# ─── Скоринг ──────────────────────────────────────────────────────

def rule_based_score(flags: list) -> int:
    total = sum(FLAG_WEIGHTS.get(f, 0) for f in flags)
    return min(total, 100)


def get_risk_level(score: int) -> str:
    if score >= 70: return "CRITICAL"
    if score >= 40: return "HIGH"
    if score >= 20: return "MEDIUM"
    return "LOW"


# ─── Основная функция ─────────────────────────────────────────────

def score_item(item: dict, group: dict, graph_context: dict = None) -> dict:
    flags       = calculate_flags(item, group, graph_context)
    score       = rule_based_score(flags)
    risk_level  = get_risk_level(score)
    stats       = _group_price_stats(group)
    unit_price  = _get_unit_price(item)
    eff_price   = _effective_price(item, group)
    total_price = _get_total_price(item)
    ref_price   = stats["median"] if stats else 0.0
    dev         = _deviation_pct(eff_price, ref_price) if ref_price > 0 and eff_price > 0 else 0.0

    return {
        "name":            _get_name(item),
        "item":            _get_name(item),
        "department":      _get_department(item),
        "contractor":      _get_contractor(item),
        "source_file":     _get_source_file(item),
        "date":            _get_date(item),
        "unit_price":      unit_price,
        "effective_price": eff_price,
        "total_price":     total_price,
        "quantity":        _to_float(item.get("quantity", 0)),
        "unit":            _get_unit(item),
        "price_stats":     stats,
        "reference_price": ref_price,
        "deviation_pct":   dev,
        "score":           score,
        "risk_level":      risk_level,
        "flags":           flags,
        "departments":     _unique_departments(group),
        "contractors":     _unique_contractors(group),
    }