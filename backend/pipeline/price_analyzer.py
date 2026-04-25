# backend/pipeline/price_analyzer.py
"""
Статистический анализ цен — IQR-based.
Экспортирует get_price_flags() для scorer.py.
"""

import statistics
from typing import Any, Optional


def _to_float(v: Any) -> float:
    try:
        return float(str(v).replace(",", ".").replace(" ", "").replace("\xa0", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _eff_price(item: dict) -> float:
    up = _to_float(item.get("unit_price") or item.get("price") or 0)
    if up > 0:
        return up
    tp  = _to_float(item.get("total_price") or 0)
    qty = _to_float(item.get("quantity") or 0)
    if tp > 0 and qty > 0:
        return round(tp / qty, 2)
    return 0.0


def _quartiles(values: list) -> tuple:
    s  = sorted(values)
    n  = len(s)
    q2 = statistics.median(s)
    lower = s[:n // 2]
    upper = s[n // 2:] if n % 2 == 0 else s[n // 2 + 1:]
    q1 = statistics.median(lower) if lower else q2
    q3 = statistics.median(upper) if upper else q2
    return q1, q2, q3


def compute_group_stats(values: list) -> Optional[dict]:
    vals = [v for v in values if v and v > 0]
    if not vals:
        return None
    q1, q2, q3 = _quartiles(vals)
    iqr  = q3 - q1
    mean = statistics.mean(vals)
    return {
        "n":             len(vals),
        "mean":          round(mean, 2),
        "median":        round(q2, 2),
        "stdev":         round(statistics.stdev(vals), 2) if len(vals) > 1 else 0.0,
        "min":           round(min(vals), 2),
        "max":           round(max(vals), 2),
        "q1":            round(q1, 2),
        "q3":            round(q3, 2),
        "iqr":           round(iqr, 2),
        "fence_soft_lo": round(q1 - 1.5 * iqr, 2),
        "fence_soft_hi": round(q3 + 1.5 * iqr, 2),
        "fence_hard_lo": round(q1 - 3.0 * iqr, 2),
        "fence_hard_hi": round(q3 + 3.0 * iqr, 2),
        "cv":            round(statistics.stdev(vals) / mean * 100, 1)
                         if len(vals) > 1 and mean > 0 else 0.0,
    }


def classify_price(value: float, values: list) -> dict:
    if not values or len(values) < 2:
        return {"classification": "insufficient_data", "n": len(values)}

    q1, q2, q3 = _quartiles(values)
    iqr     = q3 - q1
    abs_dev = abs(value - q2)
    pct_dev = round(abs_dev / q2 * 100, 2) if q2 != 0 else 0.0

    fsl = q1 - 1.5 * iqr
    fsh = q3 + 1.5 * iqr
    fhl = q1 - 3.0 * iqr
    fhh = q3 + 3.0 * iqr

    if value < fhl or value > fhh:
        classification = "strong_outlier"
        iqr_position   = "outlier_low" if value < fhl else "outlier_high"
    elif value < fsl or value > fsh:
        classification = "moderate_deviation"
        iqr_position   = "below_fence" if value < fsl else "above_fence"
    else:
        classification = "normal"
        iqr_position   = "in_range"

    try:
        stdev   = statistics.stdev(values)
        z_score = round((value - statistics.mean(values)) / stdev, 2) if stdev > 0 else 0.0
    except Exception:
        z_score = 0.0

    return {
        "classification": classification,
        "iqr_position":   iqr_position,
        "abs_deviation":  round(abs_dev, 2),
        "pct_deviation":  pct_dev,
        "z_score":        z_score,
        "value":          round(value, 2),
        "median":         round(q2, 2),
        "q1":             round(q1, 2),
        "q3":             round(q3, 2),
        "iqr":            round(iqr, 2),
        "n":              len(values),
    }


def get_price_flags(item: dict, items: list) -> list:
    """
    Вызывается из scorer.py.
    IQR-флаги для одного item относительно группы.
    Возвращает [] если n < 2.
    """
    prices = [_eff_price(i) for i in items if _eff_price(i) > 0]
    if len(prices) < 2:
        return []
    price = _eff_price(item)
    if price <= 0:
        return []
    cls = classify_price(price, prices).get("classification", "normal")
    if cls == "strong_outlier":
        return ["iqr_strong_outlier"]
    if cls == "moderate_deviation":
        return ["iqr_moderate_outlier"]
    return []


def analyze_group_prices(group: dict) -> dict:
    items  = group.get("items", [])
    prices = [_eff_price(i) for i in items if _eff_price(i) > 0]
    stats  = compute_group_stats(prices)
    item_analyses = []
    for item in items:
        ep = _eff_price(item)
        if ep > 0 and prices:
            item_analyses.append({
                "source_file": item.get("source_file", ""),
                "price":       ep,
                "analysis":    classify_price(ep, prices),
            })
    return {
        "group_stats":     stats,
        "item_analyses":   item_analyses,
        "outlier_count":   sum(1 for ia in item_analyses
                               if ia["analysis"].get("classification") == "strong_outlier"),
        "deviation_count": sum(1 for ia in item_analyses
                               if ia["analysis"].get("classification") == "moderate_deviation"),
    }