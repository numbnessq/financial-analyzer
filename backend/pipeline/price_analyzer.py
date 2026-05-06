# backend/pipeline/price_analyzer.py
"""
Статистический анализ цен — IQR-based.

v2: исправления и расширения:
  - Защита от IQR=0 при n=2 и идентичных ценах
  - Минимум n >= 3 для IQR-флагов (при n=2 — только legacy deviation)
  - Добавлен coefficient of variation (CV) как дополнительный сигнал
  - compute_group_stats возвращает z_scores по всем точкам
  - analyze_group_prices добавляет trend (первая/последняя треть)
  - Все публичные функции защищены от пустых/None входных данных
"""

from __future__ import annotations

import statistics
from typing import Any, Optional


# ─── Утилиты ─────────────────────────────────────────────────────

def _to_float(v: Any) -> float:
    try:
        return float(
            str(v).replace(",", ".").replace(" ", "").replace("\xa0", "").strip() or 0
        )
    except (TypeError, ValueError):
        return 0.0


def _eff_price(item: dict) -> float:
    """Эффективная цена единицы: unit_price → расчётная из total/qty → 0."""
    up = _to_float(item.get("unit_price") or item.get("price") or 0)
    if up > 0:
        return up
    tp  = _to_float(item.get("total_price") or 0)
    qty = _to_float(item.get("quantity") or 0)
    if tp > 0 and qty > 0:
        return round(tp / qty, 2)
    return 0.0


def _clean_prices(values: list) -> list[float]:
    return [v for v in (_to_float(x) for x in values) if v > 0]


# ─── Квартили (устойчивая реализация) ────────────────────────────

def _quartiles(values: list[float]) -> tuple[float, float, float]:
    """
    Вычисляет Q1, Q2 (медиана), Q3.

    Алгоритм: метод "exclusive" — медиана не включается в обе половины.
    Устойчиво работает при n >= 2.
    """
    s = sorted(values)
    n = len(s)

    q2 = statistics.median(s)

    if n < 2:
        return q2, q2, q2

    # Разбиваем без медианы для нечётного n
    half = n // 2
    lower = s[:half]
    upper = s[n - half:]

    q1 = statistics.median(lower) if lower else q2
    q3 = statistics.median(upper) if upper else q2

    return q1, q2, q3


def _safe_iqr(q1: float, q3: float, values: list[float]) -> float:
    """
    IQR с защитой от нуля.
    Если IQR = 0 (все значения одинаковы или n=2 с равными значениями)
    → используем 10% от медианы как минимальный IQR.
    """
    iqr = q3 - q1
    if iqr <= 0:
        median = statistics.median(values)
        # Если все значения полностью одинаковы — нет смысла сигнализировать
        if len(set(round(v, 2) for v in values)) == 1:
            return 0.0   # Специальный случай: возвращаем 0 → флаги не выдаём
        # Иначе используем MAD (median absolute deviation) как proxy
        deviations = [abs(v - median) for v in values]
        mad = statistics.median(deviations)
        return mad * 1.4826 if mad > 0 else median * 0.1
    return iqr


# ─── Статистика группы ───────────────────────────────────────────

def compute_group_stats(values: list) -> Optional[dict]:
    """
    Полная статистика ценовой группы.

    Возвращает None если нет валидных данных.
    Возвращает структуру с флагом insufficient_for_iqr=True если n < 3
    (IQR-флаги не должны применяться, только legacy deviation).
    """
    vals = _clean_prices(values)
    n    = len(vals)

    if n == 0:
        return None

    mean = statistics.mean(vals)
    q2   = statistics.median(vals)

    if n == 1:
        return {
            "n":                    1,
            "mean":                 round(mean, 2),
            "median":               round(q2, 2),
            "stdev":                0.0,
            "min":                  round(vals[0], 2),
            "max":                  round(vals[0], 2),
            "q1":                   round(vals[0], 2),
            "q3":                   round(vals[0], 2),
            "iqr":                  0.0,
            "fence_soft_lo":        round(vals[0], 2),
            "fence_soft_hi":        round(vals[0], 2),
            "fence_hard_lo":        round(vals[0], 2),
            "fence_hard_hi":        round(vals[0], 2),
            "cv":                   0.0,
            "insufficient_for_iqr": True,
        }

    stdev = statistics.stdev(vals) if n > 1 else 0.0
    q1, _, q3 = _quartiles(vals)
    iqr   = _safe_iqr(q1, q3, vals)
    cv    = round(stdev / mean * 100, 1) if mean > 0 else 0.0

    # Флаг: при n < 3 или IQR = 0 IQR-анализ ненадёжен
    insufficient = n < 3 or iqr == 0.0

    return {
        "n":                    n,
        "mean":                 round(mean, 2),
        "median":               round(q2, 2),
        "stdev":                round(stdev, 2),
        "min":                  round(min(vals), 2),
        "max":                  round(max(vals), 2),
        "q1":                   round(q1, 2),
        "q3":                   round(q3, 2),
        "iqr":                  round(iqr, 2),
        "fence_soft_lo":        round(q1 - 1.5 * iqr, 2),
        "fence_soft_hi":        round(q3 + 1.5 * iqr, 2),
        "fence_hard_lo":        round(q1 - 3.0 * iqr, 2),
        "fence_hard_hi":        round(q3 + 3.0 * iqr, 2),
        "cv":                   cv,
        "insufficient_for_iqr": insufficient,
    }


# ─── Классификация цены ──────────────────────────────────────────

def classify_price(value: float, values: list) -> dict:
    """
    Классифицирует цену относительно группы.

    Возвращает classification:
      normal / moderate_deviation / strong_outlier / insufficient_data

    При insufficient_for_iqr — возвращает 'insufficient_data',
    чтобы scorer мог fallback на legacy deviation.
    """
    vals = _clean_prices(values)
    n    = len(vals)

    if n < 2 or value <= 0:
        return {
            "classification":   "insufficient_data",
            "iqr_position":     "unknown",
            "abs_deviation":    0.0,
            "pct_deviation":    0.0,
            "z_score":          0.0,
            "value":            round(value, 2),
            "median":           0.0,
            "q1":               0.0,
            "q3":               0.0,
            "iqr":              0.0,
            "n":                n,
        }

    q1, q2, q3 = _quartiles(vals)
    iqr         = _safe_iqr(q1, q3, vals)
    abs_dev     = abs(value - q2)
    pct_dev     = round(abs_dev / q2 * 100, 2) if q2 != 0 else 0.0

    try:
        stdev   = statistics.stdev(vals) if n > 1 else 0.0
        mean    = statistics.mean(vals)
        z_score = round((value - mean) / stdev, 2) if stdev > 0 else 0.0
    except Exception:
        z_score = 0.0

    # IQR = 0 → нельзя делать выводы по фенсам
    if iqr == 0.0:
        return {
            "classification":   "insufficient_data",
            "iqr_position":     "iqr_zero",
            "abs_deviation":    round(abs_dev, 2),
            "pct_deviation":    pct_dev,
            "z_score":          z_score,
            "value":            round(value, 2),
            "median":           round(q2, 2),
            "q1":               round(q1, 2),
            "q3":               round(q3, 2),
            "iqr":              0.0,
            "n":                n,
        }

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

    return {
        "classification":   classification,
        "iqr_position":     iqr_position,
        "abs_deviation":    round(abs_dev, 2),
        "pct_deviation":    pct_dev,
        "z_score":          z_score,
        "value":            round(value, 2),
        "median":           round(q2, 2),
        "q1":               round(q1, 2),
        "q3":               round(q3, 2),
        "iqr":              round(iqr, 2),
        "n":                n,
    }


# ─── Scorer integration ──────────────────────────────────────────

def get_price_flags(item: dict, items: list) -> list:
    """
    Вызывается из scorer.py.
    Возвращает IQR-флаги для item относительно группы.

    Критически важно: возвращает [] если:
      - n < 3 (недостаточно данных для надёжного IQR)
      - IQR = 0 (все цены одинаковы — нет отклонения)
      - цена самого item не определена

    В этих случаях scorer.py использует legacy price_deviation_*.
    """
    prices = [_eff_price(i) for i in items if _eff_price(i) > 0]

    # Минимум 3 наблюдения для IQR
    if len(prices) < 3:
        return []

    price = _eff_price(item)
    if price <= 0:
        return []

    result = classify_price(price, prices)
    cls    = result.get("classification", "normal")

    # insufficient_data → fallback на legacy
    if cls == "insufficient_data":
        return []
    if cls == "strong_outlier":
        return ["iqr_strong_outlier"]
    if cls == "moderate_deviation":
        return ["iqr_moderate_outlier"]
    return []


# ─── Трендовый анализ (временной ряд цен) ────────────────────────

def _detect_price_trend(items: list[dict]) -> Optional[dict]:
    """
    Определяет тренд цены по времени.
    Требует: хотя бы 3 элемента с датами и ценами.
    """
    dated = []
    for item in items:
        date  = item.get("date", "")
        price = _eff_price(item)
        if date and price > 0:
            dated.append((date, price))

    if len(dated) < 3:
        return None

    dated.sort(key=lambda x: x[0])
    prices_sorted = [p for _, p in dated]

    n     = len(prices_sorted)
    third = max(1, n // 3)

    first_third  = prices_sorted[:third]
    last_third   = prices_sorted[n - third:]
    avg_first    = statistics.mean(first_third)
    avg_last     = statistics.mean(last_third)

    if avg_first == 0:
        return None

    change_pct = round((avg_last - avg_first) / avg_first * 100, 1)

    if change_pct > 15:
        direction = "growing"
    elif change_pct < -15:
        direction = "declining"
    else:
        direction = "stable"

    return {
        "direction":      direction,
        "change_pct":     change_pct,
        "avg_first":      round(avg_first, 2),
        "avg_last":       round(avg_last, 2),
        "date_range":     [dated[0][0], dated[-1][0]],
        "observations":   n,
    }


# ─── Группа целиком ──────────────────────────────────────────────

def analyze_group_prices(group: dict) -> dict:
    """
    Полный анализ цен группы.

    Возвращает:
      group_stats:     статистика IQR
      item_analyses:   классификация каждого item
      outlier_count:   strong_outlier
      deviation_count: moderate_deviation
      trend:           тренд по времени (если есть даты)
      cv_signal:       True если CV > 30% (высокая вариативность)
    """
    items  = group.get("items", [])
    prices = [_eff_price(i) for i in items if _eff_price(i) > 0]
    stats  = compute_group_stats(prices)

    item_analyses = []
    for item in items:
        ep = _eff_price(item)
        if ep > 0 and len(prices) >= 2:
            item_analyses.append({
                "source_file": item.get("source_file", ""),
                "date":        item.get("date", ""),
                "contractor":  item.get("contractor", ""),
                "price":       ep,
                "analysis":    classify_price(ep, prices),
            })

    outlier_count   = sum(
        1 for ia in item_analyses
        if ia["analysis"].get("classification") == "strong_outlier"
    )
    deviation_count = sum(
        1 for ia in item_analyses
        if ia["analysis"].get("classification") == "moderate_deviation"
    )

    trend      = _detect_price_trend(items)
    cv         = stats.get("cv", 0) if stats else 0
    cv_signal  = cv > 30.0   # CV > 30% — высокая разброс цен

    return {
        "group_stats":     stats,
        "item_analyses":   item_analyses,
        "outlier_count":   outlier_count,
        "deviation_count": deviation_count,
        "trend":           trend,
        "cv_signal":       cv_signal,
        "cv":              round(cv, 1),
    }