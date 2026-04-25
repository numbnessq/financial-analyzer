# backend/pipeline/supplier_analyzer.py
"""
Анализ поставщиков по набору позиций.

Метрики:
  - доля каждого поставщика по сумме и количеству позиций
  - индекс Херфиндаля–Хиршмана (HHI) по суммам
  - сигналы: монополия, высокая концентрация, резкая смена поставщика

HHI:
  < 1500  — конкурентный рынок
  1500–2500 — умеренная концентрация
  > 2500  — высокая концентрация
  10000   — монополия (один поставщик)
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any


# ─── Утилиты ─────────────────────────────────────────────────────

def _to_float(v: Any) -> float:
    try:
        return float(str(v).replace(",", ".").replace(" ", "").replace("\xa0", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _clean(v: Any) -> str:
    s = str(v or "").strip()
    junk = {"", "-", "—", "none", "null", "unknown", "н/д", "нет",
            "не указан", "неизвестно", "неизвестный контрагент"}
    return "" if s.lower() in junk else s


# ─── Основные функции ────────────────────────────────────────────

def compute_supplier_shares(items: list[dict]) -> list[dict]:
    """
    Считает долю каждого поставщика.

    Возвращает список:
    [
      {
        "contractor": str,
        "total_sum": float,         # сумма total_price по всем позициям
        "item_count": int,          # количество позиций
        "share_by_sum": float,      # % от общей суммы
        "share_by_count": float,    # % от общего числа позиций
      },
      ...
    ]
    Отсортирован по доле (убывание).
    """
    sum_by:   dict[str, float] = defaultdict(float)
    count_by: dict[str, int]   = defaultdict(int)

    for item in items:
        c = _clean(item.get("contractor") or "")
        if not c:
            c = "Не указан"
        total = _to_float(item.get("total_price") or 0)
        sum_by[c]   += total
        count_by[c] += 1

    grand_sum   = sum(sum_by.values())
    grand_count = sum(count_by.values())

    result = []
    for contractor, s in sum_by.items():
        result.append({
            "contractor":     contractor,
            "total_sum":      round(s, 2),
            "item_count":     count_by[contractor],
            "share_by_sum":   round(s / grand_sum * 100, 2)   if grand_sum   > 0 else 0.0,
            "share_by_count": round(count_by[contractor] / grand_count * 100, 2) if grand_count > 0 else 0.0,
        })

    result.sort(key=lambda x: x["share_by_sum"], reverse=True)
    return result


def compute_hhi(shares: list[dict]) -> float:
    """
    Индекс Херфиндаля–Хиршмана на основе share_by_sum.
    HHI = Σ (share_i)²  где share_i в долях (не процентах).
    Результат масштабирован к [0, 10000].
    """
    return round(sum((s["share_by_sum"] ** 2) for s in shares), 2)


def classify_hhi(hhi: float) -> str:
    if hhi >= 10000:
        return "monopoly"
    if hhi > 2500:
        return "high_concentration"
    if hhi > 1500:
        return "moderate_concentration"
    return "competitive"


def detect_supplier_signals(
    shares: list[dict],
    hhi: float,
    items: list[dict],
) -> list[dict]:
    """
    Генерирует список сигналов по поставщикам.

    Каждый сигнал:
    {
        "signal":      str,   # код сигнала
        "severity":    str,   # low / medium / high / critical
        "description": str,
        "details":     dict,
    }
    """
    signals = []
    n_suppliers = len([s for s in shares if s["contractor"] != "Не указан"])

    # Монополия
    if n_suppliers == 1:
        signals.append({
            "signal":      "single_supplier",
            "severity":    "high",
            "description": "Все закупки у одного поставщика",
            "details":     {"contractor": shares[0]["contractor"],
                            "total_sum":  shares[0]["total_sum"]},
        })

    # Высокая концентрация
    elif hhi > 2500:
        top = shares[0]
        signals.append({
            "signal":      "high_concentration",
            "severity":    "medium",
            "description": f"Высокая концентрация: HHI={hhi:.0f}. "
                           f"Лидер '{top['contractor']}' — {top['share_by_sum']:.1f}%",
            "details":     {"hhi": hhi, "top_contractor": top["contractor"],
                            "top_share_pct": top["share_by_sum"]},
        })

    # Доминирующий поставщик (>70% без монополии)
    for s in shares:
        if s["contractor"] == "Не указан":
            continue
        if s["share_by_sum"] >= 70 and n_suppliers > 1:
            signals.append({
                "signal":      "dominant_supplier",
                "severity":    "medium",
                "description": f"'{s['contractor']}' занимает {s['share_by_sum']:.1f}% объёма",
                "details":     s,
            })

    # Резкая смена поставщика (один поставщик на позицию, несколько позиций)
    supplier_per_name: dict[str, set] = defaultdict(set)
    for item in items:
        name = str(item.get("canonical_name") or item.get("name") or "").strip()
        cont = _clean(item.get("contractor") or "")
        if name and cont:
            supplier_per_name[name].add(cont)

    switched = {
        name: list(contrs)
        for name, contrs in supplier_per_name.items()
        if len(contrs) > 1
    }
    if switched:
        signals.append({
            "signal":      "supplier_switch",
            "severity":    "low",
            "description": f"Позиции с разными поставщиками: {len(switched)} шт.",
            "details":     {"items": switched},
        })

    return signals


def analyze_suppliers(items: list[dict]) -> dict:
    """
    Полный анализ поставщиков по списку позиций.

    Возвращает:
    {
        "supplier_count":    int,
        "shares":            [...],
        "hhi":               float,
        "hhi_category":      str,
        "signals":           [...],
        "has_supplier_risk": bool,
    }
    """
    if not items:
        return {
            "supplier_count":    0,
            "shares":            [],
            "hhi":               0.0,
            "hhi_category":      "competitive",
            "signals":           [],
            "has_supplier_risk": False,
        }

    shares       = compute_supplier_shares(items)
    hhi          = compute_hhi(shares)
    hhi_category = classify_hhi(hhi)
    signals      = detect_supplier_signals(shares, hhi, items)

    high_severity = {"high", "critical"}
    has_risk = any(s["severity"] in high_severity for s in signals) or hhi > 2500

    real_suppliers = len([s for s in shares if s["contractor"] != "Не указан"])

    return {
        "supplier_count":    real_suppliers,
        "shares":            shares,
        "hhi":               hhi,
        "hhi_category":      hhi_category,
        "signals":           signals,
        "has_supplier_risk": has_risk,
    }