# backend/pipeline/pattern_detector.py
"""
Детектор аномальных паттернов в закупочных данных.

Паттерны:
  1. split_procurement   — дробление закупок
     (одинаковые позиции, близко по времени, суммы чуть ниже порога)
  2. repeated_amounts    — повторяющиеся суммы у разных позиций
  3. suspicious_intervals — подозрительные временные интервалы
  4. round_number_cluster — кластер из одних круглых сумм

Каждый паттерн возвращает:
  {
    "pattern":     str,
    "severity":    str,   # low / medium / high / critical
    "description": str,
    "items":       [...], # затронутые позиции
    "details":     dict,
  }
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional


# ─── Пороги дробления (можно вынести в конфиг) ───────────────────
SPLIT_THRESHOLDS = [
    100_000,
    500_000,
    1_000_000,
    3_000_000,
    5_000_000,
]
SPLIT_MARGIN_PCT   = 0.15   # 15% ниже порога = «чуть ниже»
SPLIT_WINDOW_DAYS  = 30     # окно для временного кластера дробления
REPEATED_MIN_COUNT = 3      # сколько раз сумма должна повторяться
INTERVAL_MIN_DAYS  = 1
INTERVAL_MAX_DAYS  = 7      # интервал ≤7 дней = «подозрительный»
ROUND_DIVISORS     = [1_000_000, 500_000, 100_000, 50_000, 10_000]


# ─── Утилиты ─────────────────────────────────────────────────────

def _to_float(v: Any) -> float:
    try:
        return float(str(v).replace(",", ".").replace(" ", "").replace("\xa0", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _parse_date(s: Any) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(s).strip(), fmt)
        except ValueError:
            continue
    return None


def _is_round(value: float) -> bool:
    if value <= 0:
        return False
    for d in ROUND_DIVISORS:
        if value >= d and value % d == 0:
            return True
    return False


def _item_label(item: dict) -> str:
    return str(item.get("name") or item.get("canonical_name") or "")


# ─── Паттерны ────────────────────────────────────────────────────

def detect_split_procurement(items: list[dict]) -> list[dict]:
    """
    Дробление закупок:
    - одинаковая/похожая позиция
    - сумма чуть ниже порогового значения
    - несколько документов в пределах SPLIT_WINDOW_DAYS
    """
    patterns = []

    # Группируем по canonical_name
    by_name: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        name = str(item.get("canonical_name") or item.get("name") or "").strip()
        if name:
            by_name[name].append(item)

    for name, group in by_name.items():
        if len(group) < 2:
            continue

        for threshold in SPLIT_THRESHOLDS:
            lower = threshold * (1 - SPLIT_MARGIN_PCT)
            suspects = [
                i for i in group
                if lower <= _to_float(i.get("total_price") or 0) < threshold
            ]
            if len(suspects) < 2:
                continue

            # Проверяем временное окно
            dated = [(i, _parse_date(i.get("date"))) for i in suspects]
            dated_valid = [(i, d) for i, d in dated if d]

            if len(dated_valid) >= 2:
                dated_valid.sort(key=lambda x: x[1])
                window_groups = []
                current = [dated_valid[0]]
                for prev, nxt in zip(dated_valid, dated_valid[1:]):
                    if (nxt[1] - prev[1]).days <= SPLIT_WINDOW_DAYS:
                        current.append(nxt)
                    else:
                        if len(current) >= 2:
                            window_groups.append(current)
                        current = [nxt]
                if len(current) >= 2:
                    window_groups.append(current)

                for wg in window_groups:
                    total_split = sum(_to_float(i.get("total_price") or 0) for i, _ in wg)
                    patterns.append({
                        "pattern":     "split_procurement",
                        "severity":    "high",
                        "description": (
                            f"Возможное дробление закупки '{name}': "
                            f"{len(wg)} позиций на сумму {total_split:,.0f} "
                            f"(порог {threshold:,})"
                        ),
                        "items":   [i for i, _ in wg],
                        "details": {
                            "threshold":   threshold,
                            "item_count":  len(wg),
                            "total_split": round(total_split, 2),
                            "date_range":  [
                                wg[0][1].strftime("%Y-%m-%d"),
                                wg[-1][1].strftime("%Y-%m-%d"),
                            ],
                        },
                    })
            elif len(suspects) >= 2:
                # Нет дат — сигнализируем только по суммам
                total_split = sum(_to_float(i.get("total_price") or 0) for i in suspects)
                patterns.append({
                    "pattern":     "split_procurement",
                    "severity":    "medium",
                    "description": (
                        f"Возможное дробление '{name}': "
                        f"{len(suspects)} позиций ниже порога {threshold:,}"
                    ),
                    "items":   suspects,
                    "details": {
                        "threshold":   threshold,
                        "item_count":  len(suspects),
                        "total_split": round(total_split, 2),
                        "date_range":  None,
                    },
                })

    return patterns


def detect_repeated_amounts(items: list[dict]) -> list[dict]:
    """
    Повторяющиеся суммы у разных позиций.
    Суммы округляются до 2 знаков для сравнения.
    """
    patterns = []
    amount_map: dict[float, list[dict]] = defaultdict(list)

    for item in items:
        total = round(_to_float(item.get("total_price") or 0), 2)
        if total > 0:
            amount_map[total].append(item)

    for amount, group in amount_map.items():
        if len(group) < REPEATED_MIN_COUNT:
            continue
        # Проверяем, что это разные позиции (не один и тот же item повторился)
        names = set(_item_label(i) for i in group)
        if len(names) < 2:
            continue

        patterns.append({
            "pattern":     "repeated_amounts",
            "severity":    "medium",
            "description": (
                f"Одинаковая сумма {amount:,.2f} встречается "
                f"{len(group)} раз у {len(names)} разных позиций"
            ),
            "items":   group,
            "details": {
                "amount":     amount,
                "count":      len(group),
                "item_names": list(names),
            },
        })

    return patterns


def detect_suspicious_intervals(items: list[dict]) -> list[dict]:
    """
    Подозрительно короткие интервалы между закупками одной позиции.
    """
    patterns = []
    by_name: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        name = str(item.get("canonical_name") or item.get("name") or "").strip()
        if name:
            by_name[name].append(item)

    for name, group in by_name.items():
        dated = [(i, _parse_date(i.get("date"))) for i in group]
        dated_valid = sorted(
            [(i, d) for i, d in dated if d],
            key=lambda x: x[1],
        )
        if len(dated_valid) < 2:
            continue

        suspicious_pairs = []
        for (i1, d1), (i2, d2) in zip(dated_valid, dated_valid[1:]):
            diff = (d2 - d1).days
            if INTERVAL_MIN_DAYS <= diff <= INTERVAL_MAX_DAYS:
                suspicious_pairs.append((i1, i2, diff))

        if suspicious_pairs:
            involved = []
            seen_ids = set()
            for i1, i2, _ in suspicious_pairs:
                for i in (i1, i2):
                    key = id(i)
                    if key not in seen_ids:
                        seen_ids.add(key)
                        involved.append(i)

            patterns.append({
                "pattern":     "suspicious_intervals",
                "severity":    "low",
                "description": (
                    f"'{name}': {len(suspicious_pairs)} пар закупок "
                    f"с интервалом ≤{INTERVAL_MAX_DAYS} дней"
                ),
                "items":   involved,
                "details": {
                    "pairs": [
                        {
                            "item1":       _item_label(i1),
                            "item2":       _item_label(i2),
                            "interval_days": diff,
                            "date1":       i1.get("date"),
                            "date2":       i2.get("date"),
                        }
                        for i1, i2, diff in suspicious_pairs
                    ]
                },
            })

    return patterns


def detect_round_number_cluster(items: list[dict]) -> list[dict]:
    """
    Кластер позиций с круглыми суммами (сигнал возможной фиктивности).
    Срабатывает если >=3 позиций в одной группе имеют круглые суммы.
    """
    round_items = [i for i in items if _is_round(_to_float(i.get("total_price") or 0))]
    if len(round_items) < 3:
        return []

    return [{
        "pattern":     "round_number_cluster",
        "severity":    "low",
        "description": (
            f"{len(round_items)} из {len(items)} позиций имеют "
            f"«круглые» суммы — возможные фиктивные закупки"
        ),
        "items":   round_items,
        "details": {
            "round_count": len(round_items),
            "total_count": len(items),
            "share_pct":   round(len(round_items) / len(items) * 100, 1),
        },
    }]


# ─── Главная функция ─────────────────────────────────────────────

def detect_all_patterns(items: list[dict]) -> dict:
    """
    Запускает все детекторы и возвращает агрегированный результат.

    {
        "patterns":           [...],
        "pattern_count":      int,
        "has_critical":       bool,
        "has_high":           bool,
        "severity_summary":   {"critical": int, "high": int, "medium": int, "low": int},
    }
    """
    if not items:
        return {
            "patterns":         [],
            "pattern_count":    0,
            "has_critical":     False,
            "has_high":         False,
            "severity_summary": {"critical": 0, "high": 0, "medium": 0, "low": 0},
        }

    all_patterns = (
        detect_split_procurement(items)
        + detect_repeated_amounts(items)
        + detect_suspicious_intervals(items)
        + detect_round_number_cluster(items)
    )

    severity_summary: dict[str, int] = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    for p in all_patterns:
        sev = p.get("severity", "low")
        severity_summary[sev] = severity_summary.get(sev, 0) + 1

    return {
        "patterns":         all_patterns,
        "pattern_count":    len(all_patterns),
        "has_critical":     severity_summary["critical"] > 0,
        "has_high":         severity_summary["high"] > 0,
        "severity_summary": severity_summary,
    }