"""
scorer.py — Модуль скоринга риска для позиций и групп.

Правила начисления баллов:
  - Отклонение цены > 20%       → +30
  - Сильный разброс цен (CV>30%) → +20
  - Нет совпадения (уникальная)  → +25
  - Очень высокое отклонение >50%→ +20 (доп.)
  - Малая выборка (< 3 цен)      → +10

Итог: 0–100 (ограничен сверху)
"""

from typing import Optional
from pydantic import BaseModel


# ─────────────────────────────────────────────
# Схемы данных
# ─────────────────────────────────────────────

class ItemStats(BaseModel):
    """Статистика по группе позиций после matching/анализа."""
    name: str                          # нормализованное название
    prices: list[float]                # все найденные цены
    avg_price: Optional[float] = None  # средняя цена
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    std_dev: Optional[float] = None    # стандартное отклонение
    cv: Optional[float] = None         # коэффициент вариации (std/avg)
    deviation_pct: Optional[float] = None  # макс. отклонение от среднего, %
    has_match: bool = True             # найдено ли совпадение в других документах


class ScoreResult(BaseModel):
    """Результат скоринга одной позиции."""
    name: str
    score: int                   # итоговый балл 0–100
    risk_level: str              # LOW / MEDIUM / HIGH / CRITICAL
    reasons: list[str]           # список причин начисления баллов
    stats: ItemStats             # исходная статистика


# ─────────────────────────────────────────────
# Правила скоринга
# ─────────────────────────────────────────────

RULES = [
    {
        "id": "no_match",
        "points": 25,
        "description": "Позиция не найдена в других документах",
        "check": lambda s: not s.has_match,
    },
    {
        "id": "deviation_20",
        "points": 30,
        "description": "Отклонение цены > 20% от средней",
        "check": lambda s: s.deviation_pct is not None and s.deviation_pct > 20,
    },
    {
        "id": "deviation_50",
        "points": 20,
        "description": "Отклонение цены > 50% от средней (критично)",
        "check": lambda s: s.deviation_pct is not None and s.deviation_pct > 50,
    },
    {
        "id": "high_spread",
        "points": 20,
        "description": "Высокий разброс цен (CV > 30%)",
        "check": lambda s: s.cv is not None and s.cv > 0.30,
    },
    {
        "id": "small_sample",
        "points": 10,
        "description": "Малая выборка (менее 3 цен)",
        "check": lambda s: len(s.prices) < 3,
    },
]


def get_risk_level(score: int) -> str:
    if score >= 70:
        return "CRITICAL"
    elif score >= 45:
        return "HIGH"
    elif score >= 20:
        return "MEDIUM"
    else:
        return "LOW"


# ─────────────────────────────────────────────
# Вычисление статистики
# ─────────────────────────────────────────────

def compute_stats(item: ItemStats) -> ItemStats:
    """Дополняет ItemStats расчётными полями, если они не заданы."""
    prices = item.prices
    if not prices:
        return item

    avg = sum(prices) / len(prices)
    mn = min(prices)
    mx = max(prices)

    variance = sum((p - avg) ** 2 for p in prices) / len(prices)
    std = variance ** 0.5
    cv = std / avg if avg else 0
    deviation_pct = ((mx - mn) / avg * 100) if avg else 0

    return item.model_copy(update={
        "avg_price": round(avg, 2),
        "min_price": mn,
        "max_price": mx,
        "std_dev": round(std, 2),
        "cv": round(cv, 4),
        "deviation_pct": round(deviation_pct, 2),
    })


# ─────────────────────────────────────────────
# Основная функция скоринга
# ─────────────────────────────────────────────

def score_item(item: ItemStats) -> ScoreResult:
    """Считает риск-скор для одной позиции."""
    item = compute_stats(item)

    total = 0
    reasons = []

    for rule in RULES:
        if rule["check"](item):
            total += rule["points"]
            reasons.append(f"+{rule['points']} — {rule['description']}")

    score = min(total, 100)  # не выше 100
    risk_level = get_risk_level(score)

    return ScoreResult(
        name=item.name,
        score=score,
        risk_level=risk_level,
        reasons=reasons,
        stats=item,
    )


def score_all(items: list[ItemStats]) -> list[ScoreResult]:
    """Считает скор для списка позиций, сортирует по убыванию риска."""
    results = [score_item(item) for item in items]
    return sorted(results, key=lambda r: r.score, reverse=True)


# ─────────────────────────────────────────────
# Тест / демо
# ─────────────────────────────────────────────

if __name__ == "__main__":
    test_items = [
        ItemStats(
            name="бетон М300",
            prices=[4500, 4600, 6800, 4550],
            has_match=True,
        ),
        ItemStats(
            name="арматура А500С",
            prices=[85000],
            has_match=False,
        ),
        ItemStats(
            name="кирпич облицовочный",
            prices=[12000, 12500, 12200],
            has_match=True,
        ),
        ItemStats(
            name="услуги экскаватора",
            prices=[15000, 25000, 14500, 24000],
            has_match=True,
        ),
    ]

    results = score_all(test_items)

    print(f"{'Позиция':<30} {'Score':>6}  {'Риск':<10}  Причины")
    print("─" * 90)
    for r in results:
        reasons_str = " | ".join(r.reasons) if r.reasons else "нет"
        print(f"{r.name:<30} {r.score:>6}  {r.risk_level:<10}  {reasons_str}")