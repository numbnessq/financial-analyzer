# backend/pipeline/scorer.py

from typing import Optional
from pydantic import BaseModel


class ItemStats(BaseModel):
    name:             str
    prices:           list[float]
    has_match:        bool = True
    department_count: int  = 1
    avg_price:        Optional[float] = None
    min_price:        Optional[float] = None
    max_price:        Optional[float] = None
    std_dev:          Optional[float] = None
    cv:               Optional[float] = None
    deviation_pct:    Optional[float] = None


class ScoreResult(BaseModel):
    name:       str
    score:      int
    risk_level: str
    reasons:    list[str]
    stats:      ItemStats


RULES = [
    {
        "id":          "no_match",
        "points":      25,
        "description": "Позиция не найдена в других документах",
        "check":       lambda s: not s.has_match,
    },
    {
        "id":          "multi_department",
        "points":      35,
        "description": "Позиция закупается в 3+ подразделениях одновременно",
        "check":       lambda s: s.department_count >= 3,
    },
    {
        "id":          "two_department",
        "points":      20,
        "description": "Позиция закупается в 2 подразделениях",
        "check":       lambda s: s.department_count == 2,
    },
    {
        "id":          "deviation_20",
        "points":      30,
        "description": "Отклонение цены > 20% от средней",
        "check":       lambda s: s.deviation_pct is not None and s.deviation_pct > 20,
    },
    {
        "id":          "deviation_50",
        "points":      20,
        "description": "Отклонение цены > 50% от средней (критично)",
        "check":       lambda s: s.deviation_pct is not None and s.deviation_pct > 50,
    },
    {
        "id":          "high_spread",
        "points":      20,
        "description": "Высокий разброс цен (CV > 30%)",
        "check":       lambda s: s.cv is not None and s.cv > 0.30,
    },
    {
        "id":          "small_sample",
        "points":      10,
        "description": "Малая выборка (менее 3 цен)",
        "check":       lambda s: len(s.prices) < 3,
    },
]


def get_risk_level(score: int) -> str:
    if score >= 70: return "CRITICAL"
    if score >= 45: return "HIGH"
    if score >= 20: return "MEDIUM"
    return "LOW"


def compute_stats(item: ItemStats) -> ItemStats:
    prices = item.prices
    if not prices:
        return item
    avg = sum(prices) / len(prices)
    mn  = min(prices)
    mx  = max(prices)
    std = (sum((p - avg) ** 2 for p in prices) / len(prices)) ** 0.5
    cv  = std / avg if avg else 0
    return item.model_copy(update={
        "avg_price":     round(avg, 2),
        "min_price":     mn,
        "max_price":     mx,
        "std_dev":       round(std, 2),
        "cv":            round(cv, 4),
        "deviation_pct": round(((mx - mn) / avg * 100) if avg else 0, 2),
    })


def score_item(item: ItemStats) -> ScoreResult:
    item    = compute_stats(item)
    total   = 0
    reasons = []
    for rule in RULES:
        if rule["check"](item):
            total += rule["points"]
            reasons.append(f"+{rule['points']} — {rule['description']}")
    score = min(total, 100)
    return ScoreResult(
        name=item.name, score=score,
        risk_level=get_risk_level(score),
        reasons=reasons, stats=item,
    )


def score_all(items: list[ItemStats]) -> list[ScoreResult]:
    return sorted([score_item(i) for i in items], key=lambda r: r.score, reverse=True)