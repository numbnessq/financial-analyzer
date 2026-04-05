# backend/pipeline/analyzer.py

import statistics
from backend.pipeline.scorer import score_item, get_risk_level


def calculate_spread(prices: list[float]) -> dict:
    clean = [p for p in prices if p > 0]
    if not clean:
        return {"min": 0, "max": 0, "mean": 0, "std": 0}
    mean = round(statistics.mean(clean), 2)
    std  = round(statistics.stdev(clean), 2) if len(clean) > 1 else 0.0
    return {"min": round(min(clean), 2), "max": round(max(clean), 2), "mean": mean, "std": std}


def analyze_group(group: dict) -> dict:
    items = group.get("items", [])
    if not items:
        return {**group, "analysis": {"error": "Нет позиций"}, "aggregated": None}

    prices = [float(i.get("price", 0) or 0) for i in items]
    spread = calculate_spread(prices)

    scored_items = [score_item(item, group) for item in items]

    # Агрегация
    all_flags   = []
    departments = []
    contractors = []
    seen_d, seen_c = set(), set()

    for s in scored_items:
        for f in s["flags"]:
            if f not in all_flags:
                all_flags.append(f)
        d, c = s.get("department", ""), s.get("contractor", "")
        if d and d not in seen_d: seen_d.add(d); departments.append(d)
        if c and c not in seen_c: seen_c.add(c); contractors.append(c)

    max_score  = max(s["score"] for s in scored_items)
    risk_level = get_risk_level(max_score)

    # Фактическое объяснение
    parts = []
    if "duplicate_3_plus" in all_flags:
        parts.append(f"Закупается в {len(departments)} отделах: {', '.join(departments[:5])}")
    elif "duplicate_2" in all_flags:
        parts.append(f"Закупается в 2 отделах: {', '.join(departments)}")
    if "vague_item" in all_flags:
        parts.append("Размытая формулировка позиции")
    if "price_deviation_50" in all_flags:
        parts.append(f"Отклонение цены >50% (мин {spread['min']:,.0f} / макс {spread['max']:,.0f})")
    elif "price_deviation_20" in all_flags:
        parts.append(f"Отклонение цены >20% (мин {spread['min']:,.0f} / макс {spread['max']:,.0f})")
    if "split_suspected" in all_flags:
        parts.append(f"Возможное дробление — {len(items)} записей")
    if "single_occurrence" in all_flags:
        parts.append("Единственное упоминание")

    explanation = " | ".join(parts) if parts else "Без явных аномалий"

    aggregated = {
        "item":        group.get("canonical_name", items[0].get("name", "")),
        "name":        group.get("canonical_name", items[0].get("name", "")),
        "departments": departments,
        "contractors": contractors,
        "count":       len(items),
        "prices":      [p for p in prices if p > 0],
        "spread":      spread,
        "score":       max_score,
        "risk_level":  risk_level,
        "flags":       all_flags,
        "explanation": explanation,
    }

    return {
        **group,
        "items":      scored_items,
        "aggregated": aggregated,
        "analysis": {
            "spread":        spread,
            "has_anomalies": max_score >= 20,
            "anomaly_count": sum(1 for s in scored_items if s["score"] >= 20),
        }
    }


def analyze_all_groups(groups: list[dict]) -> dict:
    if not groups:
        return {
            "groups": [], "results": [], "flat_results": [],
            "total_groups": 0, "total_anomalies": 0,
            "summary": "Нет данных для анализа",
        }

    analyzed = [analyze_group(g) for g in groups]

    flat_results       = [item for g in analyzed for item in g.get("items", [])]
    aggregated_results = sorted(
        [g["aggregated"] for g in analyzed if g.get("aggregated")],
        key=lambda x: x["score"], reverse=True
    )

    total_anomalies = sum(1 for r in aggregated_results if r["score"] >= 20)

    return {
        "groups":          analyzed,
        "results":         aggregated_results,
        "flat_results":    flat_results,
        "total_groups":    len(analyzed),
        "total_anomalies": total_anomalies,
        "groups_with_anomalies": [r["item"] for r in aggregated_results if r["score"] >= 20],
        "summary": f"Проанализировано {len(analyzed)} позиций. Аномалий: {total_anomalies}.",
    }