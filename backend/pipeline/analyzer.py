# backend/pipeline/analyzer.py

import statistics
import networkx as nx
from backend.pipeline.scorer import score_item, get_risk_level


def calculate_spread(prices: list[float]) -> dict:
    clean = [p for p in prices if p > 0]
    if not clean:
        return {"min": 0, "max": 0, "mean": 0, "std": 0}
    mean = round(statistics.mean(clean), 2)
    std  = round(statistics.stdev(clean), 2) if len(clean) > 1 else 0.0
    return {"min": round(min(clean), 2), "max": round(max(clean), 2), "mean": mean, "std": std}


def calculate_graph_metrics(groups: list[dict]) -> dict:
    G = nx.Graph()
    for group in groups:
        for item in group.get("items", []):
            item_name  = item.get("name", "")
            contractor = item.get("contractor", "")
            if item_name and contractor:
                G.add_edge(f"item:{item_name}", f"contractor:{contractor}")

    if len(G.nodes()) > 0 and len(G.edges()) > 0:
        try:
            centrality = nx.betweenness_centrality(G)
            return {
                k: {"centrality": v}
                for k, v in centrality.items()
                if k.startswith("item:")
            }
        except Exception:
            pass
    return {}


def analyze_group(group: dict, graph_context: dict = None) -> dict:
    items = group.get("items", [])
    if not items:
        return {**group, "analysis": {"error": "Нет позиций"}, "aggregated": None}

    prices = [float(i.get("price", 0) or 0) for i in items]
    spread = calculate_spread(prices)
    scored_items = [score_item(item, group, graph_context) for item in items]

    # Агрегируем флаги и отделы
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

    # Строим объяснение для агрегата — берём лучшее из scored_items
    # (объяснение самого высокорискового item)
    best_item = max(scored_items, key=lambda s: s["score"])
    explanation = best_item["explanation"]

    # Дополняем агрегатными данными если нужно
    parts = explanation.split(" | ") if explanation != "Без явных аномалий" else []

    # Добавляем информацию об объёмах на уровне группы
    quantities = [float(i.get("quantity", 0) or 0) for i in items if float(i.get("quantity", 0) or 0) > 0]
    if "quantity_deviation_50" in all_flags and len(quantities) >= 2:
        qty_part = f"Объём расходится >50% между документами (мин {min(quantities):g} / макс {max(quantities):g})"
        if qty_part not in parts:
            parts.insert(0, qty_part)
    elif "quantity_deviation_20" in all_flags and len(quantities) >= 2:
        qty_part = f"Объём расходится >20% между документами (мин {min(quantities):g} / макс {max(quantities):g})"
        if qty_part not in parts:
            parts.insert(0, qty_part)

    if "unit_mismatch" in all_flags:
        units = list({str(i.get("unit", "") or "").lower().strip() for i in items if i.get("unit")})
        unit_part = f"Разные единицы в документах: {', '.join(units)}"
        if unit_part not in parts:
            parts.append(unit_part)

    final_explanation = " | ".join(parts) if parts else "Без явных аномалий"

    aggregated = {
        "item":        group.get("canonical_name", items[0].get("name", "")),
        "name":        group.get("canonical_name", items[0].get("name", "")),
        "departments": departments,
        "contractors": contractors,
        "count":       len(items),
        "prices":      [p for p in prices if p > 0],
        "quantities":  quantities,
        "spread":      spread,
        "score":       max_score,
        "risk_level":  risk_level,
        "flags":       all_flags,
        "explanation": final_explanation,
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

    graph_context = calculate_graph_metrics(groups)
    analyzed      = [analyze_group(g, graph_context) for g in groups]

    flat_results = [item for g in analyzed for item in g.get("items", [])]
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