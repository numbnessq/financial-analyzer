# backend/pipeline/analyzer.py

import statistics
import networkx as nx
from backend.pipeline.scorer import score_item, get_risk_level


def calculate_spread(prices: list[float]) -> dict:
    clean = [p for p in prices if p > 0]
    if not clean:
        return {"min": 0, "max": 0, "mean": 0, "std": 0}
    mean = round(statistics.mean(clean), 2)
    std = round(statistics.stdev(clean), 2) if len(clean) > 1 else 0.0
    return {"min": round(min(clean), 2), "max": round(max(clean), 2), "mean": mean, "std": std}


def calculate_graph_metrics(groups: list[dict]) -> dict:
    """Вычисляем графовые метрики для контекста скоринга"""
    G = nx.Graph()

    # Строим граф: item <-> contractor
    item_contractor_edges = []
    for group in groups:
        for item in group.get("items", []):
            item_name = item.get("name", "")
            contractor = item.get("contractor", "")
            if item_name and contractor:
                item_key = f"item:{item_name}"
                contractor_key = f"contractor:{contractor}"
                item_contractor_edges.append((item_key, contractor_key))
                G.add_edge(item_key, contractor_key)

    # Вычисляем центральность если граф не пустой
    if len(G.nodes()) > 0 and len(G.edges()) > 0:
        try:
            centrality = nx.betweenness_centrality(G)
            # Формируем контекст: для каждого item сохраняем его центральность
            graph_context = {}
            for item_key, cent_value in centrality.items():
                if item_key.startswith("item:"):
                    graph_context[item_key] = {"centrality": cent_value}
            return graph_context
        except Exception as e:
            print(f"Ошибка вычисления графовых метрик: {e}")
            return {}
    return {}


def analyze_group(group: dict, graph_context: dict = None) -> dict:
    items = group.get("items", [])
    if not items:
        return {**group, "analysis": {"error": "Нет позиций"}, "aggregated": None}

    prices = [float(i.get("price", 0) or 0) for i in items]
    spread = calculate_spread(prices)

    # Скоринг с графовым контекстом
    scored_items = [score_item(item, group, graph_context) for item in items]

    # Агрегация
    all_flags = []
    departments = []
    contractors = []
    seen_d, seen_c = set(), set()

    for s in scored_items:
        for f in s["flags"]:
            if f not in all_flags:
                all_flags.append(f)
        d, c = s.get("department", ""), s.get("contractor", "")
        if d and d not in seen_d:
            seen_d.add(d)
            departments.append(d)
        if c and c not in seen_c:
            seen_c.add(c)
            contractors.append(c)

    max_score = max(s["score"] for s in scored_items)
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
    if "contractor_blacklist" in all_flags:
        parts.append("Подозрительный контрагент")
    if "temporal_clustering" in all_flags:
        parts.append("Частые закупки в короткий срок")
    if "graph_central" in all_flags:
        parts.append("Высокая центральность в сети закупок")
    if "single_occurrence" in all_flags:
        parts.append("Единственное упоминание")

    explanation = " | ".join(parts) if parts else "Без явных аномалий"

    aggregated = {
        "item": group.get("canonical_name", items[0].get("name", "")),
        "name": group.get("canonical_name", items[0].get("name", "")),
        "departments": departments,
        "contractors": contractors,
        "count": len(items),
        "prices": [p for p in prices if p > 0],
        "spread": spread,
        "score": max_score,
        "risk_level": risk_level,
        "flags": all_flags,
        "explanation": explanation,
    }

    return {
        **group,
        "items": scored_items,
        "aggregated": aggregated,
        "analysis": {
            "spread": spread,
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

    # Вычисляем графовые метрики
    graph_context = calculate_graph_metrics(groups)

    # Анализируем группы с графовым контекстом
    analyzed = [analyze_group(g, graph_context) for g in groups]

    flat_results = [item for g in analyzed for item in g.get("items", [])]
    aggregated_results = sorted(
        [g["aggregated"] for g in analyzed if g.get("aggregated")],
        key=lambda x: x["score"], reverse=True
    )

    total_anomalies = sum(1 for r in aggregated_results if r["score"] >= 20)

    return {
        "groups": analyzed,
        "results": aggregated_results,
        "flat_results": flat_results,
        "total_groups": len(analyzed),
        "total_anomalies": total_anomalies,
        "groups_with_anomalies": [r["item"] for r in aggregated_results if r["score"] >= 20],
        "summary": f"Проанализировано {len(analyzed)} позиций. Аномалий: {total_anomalies}.",
        "graph_context": graph_context  # Для отладки
    }
