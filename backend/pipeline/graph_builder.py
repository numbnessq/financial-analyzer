# backend/pipeline/graph_builder.py
"""
Граф закупок v2.

Узлы: item, contractor, document, department
Рёбра: dept→item (bought), item→contractor (supplies), doc→item (contains)

v2:
  - Вычисление betweenness centrality после построения графа
  - Centrality сохраняется в узлы item → флаг graph_central в scorer.py начинает работать
  - export_json возвращает centrality в каждом узле
  - build_graph_context() → dict для scorer (item_key → {centrality})
  - Доминирующие поставщики выявляются через degree + total_supply
  - export_json принимает include_stats=True для сводной аналитики
"""

from __future__ import annotations

import logging
from typing import Optional

import networkx as nx

log = logging.getLogger(__name__)

RISK_COLORS = {
    "LOW":      "#3dd68c",
    "MEDIUM":   "#f7a24f",
    "HIGH":     "#f75a5a",
    "CRITICAL": "#9c27b0",
}
NODE_COLORS = {
    "item":       "#607d8b",
    "contractor": "#009688",
    "department": "#2196f3",
    "document":   "#795548",
}

_JUNK = {
    "не указан", "unknown", "неизвестно", "неизвестный контрагент",
    "неизвестный отдел", "-", "—", "none", "null", "",
}

# Порог centrality для флага graph_central в scorer.py
CENTRALITY_THRESHOLD = 0.1


def _clean(v) -> str:
    s = str(v or "").strip()
    return "" if s.lower() in _JUNK else s


def _to_float(v) -> float:
    try:
        return float(str(v).replace(",", ".").replace(" ", "") or 0)
    except (TypeError, ValueError):
        return 0.0


# ─── Построение графа ────────────────────────────────────────────

def build_graph_from_aggregated(aggregated_results: list) -> nx.DiGraph:
    G = nx.DiGraph()

    for result in aggregated_results:
        item_name   = _clean(result.get("item") or result.get("name", ""))
        departments = result.get("departments", [])
        contractors = result.get("contractors", [])
        sources     = result.get("sources", [])
        risk_score  = int(result.get("score", 0))
        risk_level  = result.get("risk_level", "LOW")
        total_price = _to_float(result.get("total_price") or 0)
        items       = result.get("items", [])
        flags       = result.get("flags", [])
        category    = result.get("category", "default")

        if not item_name:
            continue

        item_id    = f"item:{item_name}"
        n_depts    = len(departments)
        item_label = f"{item_name}\n({n_depts} отд.)" if n_depts > 1 else item_name

        # ── Узел позиции ──
        if not G.has_node(item_id):
            G.add_node(
                item_id,
                type        = "item",
                label       = item_label,
                risk_score  = risk_score,
                risk_level  = risk_level,
                departments = departments,
                total_price = total_price,
                flags       = flags,
                category    = category,
                centrality  = 0.0,   # будет заполнено ниже
            )
        else:
            existing = G.nodes[item_id]
            if risk_score > existing.get("risk_score", 0):
                existing.update({
                    "risk_score": risk_score,
                    "risk_level": risk_level,
                    "label":      item_label,
                    "flags":      flags,
                })
            existing["total_price"] = existing.get("total_price", 0) + total_price

        # ── Отделы → позиция ──
        for dept in departments:
            dept = _clean(dept)
            if not dept:
                continue
            dept_id = f"dept:{dept}"
            if not G.has_node(dept_id):
                G.add_node(dept_id, type="department", label=dept, centrality=0.0)
            if G.has_edge(dept_id, item_id):
                G[dept_id][item_id]["weight"] += 1
            else:
                G.add_edge(dept_id, item_id, type="bought", label="купил", weight=1)

        # ── Позиция → поставщики ──
        for cont in contractors:
            cont = _clean(cont)
            if not cont:
                continue
            cont_id = f"contractor:{cont}"
            if not G.has_node(cont_id):
                G.add_node(
                    cont_id,
                    type         = "contractor",
                    label        = cont,
                    total_supply = 0.0,
                    item_count   = 0,
                    centrality   = 0.0,
                )
            G.nodes[cont_id]["total_supply"] = (
                G.nodes[cont_id].get("total_supply", 0) + total_price
            )
            G.nodes[cont_id]["item_count"] = (
                G.nodes[cont_id].get("item_count", 0) + 1
            )
            if G.has_edge(item_id, cont_id):
                G[item_id][cont_id]["weight"]     += total_price
                G[item_id][cont_id]["item_count"] += 1
            else:
                G.add_edge(
                    item_id, cont_id,
                    type       = "supplies",
                    label      = "поставляет",
                    weight     = total_price,
                    item_count = 1,
                )

        # ── Документы → позиция ──
        doc_sources = sources or list({
            i.get("source_file", "") for i in items if i.get("source_file")
        })
        for src in doc_sources:
            src = _clean(src)
            if not src:
                continue
            doc_id = f"doc:{src}"
            if not G.has_node(doc_id):
                G.add_node(doc_id, type="document", label=src, centrality=0.0)
            if G.has_edge(doc_id, item_id):
                G[doc_id][item_id]["weight"] += 1
            else:
                G.add_edge(doc_id, item_id, type="contains", label="содержит", weight=1)

    # ── Вычисление централизованности ────────────────────────────
    _compute_centrality(G)

    return G


def _compute_centrality(G: nx.DiGraph) -> None:
    """
    Вычисляет betweenness centrality для всех узлов.
    Записывает нормализованное значение в атрибут 'centrality'.

    Для больших графов (> 500 узлов) использует приближённый алгоритм
    с выборкой k=min(500, n) источников — O(k·(V+E)·log V).
    """
    n = len(G.nodes)
    if n < 3:
        return

    try:
        Gu = G.to_undirected()

        if n <= 500:
            centrality = nx.betweenness_centrality(Gu, normalized=True, weight=None)
        else:
            # Приближённый алгоритм для больших графов
            k = min(500, n)
            centrality = nx.betweenness_centrality(
                Gu, k=k, normalized=True, weight=None, seed=42
            )

        for node_id, score in centrality.items():
            if node_id in G.nodes:
                G.nodes[node_id]["centrality"] = round(score, 4)

    except Exception as e:
        log.warning(f"graph_builder: centrality computation failed: {e}")


# ─── Graph context для scorer ────────────────────────────────────

def build_graph_context(G: nx.DiGraph) -> dict:
    """
    Строит словарь {item_key: {centrality: float}} для передачи в scorer.py.

    scorer.py проверяет:
      if graph_context and item_key in graph_context:
          ctx = graph_context[item_key]
          if ctx.get("centrality", 0) > CENTRALITY_THRESHOLD:
              flags.append("graph_central")

    Вызывается в main.py после build_graph_from_aggregated.
    """
    context = {}
    for node_id, data in G.nodes(data=True):
        if data.get("type") == "item":
            centrality = data.get("centrality", 0.0)
            context[node_id] = {
                "centrality":  centrality,
                "risk_score":  data.get("risk_score", 0),
                "total_price": data.get("total_price", 0),
            }
    return context


# ─── Доминирующие поставщики ─────────────────────────────────────

def find_dominant_suppliers(
    G:               nx.DiGraph,
    top_n:           int   = 10,
    min_supply:      float = 0.0,
) -> list[dict]:
    """
    Выявляет доминирующих поставщиков по:
      - total_supply (объём закупок)
      - in_degree (количество позиций)
      - centrality (роль в сети)

    Возвращает топ-N поставщиков по комбинированному score.
    """
    suppliers = []
    total_supply_all = sum(
        data.get("total_supply", 0)
        for _, data in G.nodes(data=True)
        if data.get("type") == "contractor"
    )

    for node_id, data in G.nodes(data=True):
        if data.get("type") != "contractor":
            continue
        supply = data.get("total_supply", 0)
        if supply < min_supply:
            continue

        # Количество позиций которые поставляет контрагент
        # = входящие рёбра от item-узлов
        item_count = sum(
            1 for pred in G.predecessors(node_id)
            if G.nodes[pred].get("type") == "item"
        )

        # Количество уникальных отделов через которые закупают у этого поставщика
        dept_count = sum(
            1 for pred in G.predecessors(node_id)
            if G.nodes[pred].get("type") == "department"
        )

        share_pct = round(supply / total_supply_all * 100, 2) if total_supply_all > 0 else 0.0

        suppliers.append({
            "contractor":   data.get("label", node_id),
            "node_id":      node_id,
            "total_supply": round(supply, 2),
            "share_pct":    share_pct,
            "item_count":   item_count,
            "dept_count":   dept_count,
            "centrality":   round(data.get("centrality", 0), 4),
            # Комбинированный score: 60% доля + 30% позиции + 10% centrality
            "_sort_score":  share_pct * 0.6 + item_count * 0.3 + data.get("centrality", 0) * 0.1 * 100,
        })

    suppliers.sort(key=lambda x: x["_sort_score"], reverse=True)

    for s in suppliers:
        del s["_sort_score"]

    return suppliers[:top_n]


# ─── Export ──────────────────────────────────────────────────────

def export_json(
    G:             nx.DiGraph,
    min_score:     int          = 0,
    node_types:    list         = None,
    include_stats: bool         = False,
) -> dict:
    """
    Экспортирует граф в JSON для frontend.

    Параметры:
      min_score:     минимальный risk_score для item-узлов (0 = все)
      node_types:    список типов для включения (None = все)
      include_stats: добавить сводную аналитику (dominant_suppliers, etc.)
    """
    nodes = []
    for node_id, data in G.nodes(data=True):
        node_type  = data.get("type", "unknown")
        risk_level = data.get("risk_level", "")
        risk_score = data.get("risk_score") or 0

        if node_types and node_type not in node_types:
            continue
        if node_type == "item" and risk_score < min_score:
            continue

        color = (
            RISK_COLORS.get(risk_level, NODE_COLORS["item"])
            if node_type == "item" and risk_level
            else NODE_COLORS.get(node_type, "#999")
        )

        node = {
            "id":          node_id,
            "label":       data.get("label", node_id),
            "type":        node_type,
            "color":       color,
            "risk_score":  risk_score if node_type == "item" else None,
            "risk_level":  risk_level or None,
            "total_price": round(data.get("total_price", 0) or 0, 2),
            "centrality":  round(data.get("centrality", 0), 4),
        }

        if node_type == "item":
            node["departments"] = data.get("departments", [])
            node["flags"]       = data.get("flags", [])
            node["category"]    = data.get("category", "default")

        if node_type == "contractor":
            node["total_supply"] = round(data.get("total_supply", 0), 2)
            node["item_count"]   = data.get("item_count", 0)

        nodes.append(node)

    node_ids = {n["id"] for n in nodes}
    edges = []
    for s, d, data in G.edges(data=True):
        if s not in node_ids or d not in node_ids:
            continue
        edges.append({
            "source":     s,
            "target":     d,
            "type":       data.get("type", ""),
            "label":      data.get("label", ""),
            "weight":     round(data.get("weight", 1), 2),
            "item_count": data.get("item_count"),
        })

    result: dict = {"nodes": nodes, "edges": edges}

    if include_stats:
        result["stats"] = _graph_stats(G, nodes, edges)
        result["dominant_suppliers"] = find_dominant_suppliers(G, top_n=5)

    return result


def _graph_stats(G: nx.DiGraph, nodes: list, edges: list) -> dict:
    by_type: dict[str, int] = {}
    for n in nodes:
        t = n.get("type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1

    item_nodes = [n for n in nodes if n.get("type") == "item"]
    centralities = [n.get("centrality", 0) for n in item_nodes if n.get("centrality", 0) > 0]

    return {
        "node_count":         len(nodes),
        "edge_count":         len(edges),
        "by_type":            by_type,
        "high_centrality":    sum(1 for c in centralities if c > CENTRALITY_THRESHOLD),
        "max_centrality":     round(max(centralities), 4) if centralities else 0.0,
        "density":            round(nx.density(G), 4),
    }