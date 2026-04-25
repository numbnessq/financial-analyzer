# backend/pipeline/graph_builder.py
"""
Граф закупок.
Узлы: item, contractor, document (source_file).
Рёбра: item→contractor (supplies), document→item (contains).
Веса рёбер — суммы total_price.
"""

import networkx as nx

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


def _clean(v) -> str:
    s = str(v or "").strip()
    return "" if s.lower() in _JUNK else s


def _to_float(v) -> float:
    try:
        return float(str(v).replace(",", ".").replace(" ", "") or 0)
    except (TypeError, ValueError):
        return 0.0


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

        if not item_name:
            continue

        item_id    = f"item:{item_name}"
        n_depts    = len(departments)
        item_label = f"{item_name}\n({n_depts} отд.)" if n_depts > 1 else item_name

        # ── Узел позиции ──
        if not G.has_node(item_id):
            G.add_node(item_id,
                type="item",
                label=item_label,
                risk_score=risk_score,
                risk_level=risk_level,
                departments=departments,
                total_price=total_price,
            )
        else:
            if risk_score > G.nodes[item_id].get("risk_score", 0):
                G.nodes[item_id].update({
                    "risk_score": risk_score,
                    "risk_level": risk_level,
                    "label":      item_label,
                })
            G.nodes[item_id]["total_price"] = (
                G.nodes[item_id].get("total_price", 0) + total_price
            )

        # ── Узлы и рёбра: отделы → позиция ──
        for dept in departments:
            dept = _clean(dept)
            if not dept:
                continue
            dept_id = f"dept:{dept}"
            if not G.has_node(dept_id):
                G.add_node(dept_id, type="department", label=dept)
            if G.has_edge(dept_id, item_id):
                G[dept_id][item_id]["weight"] += 1
            else:
                G.add_edge(dept_id, item_id, type="bought", label="купил", weight=1)

        # ── Узлы и рёбра: позиция → поставщики (с весом = сумма) ──
        for cont in contractors:
            cont = _clean(cont)
            if not cont:
                continue
            cont_id = f"contractor:{cont}"
            if not G.has_node(cont_id):
                G.add_node(cont_id, type="contractor", label=cont, total_supply=0.0)
            G.nodes[cont_id]["total_supply"] = (
                G.nodes[cont_id].get("total_supply", 0) + total_price
            )
            if G.has_edge(item_id, cont_id):
                G[item_id][cont_id]["weight"]      += total_price
                G[item_id][cont_id]["item_count"]  += 1
            else:
                G.add_edge(item_id, cont_id,
                    type="supplies",
                    label="поставляет",
                    weight=total_price,
                    item_count=1,
                )

        # ── Узлы и рёбра: документы → позиция ──
        doc_sources = sources or list({
            i.get("source_file", "") for i in items if i.get("source_file")
        })
        for src in doc_sources:
            src = _clean(src)
            if not src:
                continue
            doc_id = f"doc:{src}"
            if not G.has_node(doc_id):
                G.add_node(doc_id, type="document", label=src)
            if G.has_edge(doc_id, item_id):
                G[doc_id][item_id]["weight"] += 1
            else:
                G.add_edge(doc_id, item_id,
                    type="contains",
                    label="содержит",
                    weight=1,
                )

    return G


def export_json(G: nx.DiGraph, min_score: int = 0, node_types: list = None) -> dict:
    """
    Экспортирует граф в JSON для frontend.

    Параметры фильтрации:
      min_score:  минимальный risk_score для узлов типа item (0 = все)
      node_types: список типов узлов для включения (None = все)
    """
    nodes = []
    for node_id, data in G.nodes(data=True):
        node_type  = data.get("type", "unknown")
        risk_level = data.get("risk_level", "")
        risk_score = data.get("risk_score") or 0

        # Фильтрация
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
        }
        if node_type == "item":
            node["departments"] = data.get("departments", [])
        if node_type == "contractor":
            node["total_supply"] = round(data.get("total_supply", 0), 2)

        nodes.append(node)

    # Фильтруем рёбра по оставшимся узлам
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

    return {"nodes": nodes, "edges": edges}