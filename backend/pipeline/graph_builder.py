# backend/pipeline/graph_builder.py

import networkx as nx

RISK_COLORS = {"LOW": "#3dd68c", "MEDIUM": "#f7a24f", "HIGH": "#f75a5a", "CRITICAL": "#9c27b0"}
NODE_COLORS = {"department": "#2196f3", "contractor": "#009688", "item": "#607d8b"}

_JUNK = {"не указан", "unknown", "неизвестно", "неизвестный контрагент",
         "неизвестный отдел", "-", "—", "none", "null", ""}


def _clean(v) -> str:
    s = str(v or "").strip()
    return "" if s.lower() in _JUNK else s


def build_graph_from_aggregated(aggregated_results: list[dict]) -> nx.DiGraph:
    """
    Строит граф из агрегированных результатов.
    Один узел Item = одна позиция для всех отделов.
    """
    G = nx.DiGraph()

    for result in aggregated_results:
        item_name   = _clean(result.get("item") or result.get("name", ""))
        departments = result.get("departments", [])
        contractors = result.get("contractors", [])
        risk_score  = int(result.get("score", 0))
        risk_level  = result.get("risk_level", "LOW")

        if not item_name:
            continue

        item_id = f"item:{item_name}"
        n_depts = len(departments)
        item_label = f"{item_name}\n({n_depts} отд.)" if n_depts > 1 else item_name

        if not G.has_node(item_id):
            G.add_node(item_id, type="item", label=item_label,
                       risk_score=risk_score, risk_level=risk_level,
                       departments=departments)
        elif risk_score > G.nodes[item_id].get("risk_score", 0):
            G.nodes[item_id].update({"risk_score": risk_score,
                                     "risk_level": risk_level,
                                     "label": item_label})

        for dept in departments:
            dept = _clean(dept)
            if not dept: continue
            dept_id = f"dept:{dept}"
            if not G.has_node(dept_id):
                G.add_node(dept_id, type="department", label=dept)
            if G.has_edge(dept_id, item_id):
                G[dept_id][item_id]["weight"] += 1
            else:
                G.add_edge(dept_id, item_id, type="bought", label="купил", weight=1)

        for cont in contractors:
            cont = _clean(cont)
            if not cont: continue
            cont_id = f"contractor:{cont}"
            if not G.has_node(cont_id):
                G.add_node(cont_id, type="contractor", label=cont)
            if G.has_edge(item_id, cont_id):
                G[item_id][cont_id]["weight"] += 1
            else:
                G.add_edge(item_id, cont_id, type="supplies", label="поставляет", weight=1)

    return G


def export_json(G: nx.DiGraph) -> dict:
    nodes = []
    for node_id, data in G.nodes(data=True):
        node_type  = data.get("type", "unknown")
        risk_level = data.get("risk_level", "")
        color = (RISK_COLORS.get(risk_level, NODE_COLORS["item"])
                 if node_type == "item" and risk_level
                 else NODE_COLORS.get(node_type, "#999"))
        node = {"id": node_id, "label": data.get("label", node_id),
                "type": node_type, "color": color,
                "risk_score": data.get("risk_score"),
                "risk_level": risk_level or None}
        if node_type == "item":
            node["departments"] = data.get("departments", [])
        nodes.append(node)

    edges = [{"source": s, "target": d,
              "type": data.get("type", ""), "label": data.get("label", ""),
              "weight": round(data.get("weight", 1), 2)}
             for s, d, data in G.edges(data=True)]

    return {"nodes": nodes, "edges": edges}