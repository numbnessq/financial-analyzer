# backend/pipeline/graph_builder.py

import json
import networkx as nx
from pydantic import BaseModel


class PurchaseRecord(BaseModel):
    department: str
    contractor: str
    item_name:  str
    price:      float
    quantity:   float = 1
    unit:       str   = ""
    risk_score: int   = 0
    risk_level: str   = "LOW"


RISK_COLORS  = {"LOW": "#3dd68c", "MEDIUM": "#f7a24f", "HIGH": "#f75a5a", "CRITICAL": "#9c27b0"}
NODE_COLORS  = {"department": "#2196f3", "contractor": "#009688", "item": "#607d8b"}

# Мусорные значения — не создаём узлы
_JUNK = {"не указан", "unknown", "неизвестно", "неизвестный контрагент",
         "неизвестный отдел", "-", "—", "none", "null", ""}


def _clean(v: str) -> str:
    if not v:
        return ""
    s = v.strip()
    return "" if s.lower() in _JUNK else s


def build_graph(records: list[PurchaseRecord]) -> nx.DiGraph:
    G = nx.DiGraph()

    for rec in records:
        dept       = _clean(rec.department)
        contractor = _clean(rec.contractor)
        item       = _clean(rec.item_name)

        if not dept or not item:
            continue

        dept_id = f"department:{dept}"
        item_id = f"item:{item}"

        # Узел: подразделение
        if not G.has_node(dept_id):
            G.add_node(dept_id, type="department", label=dept)

        # Узел: позиция
        if not G.has_node(item_id):
            G.add_node(item_id, type="item", label=item,
                       risk_score=rec.risk_score, risk_level=rec.risk_level)
        elif rec.risk_score > G.nodes[item_id].get("risk_score", 0):
            G.nodes[item_id]["risk_score"] = rec.risk_score
            G.nodes[item_id]["risk_level"] = rec.risk_level

        # Ребро: department → item
        if G.has_edge(dept_id, item_id):
            G[dept_id][item_id]["weight"] += rec.price * rec.quantity
        else:
            G.add_edge(dept_id, item_id, type="bought", label="купил",
                       weight=rec.price * rec.quantity)

        # Контрагент — только если он реально задан
        if contractor:
            cont_id = f"contractor:{contractor}"
            if not G.has_node(cont_id):
                G.add_node(cont_id, type="contractor", label=contractor)

            if G.has_edge(dept_id, cont_id):
                G[dept_id][cont_id]["weight"] += rec.price * rec.quantity
            else:
                G.add_edge(dept_id, cont_id, type="works_with", label="работает с",
                           weight=rec.price * rec.quantity)

            if G.has_edge(cont_id, item_id):
                G[cont_id][item_id]["weight"] += rec.price * rec.quantity
            else:
                G.add_edge(cont_id, item_id, type="supplies", label="поставляет",
                           weight=rec.price * rec.quantity)

    return G


def export_json(G: nx.DiGraph) -> dict:
    nodes = []
    for node_id, data in G.nodes(data=True):
        node_type  = data.get("type", "unknown")
        risk_level = data.get("risk_level", "")
        color = RISK_COLORS.get(risk_level, NODE_COLORS["item"]) if node_type == "item" and risk_level \
                else NODE_COLORS.get(node_type, "#999")
        nodes.append({
            "id": node_id, "label": data.get("label", node_id),
            "type": node_type, "color": color,
            "risk_score": data.get("risk_score"), "risk_level": risk_level or None,
        })

    edges = []
    for src, dst, data in G.edges(data=True):
        edges.append({
            "source": src, "target": dst,
            "type": data.get("type", ""), "label": data.get("label", ""),
            "weight": round(data.get("weight", 1), 2),
        })

    return {"nodes": nodes, "edges": edges}