"""
graph_builder.py — Построение графа связей организации.
Файл: backend/pipeline/graph_builder.py

Узлы:
  - department  (отделение)
  - contractor  (контрагент)
  - item        (позиция/товар)

Связи:
  - department → item       (отделение купило позицию)
  - department → contractor (отделение работает с контрагентом)
  - contractor → item       (контрагент поставляет позицию)
  - department → department (общие позиции между отделениями)

Экспорт: JSON { nodes: [...], edges: [...] }
"""

import json
import networkx as nx
from pydantic import BaseModel
from backend.pipeline.scorer import ScoreResult


# ─────────────────────────────────────────────
# Схемы входных данных
# ─────────────────────────────────────────────

class PurchaseRecord(BaseModel):
    """Одна запись о закупке из документа."""
    department:  str        # отделение, например "Отдел строительства"
    contractor:  str        # контрагент, например "ООО Стройснаб"
    item_name:   str        # позиция, например "бетон М300"
    price:       float      # цена
    quantity:    float = 1  # количество
    unit:        str = ""   # единица измерения
    risk_score:  int = 0    # скор из scorer.py (0–100)
    risk_level:  str = "LOW"


# ─────────────────────────────────────────────
# Построение графа
# ─────────────────────────────────────────────

def build_graph(records: list[PurchaseRecord]) -> nx.DiGraph:
    G = nx.DiGraph()

    for rec in records:
        dept_id       = f"dept::{rec.department}"
        contractor_id = f"cont::{rec.contractor}"
        item_id       = f"item::{rec.item_name}"

        # ── Узлы ──────────────────────────────

        if not G.has_node(dept_id):
            G.add_node(dept_id,
                type="department",
                label=rec.department,
            )

        if not G.has_node(contractor_id):
            G.add_node(contractor_id,
                type="contractor",
                label=rec.contractor,
            )

        if not G.has_node(item_id):
            G.add_node(item_id,
                type="item",
                label=rec.item_name,
                risk_score=rec.risk_score,
                risk_level=rec.risk_level,
            )
        else:
            # обновляем риск если выше
            if rec.risk_score > G.nodes[item_id].get("risk_score", 0):
                G.nodes[item_id]["risk_score"] = rec.risk_score
                G.nodes[item_id]["risk_level"] = rec.risk_level

        # ── Связи ─────────────────────────────

        # отделение → позиция
        if G.has_edge(dept_id, item_id):
            G[dept_id][item_id]["weight"] += rec.price * rec.quantity
        else:
            G.add_edge(dept_id, item_id,
                type="bought",
                label="купил",
                weight=rec.price * rec.quantity,
            )

        # отделение → контрагент
        if G.has_edge(dept_id, contractor_id):
            G[dept_id][contractor_id]["weight"] += rec.price * rec.quantity
        else:
            G.add_edge(dept_id, contractor_id,
                type="works_with",
                label="работает с",
                weight=rec.price * rec.quantity,
            )

        # контрагент → позиция
        if G.has_edge(contractor_id, item_id):
            G[contractor_id][item_id]["weight"] += rec.price * rec.quantity
        else:
            G.add_edge(contractor_id, item_id,
                type="supplies",
                label="поставляет",
                weight=rec.price * rec.quantity,
            )

    # ── Связи между отделениями (общие позиции) ──
    dept_items: dict[str, set] = {}
    for rec in records:
        dept_id = f"dept::{rec.department}"
        item_id = f"item::{rec.item_name}"
        dept_items.setdefault(dept_id, set()).add(item_id)

    depts = list(dept_items.keys())
    for i in range(len(depts)):
        for j in range(i + 1, len(depts)):
            shared = dept_items[depts[i]] & dept_items[depts[j]]
            if shared:
                G.add_edge(depts[i], depts[j],
                    type="shared_items",
                    label="общие позиции",
                    shared_count=len(shared),
                    shared_items=[s.replace("item::", "") for s in shared],
                )

    return G


# ─────────────────────────────────────────────
# Экспорт в JSON для фронтенда
# ─────────────────────────────────────────────

RISK_COLORS = {
    "LOW":      "#4caf50",  # зелёный
    "MEDIUM":   "#ff9800",  # оранжевый
    "HIGH":     "#f44336",  # красный
    "CRITICAL": "#9c27b0",  # фиолетовый
}

NODE_COLORS = {
    "department": "#2196f3",  # синий
    "contractor": "#009688",  # бирюзовый
    "item":       "#607d8b",  # серый (перекрывается риском)
}

def export_json(G: nx.DiGraph) -> dict:
    nodes = []
    for node_id, data in G.nodes(data=True):
        node_type = data.get("type", "unknown")
        risk_level = data.get("risk_level", "")

        # цвет: для позиций — по риску, для остальных — по типу
        if node_type == "item" and risk_level:
            color = RISK_COLORS.get(risk_level, NODE_COLORS["item"])
        else:
            color = NODE_COLORS.get(node_type, "#999")

        nodes.append({
            "id":         node_id,
            "label":      data.get("label", node_id),
            "type":       node_type,
            "color":      color,
            "risk_score": data.get("risk_score", None),
            "risk_level": risk_level or None,
        })

    edges = []
    for src, dst, data in G.edges(data=True):
        edges.append({
            "source": src,
            "target": dst,
            "type":   data.get("type", ""),
            "label":  data.get("label", ""),
            "weight": round(data.get("weight", 1), 2),
        })

    return {"nodes": nodes, "edges": edges}


def export_json_file(G: nx.DiGraph, path: str = "graph.json"):
    data = export_json(G)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Граф сохранён: {path}  ({len(data['nodes'])} узлов, {len(data['edges'])} связей)")


# ─────────────────────────────────────────────
# Демо / запуск
# ─────────────────────────────────────────────

if __name__ == "__main__":
    records = [
        PurchaseRecord(department="Отдел строительства", contractor="ООО СтройСнаб",   item_name="бетон М300",        price=4500,  quantity=50, unit="м3",  risk_score=30, risk_level="MEDIUM"),
        PurchaseRecord(department="Отдел строительства", contractor="ООО СтройСнаб",   item_name="арматура А500С",    price=85000, quantity=2,  unit="т",   risk_score=35, risk_level="MEDIUM"),
        PurchaseRecord(department="Отдел строительства", contractor="ИП Иванов",        item_name="бетон М300",        price=7000,  quantity=20, unit="м3",  risk_score=30, risk_level="MEDIUM"),
        PurchaseRecord(department="Отдел ремонта",       contractor="ООО РемСервис",    item_name="кирпич облицовочный", price=12000, quantity=10, unit="шт", risk_score=0,  risk_level="LOW"),
        PurchaseRecord(department="Отдел ремонта",       contractor="ООО СтройСнаб",   item_name="бетон М300",        price=4600,  quantity=30, unit="м3",  risk_score=30, risk_level="MEDIUM"),
        PurchaseRecord(department="Отдел логистики",     contractor="ИП Петров",        item_name="услуги экскаватора", price=25000, quantity=5,  unit="ч",  risk_score=100, risk_level="CRITICAL"),
    ]

    G = build_graph(records)
    export_json_file(G, "graph.json")

    print(f"\nУзлы ({G.number_of_nodes()}):")
    for node_id, data in G.nodes(data=True):
        print(f"  [{data['type']:12}] {data['label']}", end="")
        if data.get("risk_score"):
            print(f"  ← риск {data['risk_score']}/100 {data['risk_level']}", end="")
        print()

    print(f"\nСвязи ({G.number_of_edges()}):")
    for src, dst, data in G.edges(data=True):
        print(f"  {G.nodes[src]['label']:25} --[{data['label']}]--> {G.nodes[dst]['label']}")