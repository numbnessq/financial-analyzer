# backend/pipeline/clusterer.py
"""
Кластеризация позиций по смысловому сходству.

Алгоритм:
  1. canonical_name из normalizer (уже есть)
  2. token-level jaccard similarity
  3. rapidfuzz.fuzz.token_sort_ratio как второй сигнал
  4. Union-Find для группировки транзитивно схожих позиций

Порог сходства: SIMILARITY_THRESHOLD (0.72 по умолчанию).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

try:
    from rapidfuzz import fuzz as _rfuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False


SIMILARITY_THRESHOLD = 0.72   # минимальный score для объединения в кластер


# ─── Нормализация ────────────────────────────────────────────────

def _canonicalize(name: str) -> str:
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", name).lower().replace("ё", "е")
    s = re.sub(r'\d+([.,]\d+)?\s*(м2|м3|шт|кг|л|м)\b', '', s)
    s = re.sub(r'[^a-zа-я0-9 ]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def _tokens(name: str) -> set[str]:
    return set(_canonicalize(name).split())


# ─── Метрики сходства ────────────────────────────────────────────

def jaccard(a: str, b: str) -> float:
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0
    inter = ta & tb
    union = ta | tb
    return len(inter) / len(union)


def similarity(a: str, b: str) -> float:
    """
    Комбинированная метрика:
      - если rapidfuzz доступен: 0.5·jaccard + 0.5·token_sort_ratio
      - иначе: только jaccard
    """
    j = jaccard(a, b)
    if _HAS_RAPIDFUZZ:
        rf = _rfuzz.token_sort_ratio(_canonicalize(a), _canonicalize(b)) / 100
        return round(0.5 * j + 0.5 * rf, 4)
    return round(j, 4)


# ─── Union-Find ──────────────────────────────────────────────────

class _UF:
    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank   = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x: int, y: int):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1


# ─── Кластеризация ───────────────────────────────────────────────

def cluster_items(
    items: list[dict],
    threshold: float = SIMILARITY_THRESHOLD,
    name_key: str = "canonical_name",
) -> list[dict]:
    """
    Принимает список нормализованных позиций.
    Возвращает тот же список с добавленным полем 'cluster_id'.

    cluster_id — строка вида 'cluster_0', 'cluster_1', ...
    Уникальные позиции получают собственный cluster_id.

    Алгоритм O(n²) — приемлем до ~5000 позиций.
    """
    if not items:
        return items

    names = [str(i.get(name_key) or i.get("name") or "") for i in items]
    n     = len(names)
    uf    = _UF(n)

    for i in range(n):
        for j in range(i + 1, n):
            if not names[i] or not names[j]:
                continue
            if similarity(names[i], names[j]) >= threshold:
                uf.union(i, j)

    # Назначаем cluster_id по корню дерева
    root_to_cluster: dict[int, str] = {}
    cluster_counter = 0
    result = []
    for idx, item in enumerate(items):
        root = uf.find(idx)
        if root not in root_to_cluster:
            root_to_cluster[root] = f"cluster_{cluster_counter}"
            cluster_counter += 1
        result.append({**item, "cluster_id": root_to_cluster[root]})

    return result


def build_cluster_map(items: list[dict]) -> dict[str, list[dict]]:
    """
    Возвращает словарь {cluster_id: [items]}.
    Удобно для итерации по кластерам.
    """
    clustered = cluster_items(items)
    mapping: dict[str, list[dict]] = {}
    for item in clustered:
        cid = item.get("cluster_id", "cluster_unknown")
        mapping.setdefault(cid, []).append(item)
    return mapping


def get_cluster_representatives(items: list[dict]) -> list[dict]:
    """
    Для каждого кластера возвращает одного представителя
    (позицию с наибольшим total_price — как наиболее значимую).
    Добавляет поле 'cluster_size'.
    """
    mapping = build_cluster_map(items)
    reps    = []
    for cid, cluster in mapping.items():
        best = max(
            cluster,
            key=lambda i: float(str(i.get("total_price") or 0)
                                .replace(",", ".").replace(" ", "") or 0)
        )
        reps.append({**best, "cluster_id": cid, "cluster_size": len(cluster)})
    return reps