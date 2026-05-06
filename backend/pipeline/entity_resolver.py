# backend/pipeline/entity_resolver.py
"""
Entity Resolution для контрагентов.

Задача: "ООО «Ромашка»" = "ООО Ромашка" = "РОМАШКА ООО" → canonical entity.

Алгоритм:
  1. Нормализация: удаление OPF, кавычек, приведение к нижнему регистру
  2. INN-based dedup (primary key если есть)
  3. Exact normalized match
  4. Fuzzy clustering с blocking по токенам (O(n·k), k << n)
  5. Union-Find для транзитивного объединения
  6. Canonical name: наиболее длинное/частое название в кластере

Интеграция: вызывается в main.py::_run_analysis_job ДО match_across_documents.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

try:
    from rapidfuzz import fuzz as _fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False


# ─── OPF patterns ────────────────────────────────────────────────

_OPF_RE = re.compile(
    r'\b(?:'
    r'общество\s+с\s+ограниченной\s+ответственностью'
    r'|закрытое\s+акционерное\s+общество'
    r'|открытое\s+акционерное\s+общество'
    r'|публичное\s+акционерное\s+общество'
    r'|акционерное\s+общество'
    r'|индивидуальный\s+предприниматель'
    r'|о{1,2}о|зао|оао|пао|ао|гуп|муп|фгуп|фгбу|фку|бу|нко|нп|ип|сп|тоо|пк'
    r'|llc|ltd|gmbh|sarl|sas|inc'
    r')\b',
    re.IGNORECASE,
)

_JUNK_NAMES = {
    "", "-", "—", "none", "null", "unknown",
    "н/д", "нет", "не указан", "неизвестно",
    "неизвестный контрагент", "не определен", "не определён",
}


# ─── Нормализация ────────────────────────────────────────────────

def normalize_contractor(name: str) -> str:
    """
    Полная нормализация названия контрагента для сравнения.
    НЕ изменяет canonical_name — только для внутренних операций.
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", name)
    s = s.lower().replace("ё", "е")
    # Убираем кавычки всех видов
    s = re.sub(r'[«»""„‟\'`\u2018\u2019\u201c\u201d\u201e\u201f]', '', s)
    # Убираем OPF
    s = _OPF_RE.sub('', s)
    # Убираем скобки, пунктуацию (оставляем дефисы внутри слов)
    s = re.sub(r'[\(\)\[\]{}/\\|]', ' ', s)
    s = re.sub(r'[^\w\s\-]', ' ', s)
    # Коллапсируем пробелы
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _tokens(name: str) -> set[str]:
    """Токены нормализованного имени длиной >= 3 символа."""
    return {t for t in normalize_contractor(name).split() if len(t) >= 3}


def _is_junk(name: str) -> bool:
    return not name or name.lower().strip() in _JUNK_NAMES


# ─── Similarity ──────────────────────────────────────────────────

ENTITY_THRESHOLD = 0.88   # строже, чем для item clustering


def _similarity(a: str, b: str) -> float:
    na, nb = normalize_contractor(a), normalize_contractor(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    ta, tb = set(na.split()), set(nb.split())
    if not ta or not tb:
        return 0.0
    jaccard = len(ta & tb) / len(ta | tb)
    if _HAS_RAPIDFUZZ:
        ratio = _fuzz.token_sort_ratio(na, nb) / 100.0
        return round(0.35 * jaccard + 0.65 * ratio, 4)
    return round(jaccard, 4)


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


# ─── Модели ──────────────────────────────────────────────────────

@dataclass
class ResolvedEntity:
    entity_id:       str
    canonical_name:  str
    normalized_name: str
    aliases:         list[str]       = field(default_factory=list)
    inn:             Optional[str]   = None
    confidence:      float           = 1.0   # 1.0 = INN; 0.9 = exact norm; <0.9 = fuzzy
    mention_count:   int             = 0

    def to_dict(self) -> dict:
        return {
            "entity_id":       self.entity_id,
            "canonical_name":  self.canonical_name,
            "normalized_name": self.normalized_name,
            "aliases":         self.aliases,
            "inn":             self.inn,
            "confidence":      self.confidence,
            "mention_count":   self.mention_count,
        }


# ─── Entity Registry ─────────────────────────────────────────────

class EntityRegistry:
    """
    Реестр resolved entities.
    Маппинг: raw_name → ResolvedEntity.

    После построения (resolve_contractors) используется для:
      - apply_resolution(items) → замена contractor на canonical
      - get_canonical_name(raw) → строка
      - get_entity_id(raw) → id для аналитики
    """

    def __init__(self):
        self._entities:   dict[str, ResolvedEntity] = {}
        self._raw_to_id:  dict[str, str]            = {}
        self._inn_to_id:  dict[str, str]            = {}
        self._norm_to_id: dict[str, str]            = {}
        # Заготовим singleton для "неизвестных"
        _unknown = ResolvedEntity(
            entity_id="contractor:__unknown__",
            canonical_name="Не указан",
            normalized_name="",
        )
        self._entities[_unknown.entity_id] = _unknown

    def _make_id(self, name: str, inn: Optional[str]) -> str:
        if inn:
            return f"inn:{inn}"
        base = normalize_contractor(name)[:30].strip().replace(" ", "_")
        h    = hashlib.md5(base.encode()).hexdigest()[:6]
        return f"c:{base}:{h}"

    def register(self, raw_name: str, inn: Optional[str] = None) -> ResolvedEntity:
        """
        Регистрирует контрагента.
        Если уже существует (по raw, INN или normalized) — возвращает существующего.
        """
        if _is_junk(raw_name):
            return self._entities["contractor:__unknown__"]

        raw_name = raw_name.strip()

        # 1. Кэш raw
        if raw_name in self._raw_to_id:
            eid = self._raw_to_id[raw_name]
            if eid in self._entities:
                self._entities[eid].mention_count += 1
                return self._entities[eid]

        # 2. INN match
        if inn and inn in self._inn_to_id:
            eid    = self._inn_to_id[inn]
            entity = self._entities[eid]
            if raw_name not in entity.aliases:
                entity.aliases.append(raw_name)
            entity.mention_count += 1
            self._raw_to_id[raw_name] = eid
            return entity

        norm = normalize_contractor(raw_name)

        # 3. Normalized exact match
        if norm and norm in self._norm_to_id:
            eid    = self._norm_to_id[norm]
            entity = self._entities[eid]
            if raw_name not in entity.aliases:
                entity.aliases.append(raw_name)
            entity.mention_count += 1
            self._raw_to_id[raw_name] = eid
            return entity

        # 4. Новая сущность
        base_id   = self._make_id(raw_name, inn)
        entity_id = base_id
        suffix    = 0
        while entity_id in self._entities:
            suffix   += 1
            entity_id = f"{base_id}_{suffix}"

        entity = ResolvedEntity(
            entity_id       = entity_id,
            canonical_name  = raw_name,
            normalized_name = norm,
            aliases         = [raw_name],
            inn             = inn,
            confidence      = 1.0 if inn else 0.9,
            mention_count   = 1,
        )
        self._entities[entity_id] = entity
        self._raw_to_id[raw_name] = entity_id
        if inn:
            self._inn_to_id[inn]  = entity_id
        if norm:
            self._norm_to_id[norm] = entity_id

        return entity

    def merge(self, primary_id: str, secondary_id: str) -> bool:
        """
        Поглощает secondary → primary.
        Primary получает все aliases и mentions secondary.
        Выбирает более полный canonical_name.
        """
        if primary_id == secondary_id:
            return False
        if primary_id not in self._entities or secondary_id not in self._entities:
            return False
        if primary_id == "contractor:__unknown__":
            return False

        ep = self._entities[primary_id]
        es = self._entities[secondary_id]

        # canonical_name: выбираем более длинное и упоминаемое
        if (len(es.canonical_name) > len(ep.canonical_name)
                and es.mention_count >= ep.mention_count):
            ep.canonical_name = es.canonical_name

        # Объединяем aliases
        for alias in es.aliases:
            if alias not in ep.aliases:
                ep.aliases.append(alias)
            self._raw_to_id[alias] = primary_id

        ep.mention_count += es.mention_count
        ep.confidence     = min(ep.confidence, es.confidence)

        if not ep.inn and es.inn:
            ep.inn = es.inn
            self._inn_to_id[es.inn] = primary_id

        if es.normalized_name and es.normalized_name not in self._norm_to_id:
            self._norm_to_id[es.normalized_name] = primary_id
        elif es.normalized_name in self._norm_to_id:
            self._norm_to_id[es.normalized_name] = primary_id

        del self._entities[secondary_id]
        return True

    # ── Публичный API ──────────────────────────────────────

    def resolve(self, raw_name: str) -> ResolvedEntity:
        if _is_junk(raw_name):
            return self._entities["contractor:__unknown__"]
        raw_name = raw_name.strip()
        eid = self._raw_to_id.get(raw_name)
        if eid and eid in self._entities:
            return self._entities[eid]
        return self._entities["contractor:__unknown__"]

    def get_canonical_name(self, raw_name: str) -> str:
        return self.resolve(raw_name).canonical_name

    def get_entity_id(self, raw_name: str) -> str:
        return self.resolve(raw_name).entity_id

    def all_entities(self) -> list[ResolvedEntity]:
        return [
            e for eid, e in self._entities.items()
            if eid != "contractor:__unknown__"
        ]

    def stats(self) -> dict:
        entities = self.all_entities()
        n_raw    = len(self._raw_to_id)
        n_canon  = len(entities)
        merged   = sum(1 for e in entities if len(e.aliases) > 1)
        return {
            "total_raw":       n_raw,
            "total_canonical": n_canon,
            "merged_count":    merged,
            "dedup_ratio":     round(1 - n_canon / max(n_raw, 1), 3),
        }

    def to_dict(self) -> dict:
        return {
            "entities": [e.to_dict() for e in self.all_entities()],
            "stats":    self.stats(),
        }


# ─── Blocking index ──────────────────────────────────────────────

def _build_token_index(norm_names: list[str]) -> dict[str, list[int]]:
    """Inverted token index для candidate blocking."""
    index: dict[str, list[int]] = defaultdict(list)
    for idx, name in enumerate(norm_names):
        for tok in set(name.split()):
            if len(tok) >= 3:
                index[tok].append(idx)
    return index


def _get_candidates(i: int, name: str, index: dict[str, list[int]]) -> set[int]:
    candidates: set[int] = set()
    for tok in set(name.split()):
        if len(tok) >= 3:
            candidates.update(index.get(tok, []))
    candidates.discard(i)
    return candidates


# ─── Основная функция ────────────────────────────────────────────

def resolve_contractors(
    items:      list[dict],
    threshold:  float  = ENTITY_THRESHOLD,
    name_field: str    = "contractor",
    inn_field:  str    = "inn",
) -> EntityRegistry:
    """
    Строит EntityRegistry из списка items.

    Шаг 1: Регистрация всех уникальных raw names.
    Шаг 2: Fuzzy clustering с token blocking.
             Сложность: O(n·k) где k — средний размер candidate set.
             Для типичных закупок k << n.
    Шаг 3: Merge похожих сущностей (Union-Find → registry.merge).

    Returns: EntityRegistry готовый к использованию в apply_resolution().
    """
    registry   = EntityRegistry()
    raw_names:  list[str]        = []
    raw_inns:   dict[str, str]   = {}
    seen:       set[str]         = set()

    for item in items:
        raw = str(item.get(name_field) or "").strip()
        inn = str(item.get(inn_field)  or "").strip() or None
        if raw and not _is_junk(raw) and raw not in seen:
            seen.add(raw)
            raw_names.append(raw)
            if inn:
                raw_inns[raw] = inn

    if not raw_names:
        return registry

    # Регистрируем всех
    entity_ids: list[str] = []
    for name in raw_names:
        entity = registry.register(name, raw_inns.get(name))
        entity_ids.append(entity.entity_id)

    # Fuzzy clustering с blocking
    norm_names  = [normalize_contractor(n) for n in raw_names]
    token_index = _build_token_index(norm_names)
    n           = len(raw_names)
    uf          = _UF(n)
    compared:   set[tuple[int, int]] = set()

    for i in range(n):
        if not norm_names[i]:
            continue
        for j in _get_candidates(i, norm_names[i], token_index):
            if j <= i:
                continue
            pair = (i, j)
            if pair in compared:
                continue
            compared.add(pair)
            if uf.find(i) == uf.find(j):
                continue
            if _similarity(raw_names[i], raw_names[j]) >= threshold:
                uf.union(i, j)

    # Определяем primary для каждого кластера (наиболее упоминаемый)
    root_to_primary: dict[int, int] = {}
    mentions = [registry.resolve(raw_names[i]).mention_count for i in range(n)]

    for i in range(n):
        root = uf.find(i)
        if root not in root_to_primary:
            root_to_primary[root] = i
        else:
            cur = root_to_primary[root]
            if (mentions[i] > mentions[cur]
                    or (mentions[i] == mentions[cur]
                        and len(raw_names[i]) > len(raw_names[cur]))):
                root_to_primary[root] = i

    # Merge
    for i in range(n):
        root    = uf.find(i)
        primary = root_to_primary[root]
        if i == primary:
            continue
        pid = entity_ids[primary]
        sid = entity_ids[i]
        if pid != sid and pid in registry._entities and sid in registry._entities:
            registry.merge(pid, sid)

    return registry


def apply_resolution(
    items:      list[dict],
    registry:   EntityRegistry,
    name_field: str = "contractor",
) -> list[dict]:
    """
    Применяет entity resolution к списку items.
    Добавляет/обновляет поля:
      - contractor         → canonical_name
      - contractor_raw     → исходное значение
      - contractor_id      → entity_id (для аналитики)
      - contractor_confidence → уверенность разрешения
    """
    result = []
    for item in items:
        raw    = str(item.get(name_field) or "").strip()
        entity = registry.resolve(raw)
        patched = dict(item)
        patched["contractor_raw"]        = raw
        patched["contractor_id"]         = entity.entity_id
        patched["contractor_confidence"] = entity.confidence
        patched[name_field]              = entity.canonical_name
        result.append(patched)
    return result