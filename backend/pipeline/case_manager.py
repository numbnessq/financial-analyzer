# backend/pipeline/case_manager.py
"""
Case Management System — система расследований.

Сущность Case:
  - уникальный id
  - список связанных items (по имени / source_file)
  - статус: new → in_review → confirmed / false_positive / closed
  - приоритет: critical / high / medium / low (из max score items)
  - комментарии с историей
  - полная история изменений статуса
  - назначенный аналитик

Хранение: JSON-файл (cases.json в APP_DIR).
API-контракт для main.py:
  - CaseManager.create_case(items, title, ...)  → Case
  - CaseManager.get_case(case_id)               → Case
  - CaseManager.list_cases(filters)             → list[Case]
  - CaseManager.update_status(id, status, note) → Case
  - CaseManager.add_comment(id, text, analyst)  → Case
  - CaseManager.link_items(id, items)           → Case
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

VALID_STATUSES = {"new", "in_review", "confirmed", "false_positive", "closed"}
VALID_PRIORITIES = {"critical", "high", "medium", "low"}

_STATUS_TRANSITIONS = {
    "new":           {"in_review", "closed"},
    "in_review":     {"confirmed", "false_positive", "closed", "new"},
    "confirmed":     {"closed", "in_review"},
    "false_positive":{"closed", "in_review"},
    "closed":        {"in_review"},
}


# ─── Модели ──────────────────────────────────────────────────────

@dataclass
class CaseComment:
    text:       str
    analyst:    str   = ""
    created_at: str   = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.utcnow().isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "CaseComment":
        return cls(**{k: d.get(k, "") for k in cls.__dataclass_fields__})


@dataclass
class StatusChange:
    from_status: str
    to_status:   str
    analyst:     str   = ""
    note:        str   = ""
    changed_at:  str   = ""

    def __post_init__(self):
        if not self.changed_at:
            self.changed_at = datetime.utcnow().isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "StatusChange":
        return cls(**{k: d.get(k, "") for k in cls.__dataclass_fields__})


@dataclass
class CaseItem:
    """Ссылка на позицию в рамках дела."""
    item_name:   str
    source_file: str   = ""
    department:  str   = ""
    contractor:  str   = ""
    score:       int   = 0
    risk_level:  str   = ""
    flags:       list[str] = field(default_factory=list)
    added_at:    str   = ""

    def __post_init__(self):
        if not self.added_at:
            self.added_at = datetime.utcnow().isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "CaseItem":
        return cls(**{
            k: d.get(k, fld.default if fld.default is not fld.default_factory
                     else fld.default_factory())
            for k, fld in cls.__dataclass_fields__.items()
        })


@dataclass
class Case:
    id:              str
    title:           str
    status:          str                  = "new"
    priority:        str                  = "medium"
    items:           list[CaseItem]       = field(default_factory=list)
    comments:        list[CaseComment]    = field(default_factory=list)
    status_history:  list[StatusChange]   = field(default_factory=list)
    assigned_to:     str                  = ""
    tags:            list[str]            = field(default_factory=list)
    created_at:      str                  = ""
    updated_at:      str                  = ""
    source_files:    list[str]            = field(default_factory=list)
    analysis_session:str                  = ""

    def __post_init__(self):
        now = datetime.utcnow().isoformat()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = now

    @property
    def max_score(self) -> int:
        return max((i.score for i in self.items), default=0)

    @property
    def item_count(self) -> int:
        return len(self.items)

    def to_dict(self) -> dict:
        return {
            "id":               self.id,
            "title":            self.title,
            "status":           self.status,
            "priority":         self.priority,
            "items":            [i.to_dict() for i in self.items],
            "comments":         [c.to_dict() for c in self.comments],
            "status_history":   [s.to_dict() for s in self.status_history],
            "assigned_to":      self.assigned_to,
            "tags":             self.tags,
            "created_at":       self.created_at,
            "updated_at":       self.updated_at,
            "source_files":     self.source_files,
            "analysis_session": self.analysis_session,
            "max_score":        self.max_score,
            "item_count":       self.item_count,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Case":
        case = cls(
            id               = d.get("id", str(uuid.uuid4())),
            title            = d.get("title", ""),
            status           = d.get("status", "new"),
            priority         = d.get("priority", "medium"),
            assigned_to      = d.get("assigned_to", ""),
            tags             = d.get("tags", []),
            created_at       = d.get("created_at", ""),
            updated_at       = d.get("updated_at", ""),
            source_files     = d.get("source_files", []),
            analysis_session = d.get("analysis_session", ""),
        )
        case.items          = [CaseItem.from_dict(i) for i in d.get("items", [])]
        case.comments       = [CaseComment.from_dict(c) for c in d.get("comments", [])]
        case.status_history = [StatusChange.from_dict(s) for s in d.get("status_history", [])]
        return case


# ─── Фильтры ─────────────────────────────────────────────────────

@dataclass
class CaseFilter:
    status:     Optional[str]  = None
    priority:   Optional[str]  = None
    assigned_to:Optional[str]  = None
    tag:        Optional[str]  = None
    min_score:  int            = 0
    limit:      int            = 100
    offset:     int            = 0

    def matches(self, case: Case) -> bool:
        if self.status and case.status != self.status:
            return False
        if self.priority and case.priority != self.priority:
            return False
        if self.assigned_to and case.assigned_to != self.assigned_to:
            return False
        if self.tag and self.tag not in case.tags:
            return False
        if self.min_score > 0 and case.max_score < self.min_score:
            return False
        return True


# ─── CaseManager ────────────────────────────────────────────────

class CaseManager:
    """
    In-memory хранилище дел с персистентностью в JSON.
    """

    def __init__(self, data_path: Optional[str] = None):
        self._path:  Optional[Path]     = Path(data_path) if data_path else None
        self._cases: dict[str, Case]    = {}

    # ── Persistence ─────────────────────────────────────

    def load(self) -> None:
        if not self._path or not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            for d in raw.get("cases", []):
                case = Case.from_dict(d)
                self._cases[case.id] = case
            log.info(f"case_manager: loaded {len(self._cases)} cases")
        except Exception as e:
            log.error(f"case_manager: failed to load: {e}")

    def save(self) -> None:
        if not self._path:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps(
                    {"cases": [c.to_dict() for c in self._cases.values()],
                     "saved_at": datetime.utcnow().isoformat()},
                    ensure_ascii=False, indent=2,
                ),
                encoding="utf-8",
            )
        except Exception as e:
            log.error(f"case_manager: save failed: {e}")

    # ── CRUD ─────────────────────────────────────────────

    def create_case(
        self,
        title:            str,
        items:            list[dict]   = None,
        priority:         str          = "medium",
        assigned_to:      str          = "",
        tags:             list[str]    = None,
        analysis_session: str          = "",
        initial_comment:  str          = "",
        analyst:          str          = "",
    ) -> Case:
        if priority not in VALID_PRIORITIES:
            priority = "medium"

        case_items   = [self._dict_to_case_item(i) for i in (items or [])]
        source_files = list({i.source_file for i in case_items if i.source_file})

        # Авто-приоритет из max_score если не указан явно
        if not items or priority == "medium":
            max_s = max((i.score for i in case_items), default=0)
            if max_s >= 70:
                priority = "critical"
            elif max_s >= 40:
                priority = "high"
            elif max_s >= 20:
                priority = "medium"
            else:
                priority = "low"

        case = Case(
            id               = str(uuid.uuid4()),
            title            = title,
            priority         = priority,
            items            = case_items,
            assigned_to      = assigned_to,
            tags             = list(tags or []),
            analysis_session = analysis_session,
            source_files     = source_files,
        )
        if initial_comment:
            case.comments.append(CaseComment(text=initial_comment, analyst=analyst))

        self._cases[case.id] = case
        self.save()
        log.info(f"case_manager: created case {case.id} [{case.priority}] '{title}'")
        return case

    def get_case(self, case_id: str) -> Optional[Case]:
        return self._cases.get(case_id)

    def list_cases(self, f: Optional[CaseFilter] = None) -> list[Case]:
        cases = list(self._cases.values())
        if f:
            cases = [c for c in cases if f.matches(c)]
            cases = sorted(cases, key=lambda c: c.updated_at, reverse=True)
            cases = cases[f.offset: f.offset + f.limit]
        else:
            cases = sorted(cases, key=lambda c: c.updated_at, reverse=True)
        return cases

    def update_status(
        self,
        case_id:  str,
        status:   str,
        note:     str  = "",
        analyst:  str  = "",
    ) -> Case:
        case = self._require(case_id)
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status: {status!r}")

        allowed = _STATUS_TRANSITIONS.get(case.status, set())
        if status not in allowed:
            raise ValueError(
                f"Transition {case.status!r} → {status!r} not allowed. "
                f"Allowed: {sorted(allowed)}"
            )

        change = StatusChange(
            from_status = case.status,
            to_status   = status,
            analyst     = analyst,
            note        = note,
        )
        case.status_history.append(change)
        case.status     = status
        case.updated_at = datetime.utcnow().isoformat()
        self.save()
        return case

    def add_comment(
        self,
        case_id: str,
        text:    str,
        analyst: str = "",
    ) -> Case:
        case = self._require(case_id)
        if not text.strip():
            raise ValueError("Comment text cannot be empty")
        case.comments.append(CaseComment(text=text.strip(), analyst=analyst))
        case.updated_at = datetime.utcnow().isoformat()
        self.save()
        return case

    def link_items(
        self,
        case_id: str,
        items:   list[dict],
    ) -> Case:
        case = self._require(case_id)
        existing_names = {i.item_name for i in case.items}
        for item_dict in items:
            ci = self._dict_to_case_item(item_dict)
            if ci.item_name not in existing_names:
                case.items.append(ci)
                existing_names.add(ci.item_name)
        case.updated_at = datetime.utcnow().isoformat()
        self.save()
        return case

    def assign(
        self,
        case_id:  str,
        analyst:  str,
        note:     str = "",
    ) -> Case:
        case = self._require(case_id)
        case.assigned_to = analyst
        case.updated_at  = datetime.utcnow().isoformat()
        if note:
            case.comments.append(CaseComment(
                text    = f"Назначено на: {analyst}. {note}",
                analyst = analyst,
            ))
        self.save()
        return case

    def delete_case(self, case_id: str) -> bool:
        if case_id in self._cases:
            del self._cases[case_id]
            self.save()
            return True
        return False

    # ── Auto-create from analysis ────────────────────────

    def auto_create_from_results(
        self,
        results:           list[dict],
        analysis_session:  str  = "",
        min_score:         int  = 40,
        analyst:           str  = "",
    ) -> list[Case]:
        """
        Автоматически создаёт дела из результатов анализа.

        Стратегия группировки:
          1. CRITICAL items (score >= 70) → одно дело на item (высокий риск)
          2. HIGH items (40-69) → группируются по contractor
          3. Дубликаты (уже есть дело с таким item_name) → пропускаются

        Возвращает список созданных дел.
        """
        existing_items = {
            i.item_name
            for case in self._cases.values()
            for i in case.items
            if case.status not in ("closed", "false_positive")
        }

        high_risk  = [r for r in results if r.get("score", 0) >= 70
                      and r.get("name") not in existing_items]
        medium_risk = [r for r in results
                       if 40 <= r.get("score", 0) < 70
                       and r.get("name") not in existing_items]

        created = []

        # CRITICAL → отдельные дела
        for r in high_risk:
            case = self.create_case(
                title            = f"[CRITICAL] {r.get('name', 'Без названия')}",
                items            = [r],
                analysis_session = analysis_session,
                analyst          = analyst,
                initial_comment  = (
                    f"Авто-создано. Score: {r.get('score')}. "
                    f"Флаги: {', '.join(r.get('flags', []))}"
                ),
            )
            created.append(case)

        # HIGH → группировка по контрагенту
        by_contractor: dict[str, list[dict]] = {}
        for r in medium_risk:
            cont = r.get("contractor") or "Без контрагента"
            by_contractor.setdefault(cont, []).append(r)

        for cont, items in by_contractor.items():
            case = self.create_case(
                title            = f"[HIGH] {cont} — {len(items)} позиций",
                items            = items,
                analysis_session = analysis_session,
                analyst          = analyst,
                initial_comment  = (
                    f"Авто-создано по контрагенту '{cont}'. "
                    f"Позиций: {len(items)}."
                ),
            )
            created.append(case)

        return created

    # ── Stats ────────────────────────────────────────────

    def stats(self) -> dict:
        cases = list(self._cases.values())
        by_status:   dict[str, int] = {}
        by_priority: dict[str, int] = {}
        for c in cases:
            by_status[c.status]     = by_status.get(c.status, 0) + 1
            by_priority[c.priority] = by_priority.get(c.priority, 0) + 1
        return {
            "total":       len(cases),
            "by_status":   by_status,
            "by_priority": by_priority,
            "open":        sum(1 for c in cases if c.status in ("new", "in_review")),
        }

    # ── Helpers ──────────────────────────────────────────

    def _require(self, case_id: str) -> Case:
        case = self._cases.get(case_id)
        if not case:
            raise KeyError(f"Case {case_id!r} not found")
        return case

    @staticmethod
    def _dict_to_case_item(d: dict) -> CaseItem:
        return CaseItem(
            item_name   = str(d.get("name") or d.get("item") or d.get("item_name") or ""),
            source_file = str(d.get("source_file") or ""),
            department  = str(d.get("department") or ""),
            contractor  = str(d.get("contractor") or ""),
            score       = int(d.get("score") or 0),
            risk_level  = str(d.get("risk_level") or ""),
            flags       = list(d.get("flags") or []),
        )