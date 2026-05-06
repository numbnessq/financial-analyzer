# backend/pipeline/feedback_store.py
"""
Feedback Loop — хранение вердиктов аналитика и адаптация весов.

Жизненный цикл:
  1. Аналитик ставит verdict: valid / false_positive / confirmed_fraud
  2. FeedbackStore сохраняет запись
  3. При следующем анализе:
     a. score подавляется если item в false_positive cache
     b. context_manager.apply_feedback_adjustment() корректирует веса флагов
        (накопительный эффект: 10+ false_positive на флаг → его вес падает)
  4. Персистентность: JSON-файл в APP_DIR

Структура хранилища:
  {
    "records": [FeedbackRecord, ...],
    "fp_cache": {"canonical_name": FeedbackRecord},  // последний FP вердикт
    "flag_fp_counts": {"flag_name": int},             // сколько раз flag был в FP
    "flag_cf_counts": {"flag_name": int},             // сколько раз flag был в confirmed_fraud
  }
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

VERDICT_VALID             = "valid"
VERDICT_FALSE_POSITIVE    = "false_positive"
VERDICT_CONFIRMED_FRAUD   = "confirmed_fraud"
VALID_VERDICTS            = {VERDICT_VALID, VERDICT_FALSE_POSITIVE, VERDICT_CONFIRMED_FRAUD, None}

# Порог срабатывания авто-коррекции весов
FP_CORRECTION_THRESHOLD = 5    # 5+ FP на флаг → suppress
CF_CORRECTION_THRESHOLD = 3    # 3+ confirmed_fraud → amplify


@dataclass
class FeedbackRecord:
    item_name:         str
    verdict:           str
    flags:             list[str]   = field(default_factory=list)
    score:             int         = 0
    department:        str         = ""
    contractor:        str         = ""
    source_file:       str         = ""
    comment:           str         = ""
    analyst:           str         = ""
    created_at:        str         = ""
    analysis_session:  str         = ""   # job_id

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "FeedbackRecord":
        return cls(**{
            f: d.get(f, default)
            for f, fld in cls.__dataclass_fields__.items()
            for default in [fld.default if fld.default is not fld.default_factory else fld.default_factory()]
        })


class FeedbackStore:
    """
    Thread-safe (однопоточный FastAPI + BackgroundTasks) хранилище фидбека.
    """

    def __init__(self, data_path: Optional[str] = None):
        self._path:             Optional[Path]            = Path(data_path) if data_path else None
        self._records:          list[FeedbackRecord]      = []
        self._fp_cache:         dict[str, FeedbackRecord] = {}   # item_name → latest FP
        self._flag_fp_counts:   dict[str, int]            = defaultdict(int)
        self._flag_cf_counts:   dict[str, int]            = defaultdict(int)

    # ── Persistence ─────────────────────────────────────

    def load(self) -> None:
        if not self._path or not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))

            for rd in raw.get("records", []):
                try:
                    rec = FeedbackRecord(**{
                        k: rd.get(k, v.default if v.default is not v.default_factory
                                  else v.default_factory())
                        for k, v in FeedbackRecord.__dataclass_fields__.items()
                    })
                    self._records.append(rec)
                except Exception:
                    pass

            self._fp_cache = {
                k: FeedbackRecord(**{
                    f: v.get(f, fld.default if fld.default is not fld.default_factory
                             else fld.default_factory())
                    for f, fld in FeedbackRecord.__dataclass_fields__.items()
                })
                for k, v in raw.get("fp_cache", {}).items()
            }
            for k, v in raw.get("flag_fp_counts", {}).items():
                self._flag_fp_counts[k] = int(v)
            for k, v in raw.get("flag_cf_counts", {}).items():
                self._flag_cf_counts[k] = int(v)

            log.info(f"feedback_store: loaded {len(self._records)} records")
        except Exception as e:
            log.error(f"feedback_store: failed to load: {e}")

    def save(self) -> None:
        if not self._path:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "records":       [r.to_dict() for r in self._records],
                "fp_cache":      {k: v.to_dict() for k, v in self._fp_cache.items()},
                "flag_fp_counts":dict(self._flag_fp_counts),
                "flag_cf_counts":dict(self._flag_cf_counts),
                "saved_at":      datetime.utcnow().isoformat(),
            }
            self._path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            log.error(f"feedback_store: save failed: {e}")

    # ── Write ────────────────────────────────────────────

    def record_verdict(
        self,
        item_name:        str,
        verdict:          str,
        flags:            list[str]  = None,
        score:            int        = 0,
        department:       str        = "",
        contractor:       str        = "",
        source_file:      str        = "",
        comment:          str        = "",
        analyst:          str        = "",
        analysis_session: str        = "",
    ) -> FeedbackRecord:
        if verdict not in VALID_VERDICTS:
            raise ValueError(f"Invalid verdict: {verdict!r}. Must be one of {VALID_VERDICTS}")

        rec = FeedbackRecord(
            item_name        = item_name,
            verdict          = verdict,
            flags            = list(flags or []),
            score            = score,
            department       = department,
            contractor       = contractor,
            source_file      = source_file,
            comment          = comment,
            analyst          = analyst,
            created_at       = datetime.utcnow().isoformat(),
            analysis_session = analysis_session,
        )
        self._records.append(rec)

        if verdict == VERDICT_FALSE_POSITIVE:
            self._fp_cache[item_name] = rec
            for f in rec.flags:
                self._flag_fp_counts[f] += 1

        elif verdict == VERDICT_CONFIRMED_FRAUD:
            # Убираем из FP кэша если вдруг был
            self._fp_cache.pop(item_name, None)
            for f in rec.flags:
                self._flag_cf_counts[f] += 1

        elif verdict == VERDICT_VALID:
            self._fp_cache.pop(item_name, None)

        self.save()
        return rec

    # ── Read ─────────────────────────────────────────────

    def is_known_false_positive(self, item_name: str) -> bool:
        return item_name in self._fp_cache

    def get_last_verdict(self, item_name: str) -> Optional[str]:
        for rec in reversed(self._records):
            if rec.item_name == item_name:
                return rec.verdict
        return None

    def get_suppressed_score(self, item_name: str, original_score: int) -> tuple[int, bool]:
        """
        Если item известен как false_positive — подавляем score до max 15.
        Возвращает (new_score, was_suppressed).
        """
        if self.is_known_false_positive(item_name):
            return (min(original_score, 15), True)
        return (original_score, False)

    def get_flag_recommendations(self) -> list[dict]:
        """
        Возвращает рекомендации по изменению весов флагов.
        Основывается на накопленных fp/cf counts.
        """
        recs = []
        all_flags = set(self._flag_fp_counts) | set(self._flag_cf_counts)
        for flag in all_flags:
            fp = self._flag_fp_counts[flag]
            cf = self._flag_cf_counts[flag]
            if fp >= FP_CORRECTION_THRESHOLD:
                recs.append({
                    "flag":        flag,
                    "action":      "suppress",
                    "reason":      f"{fp} false positives",
                    "fp_count":    fp,
                    "cf_count":    cf,
                })
            elif cf >= CF_CORRECTION_THRESHOLD and fp == 0:
                recs.append({
                    "flag":        flag,
                    "action":      "amplify",
                    "reason":      f"{cf} confirmed frauds",
                    "fp_count":    fp,
                    "cf_count":    cf,
                })
        return sorted(recs, key=lambda x: x["fp_count"] + x["cf_count"], reverse=True)

    def apply_to_context(self, context_manager) -> list[dict]:
        """
        Применяет накопленные рекомендации к ContextManager.
        Вызывается при старте или после пачки фидбеков.
        Возвращает список применённых действий.
        """
        applied = []
        for rec in self.get_flag_recommendations():
            context_manager.apply_feedback_adjustment(rec["flag"], rec["action"])
            applied.append(rec)
        if applied:
            context_manager.save()
        return applied

    # ── Query ─────────────────────────────────────────────

    def get_records(
        self,
        verdict:    Optional[str] = None,
        limit:      int           = 200,
        offset:     int           = 0,
    ) -> list[dict]:
        records = self._records
        if verdict:
            records = [r for r in records if r.verdict == verdict]
        return [r.to_dict() for r in reversed(records[offset:offset + limit])]

    def stats(self) -> dict:
        total   = len(self._records)
        by_v: dict[str, int] = defaultdict(int)
        for r in self._records:
            by_v[r.verdict] += 1
        return {
            "total":           total,
            "by_verdict":      dict(by_v),
            "fp_cache_size":   len(self._fp_cache),
            "flag_fp_counts":  dict(self._flag_fp_counts),
            "flag_cf_counts":  dict(self._flag_cf_counts),
            "recommendations": len(self.get_flag_recommendations()),
        }