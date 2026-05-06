# backend/pipeline/context_manager.py
"""
Business Context Layer.

Предоставляет контекст для scorer и analyzer:
  - справочник контрагентов (whitelist / blacklist / trusted)
  - справочник категорий закупок
  - исторические цены по категориям
  - рыночные цены (заглушка / внешний источник)
  - динамические веса флагов по категории/департаменту

Хранение:
  - в памяти (актуальное состояние)
  - персистентность через JSON-файл (context_data.json в APP_DIR)

Интеграция:
  main.py загружает/создаёт ContextManager при старте.
  scorer.py получает context через get_context_for_item().
  analyzer.py передаёт context в score_item().
"""

from __future__ import annotations

import json
import logging
import os
import statistics
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)

_SENTINEL = object()

# ─── Defaults ────────────────────────────────────────────────────

DEFAULT_FLAG_WEIGHTS: dict[str, int] = {
    "duplicate_3_plus":          25,
    "duplicate_2":               10,
    "single_occurrence":          5,
    "split_suspected":           15,
    "iqr_strong_outlier":        35,
    "iqr_moderate_outlier":      18,
    "price_deviation_100":       30,
    "price_deviation_50":        20,
    "price_deviation_20":        10,
    "total_price_deviation_40":  20,
    "total_price_deviation_15":   8,
    "total_mismatch":            20,
    "volume_without_price":      15,
    "price_without_volume":      10,
    "zero_quantity":             15,
    "unit_mismatch":             10,
    "contractor_concentration":   8,
    "contractor_blacklist":       35,
    "vague_item":                10,
    "round_number":               5,
    "quantity_deviation_50":     15,
    "quantity_deviation_20":      8,
    "temporal_clustering":       10,
    "graph_central":              8,
}

# Мультипликаторы по категориям —
# высокорисковые категории получают повышенные веса
CATEGORY_MULTIPLIERS: dict[str, float] = {
    "строительство":     1.3,
    "ит":                1.1,
    "консалтинг":        1.2,
    "медикаменты":       1.2,
    "оборудование":      1.0,
    "расходные материалы": 0.9,
    "услуги":            1.15,
    "default":           1.0,
}

# Мультипликаторы по департаментам — пример
DEPARTMENT_MULTIPLIERS: dict[str, float] = {
    "default": 1.0,
}


# ─── Модели данных ────────────────────────────────────────────────

@dataclass
class ContractorRecord:
    name:          str
    normalized:    str            = ""
    inn:           Optional[str]  = None
    status:        str            = "unknown"   # trusted / neutral / suspicious / blacklisted
    tags:          list[str]      = field(default_factory=list)
    added_at:      str            = ""
    note:          str            = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CategoryRecord:
    name:                str
    keywords:            list[str]   = field(default_factory=list)
    risk_multiplier:     float       = 1.0
    typical_price_range: dict        = field(default_factory=dict)  # {unit: [min, max]}
    note:                str         = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MarketPrice:
    item_name:   str
    category:    str
    unit:        str
    price_min:   float
    price_max:   float
    price_ref:   float
    source:      str     = "manual"   # manual / external_api / parsed
    updated_at:  str     = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class HistoricalPrice:
    item_normalized: str
    category:        str
    prices:          list[float]       = field(default_factory=list)
    dates:           list[str]         = field(default_factory=list)
    contractors:     list[str]         = field(default_factory=list)

    @property
    def median(self) -> Optional[float]:
        return round(statistics.median(self.prices), 2) if self.prices else None

    @property
    def mean(self) -> Optional[float]:
        return round(statistics.mean(self.prices), 2) if self.prices else None

    @property
    def stdev(self) -> Optional[float]:
        return round(statistics.stdev(self.prices), 2) if len(self.prices) > 1 else None

    def summary(self) -> dict:
        return {
            "item":   self.item_normalized,
            "n":      len(self.prices),
            "median": self.median,
            "mean":   self.mean,
            "stdev":  self.stdev,
            "min":    round(min(self.prices), 2) if self.prices else None,
            "max":    round(max(self.prices), 2) if self.prices else None,
        }

    def to_dict(self) -> dict:
        return {
            "item_normalized": self.item_normalized,
            "category":        self.category,
            "prices":          self.prices,
            "dates":           self.dates,
            "contractors":     self.contractors,
        }


# ─── Context for scoring ─────────────────────────────────────────

@dataclass
class ItemContext:
    """Контекст одного item для scorer."""
    category:              str            = "default"
    flag_weight_overrides: dict[str, int] = field(default_factory=dict)
    weight_multiplier:     float          = 1.0
    blacklisted:           bool           = False
    contractor_status:     str            = "unknown"
    market_ref_price:      Optional[float]= None
    historical_ref_price:  Optional[float]= None
    historical_n:          int            = 0
    note:                  str            = ""

    def effective_weight(self, flag: str, base_weight: int) -> int:
        override = self.flag_weight_overrides.get(flag)
        if override is not None:
            return override
        return round(base_weight * self.weight_multiplier)


# ─── ContextManager ──────────────────────────────────────────────

class ContextManager:
    """
    Единый реестр контекстных данных.

    Жизненный цикл:
      1. ctx = ContextManager(data_path)   → загружает JSON если есть
      2. ctx.load() при старте приложения
      3. Во время анализа: ctx.get_context_for_item(item, group)
      4. ctx.save() при изменениях
    """

    def __init__(self, data_path: Optional[str] = None):
        self._path: Optional[Path] = Path(data_path) if data_path else None

        self._contractors:  dict[str, ContractorRecord] = {}
        self._categories:   dict[str, CategoryRecord]   = {}
        self._market:       dict[str, MarketPrice]       = {}
        self._historical:   dict[str, HistoricalPrice]  = {}

        self._flag_weights: dict[str, int]   = dict(DEFAULT_FLAG_WEIGHTS)
        self._feedback_adjustments: dict[str, float] = {}  # flag → weight multiplier

    # ── Persistence ─────────────────────────────────────

    def load(self) -> None:
        if not self._path or not self._path.exists():
            log.info("context_manager: no data file, starting fresh")
            self._seed_defaults()
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            self._load_from_dict(raw)
            log.info(
                f"context_manager: loaded {len(self._contractors)} contractors, "
                f"{len(self._categories)} categories, "
                f"{len(self._historical)} historical price series"
            )
        except Exception as e:
            log.error(f"context_manager: failed to load {self._path}: {e}")
            self._seed_defaults()

    def save(self) -> None:
        if not self._path:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps(self._to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            log.error(f"context_manager: failed to save: {e}")

    def _to_dict(self) -> dict:
        return {
            "contractors":         {k: v.to_dict() for k, v in self._contractors.items()},
            "categories":          {k: v.to_dict() for k, v in self._categories.items()},
            "market_prices":       {k: v.to_dict() for k, v in self._market.items()},
            "historical_prices":   {k: v.to_dict() for k, v in self._historical.items()},
            "flag_weights":        self._flag_weights,
            "feedback_adjustments":self._feedback_adjustments,
            "saved_at":            datetime.utcnow().isoformat(),
        }

    def _load_from_dict(self, raw: dict) -> None:
        for k, v in raw.get("contractors", {}).items():
            self._contractors[k] = ContractorRecord(**{
                f: v.get(f, d)
                for f, d in ContractorRecord.__dataclass_fields__.items()
            })
        for k, v in raw.get("categories", {}).items():
            self._categories[k] = CategoryRecord(**{
                f: v.get(f, d)
                for f, d in CategoryRecord.__dataclass_fields__.items()
            })
        for k, v in raw.get("market_prices", {}).items():
            self._market[k] = MarketPrice(**{
                f: v.get(f, d)
                for f, d in MarketPrice.__dataclass_fields__.items()
            })
        for k, v in raw.get("historical_prices", {}).items():
            self._historical[k] = HistoricalPrice(**{
                f: v.get(f, d)
                for f, d in HistoricalPrice.__dataclass_fields__.items()
            })
        if "flag_weights" in raw:
            self._flag_weights.update(raw["flag_weights"])
        if "feedback_adjustments" in raw:
            self._feedback_adjustments.update(raw["feedback_adjustments"])

    def _seed_defaults(self) -> None:
        """Заполняет стандартный набор категорий."""
        defaults = [
            CategoryRecord("строительство",    ["строит", "ремонт", "монтаж", "кс-2", "кс-6"],   1.3),
            CategoryRecord("ит",               ["программ", "лицензи", "по ", "сервер", "компьют"], 1.1),
            CategoryRecord("консалтинг",        ["консульт", "аудит", "экспертиз", "оценк"],       1.2),
            CategoryRecord("медикаменты",       ["медик", "лекарств", "фармацевт", "препарат"],     1.2),
            CategoryRecord("оборудование",      ["оборудован", "станок", "агрегат", "установк"],    1.0),
            CategoryRecord("услуги",            ["услуг", "обслужив", "сопровожден"],               1.15),
            CategoryRecord("расходные материалы",["расходн", "канцелярия", "бумага", "картридж"],  0.9),
        ]
        for cat in defaults:
            self._categories[cat.name] = cat

    # ── Contractors ─────────────────────────────────────

    def add_contractor(
        self,
        name:       str,
        status:     str            = "neutral",
        inn:        Optional[str]  = None,
        tags:       list[str]      = None,
        note:       str            = "",
    ) -> ContractorRecord:
        from backend.pipeline.entity_resolver import normalize_contractor
        norm = normalize_contractor(name)
        rec  = ContractorRecord(
            name       = name,
            normalized = norm,
            inn        = inn,
            status     = status,
            tags       = tags or [],
            added_at   = datetime.utcnow().isoformat(),
            note       = note,
        )
        key = inn or norm or name
        self._contractors[key] = rec
        return rec

    def get_contractor_status(self, name: str) -> str:
        if not name:
            return "unknown"
        from backend.pipeline.entity_resolver import normalize_contractor
        norm = normalize_contractor(name)
        # Поиск по INN-ключу или нормализованному имени
        for key, rec in self._contractors.items():
            if key == norm or rec.normalized == norm or rec.name == name:
                return rec.status
        return "unknown"

    def is_blacklisted(self, name: str) -> bool:
        return self.get_contractor_status(name) == "blacklisted"

    def get_blacklist(self) -> set[str]:
        return {
            rec.name
            for rec in self._contractors.values()
            if rec.status == "blacklisted"
        }

    # ── Categories ──────────────────────────────────────

    def detect_category(self, item_name: str) -> str:
        """Определяет категорию по ключевым словам в названии позиции."""
        if not item_name:
            return "default"
        name_lower = item_name.lower()
        for cat_name, cat in self._categories.items():
            for kw in cat.keywords:
                if kw in name_lower:
                    return cat_name
        return "default"

    def get_category_multiplier(self, category: str) -> float:
        if category in self._categories:
            return self._categories[category].risk_multiplier
        return CATEGORY_MULTIPLIERS.get(category, 1.0)

    # ── Historical prices ────────────────────────────────

    def record_price(
        self,
        item_normalized: str,
        price:           float,
        category:        str   = "default",
        date:            str   = "",
        contractor:      str   = "",
    ) -> None:
        """Записывает наблюдаемую цену в исторический ряд."""
        if price <= 0:
            return
        key = item_normalized.lower().strip()
        if key not in self._historical:
            self._historical[key] = HistoricalPrice(
                item_normalized = key,
                category        = category,
            )
        h = self._historical[key]
        h.prices.append(price)
        if date:
            h.dates.append(date)
        if contractor:
            h.contractors.append(contractor)

    def get_historical_ref(self, item_normalized: str) -> Optional[dict]:
        key = item_normalized.lower().strip()
        h   = self._historical.get(key)
        if not h or not h.prices:
            return None
        return h.summary()

    def ingest_analysis_results(self, results: list[dict]) -> None:
        """
        Обновляет исторические цены из результатов анализа.
        Вызывается после каждого успешного analyze.
        """
        for r in results:
            name     = str(r.get("name") or r.get("item") or "").strip().lower()
            cat      = self.detect_category(name)
            items    = r.get("items", [])
            for item in items:
                try:
                    price = float(str(
                        item.get("unit_price") or item.get("price") or 0
                    ).replace(",", ".").replace(" ", "") or 0)
                    self.record_price(
                        item_normalized = name,
                        price           = price,
                        category        = cat,
                        date            = str(item.get("date") or ""),
                        contractor      = str(item.get("contractor") or ""),
                    )
                except Exception:
                    pass

    # ── Market prices (stub) ────────────────────────────

    def get_market_price(self, item_name: str, unit: str = "") -> Optional[MarketPrice]:
        """
        Заглушка для рыночных цен.
        В реальной интеграции здесь вызов внешнего API или БД.
        """
        key = item_name.lower().strip()
        return self._market.get(key)

    def set_market_price(
        self,
        item_name:  str,
        price_ref:  float,
        price_min:  float = 0.0,
        price_max:  float = 0.0,
        unit:       str   = "",
        category:   str   = "default",
        source:     str   = "manual",
    ) -> MarketPrice:
        mp = MarketPrice(
            item_name  = item_name,
            category   = category,
            unit       = unit,
            price_min  = price_min,
            price_max  = price_max,
            price_ref  = price_ref,
            source     = source,
            updated_at = datetime.utcnow().isoformat(),
        )
        self._market[item_name.lower().strip()] = mp
        return mp

    # ── Flag weight management ───────────────────────────

    def get_effective_weights(
        self,
        category:   str,
        department: str = "",
    ) -> dict[str, int]:
        """
        Возвращает веса флагов с учётом:
          1. Базовых весов
          2. Обратной связи (feedback_adjustments)
          3. Категорийного мультипликатора
        """
        cat_mult  = self.get_category_multiplier(category)
        dept_mult = DEPARTMENT_MULTIPLIERS.get(department, 1.0)
        combined  = cat_mult * dept_mult

        weights = {}
        for flag, base in self._flag_weights.items():
            fb_mult = self._feedback_adjustments.get(flag, 1.0)
            weights[flag] = min(100, round(base * combined * fb_mult))

        return weights

    def apply_feedback_adjustment(self, flag: str, direction: str) -> None:
        """
        direction: 'suppress' (ложные сигналы) / 'amplify' (пропущенные).
        Изменяет мультипликатор на ±10% с ограничением [0.3, 2.0].
        """
        current = self._feedback_adjustments.get(flag, 1.0)
        if direction == "suppress":
            new_val = max(0.3, current * 0.9)
        elif direction == "amplify":
            new_val = min(2.0, current * 1.1)
        else:
            return
        self._feedback_adjustments[flag] = round(new_val, 4)

    def update_flag_weight(self, flag: str, weight: int) -> None:
        """Прямое обновление веса флага (через API)."""
        if flag in self._flag_weights:
            self._flag_weights[flag] = max(0, min(100, weight))

    # ── Main: context for a single item ─────────────────

    def get_context_for_item(
        self,
        item:  dict,
        group: dict,
    ) -> ItemContext:
        """
        Главная функция для scorer.
        Собирает ItemContext: категория, веса, blacklist, рыночная цена.
        """
        name       = str(item.get("name") or item.get("canonical_name") or "").strip()
        contractor = str(item.get("contractor") or "").strip()
        department = str(item.get("department") or "").strip()

        category    = self.detect_category(name)
        blacklisted = self.is_blacklisted(contractor)
        cont_status = self.get_contractor_status(contractor)

        # Веса с учётом категории, департамента, feedback
        eff_weights = self.get_effective_weights(category, department)

        # Рыночная цена (заглушка)
        mp = self.get_market_price(name)
        market_ref = mp.price_ref if mp else None

        # Историческая цена
        hist    = self.get_historical_ref(name.lower())
        hist_p  = hist["median"] if hist else None
        hist_n  = hist["n"] if hist else 0

        return ItemContext(
            category              = category,
            flag_weight_overrides = eff_weights,
            weight_multiplier     = self.get_category_multiplier(category),
            blacklisted           = blacklisted,
            contractor_status     = cont_status,
            market_ref_price      = market_ref,
            historical_ref_price  = hist_p,
            historical_n          = hist_n,
        )

    # ── Stats & export ───────────────────────────────────

    def summary(self) -> dict:
        return {
            "contractors":      len(self._contractors),
            "blacklisted":      sum(1 for c in self._contractors.values() if c.status == "blacklisted"),
            "categories":       len(self._categories),
            "market_prices":    len(self._market),
            "historical_series":len(self._historical),
            "flag_adjustments": {k: v for k, v in self._feedback_adjustments.items() if v != 1.0},
        }