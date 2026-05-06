# backend/pipeline/explainer.py
"""
Объяснение флагов для аналитика.

Принципы:
  - Только факты и числа из данных
  - Нейтральные формулировки (без обвинений)
  - Структура: ФАКТЫ / ОТКЛОНЕНИЕ / ИНТЕРПРЕТАЦИЯ
  - IQR-флаги объясняются с полным контекстом (Q1, Q3, IQR, fences)

v2: исправлены IQR-флаги, согласованы пороги с scorer.py,
    добавлен контекст entity_id / contractor_status.
"""

import statistics
from backend.pipeline.scorer import (
    _get_name, _get_department, _get_contractor, _get_source_file,
    _get_unit_price, _get_total_price, _get_unit, _to_float, _get_date,
    _unique_departments, _unique_contractors, _group_price_stats,
    _effective_price, _deviation_pct,
)


# ─── Пороги: должны совпадать со scorer.py ───────────────────────
_ROUND_DIVISORS = (1_000_000, 500_000, 100_000)


def _is_round(value: float) -> bool:
    if value <= 0:
        return False
    for d in _ROUND_DIVISORS:
        if value >= d and value % d == 0:
            return True
    return False


def _fmt(v: float) -> str:
    """Форматирование числа: 1 234 567,00 руб."""
    try:
        return f"{v:,.0f} руб.".replace(",", "\u202f")
    except (TypeError, ValueError):
        return "—"


def _fmt_pct(v: float) -> str:
    return f"{v:.1f}%"


# ─── IQR helper ──────────────────────────────────────────────────

def _get_iqr_context(item: dict, group: dict) -> dict | None:
    """
    Получает IQR-статистику группы и позицию item в ней.
    Используется для объяснения iqr_strong_outlier / iqr_moderate_outlier.
    """
    try:
        from backend.pipeline.price_analyzer import (
            compute_group_stats, classify_price, _eff_price,
        )
        items  = group.get("items", [])
        prices = [_eff_price(i) for i in items if _eff_price(i) > 0]
        if len(prices) < 2:
            return None
        stats    = compute_group_stats(prices)
        price    = _eff_price(item)
        if price <= 0 or not stats:
            return None
        cls_info = classify_price(price, prices)
        return {"stats": stats, "cls": cls_info, "price": price}
    except Exception:
        return None


# ─── Объяснение каждого флага ────────────────────────────────────

def _explain_flag(flag: str, item: dict, group: dict) -> dict:
    """
    Возвращает dict: facts / deviation / interpretation.
    Все формулировки нейтральны.
    """
    items       = group.get("items", [])
    departments = _unique_departments(group)
    contractors = _unique_contractors(group)
    unit_price  = _get_unit_price(item)
    eff_price   = _effective_price(item, group)
    total_price = _get_total_price(item)
    stats       = _group_price_stats(group)
    unit        = _get_unit(item)

    # ── Структурные ──────────────────────────────────────────────

    if flag == "duplicate_3_plus":
        depts_str = ", ".join(departments[:5])
        suffix    = f" и ещё {len(departments) - 5}" if len(departments) > 5 else ""
        return {
            "facts":          f"Позиция закупается в {len(departments)} подразделениях: {depts_str}{suffix}",
            "deviation":      f"Закупается в {len(departments)} подразделениях (норма: 1–2)",
            "interpretation": "Возможные причины: раздельное планирование без координации, централизованная закупка не производилась, либо одна закупка отражена в нескольких документах",
        }

    if flag == "duplicate_2":
        return {
            "facts":          f"Позиция присутствует в 2 подразделениях: {', '.join(departments)}",
            "deviation":      "Закупается в 2 подразделениях",
            "interpretation": "Возможные причины: смежные подразделения закупают самостоятельно, либо документ скопирован",
        }

    if flag == "single_occurrence":
        return {
            "facts":          "Позиция встречается только в одном документе",
            "deviation":      "Нет базы сравнения — 1 наблюдение",
            "interpretation": "Разовая закупка или нестандартная номенклатура",
        }

    if flag == "split_suspected":
        source      = _get_source_file(item)
        same_source = [i for i in items if _get_source_file(i) == source]
        return {
            "facts":          f"В документе «{source}» позиция встречается {len(same_source)} раз",
            "deviation":      f"Повторений в одном источнике: {len(same_source)} (норма: 1)",
            "interpretation": "Возможные причины: позиция разбита на этапы или партии, дублирование строк при вводе данных",
        }

    # ── IQR-флаги (приоритетный обработчик) ──────────────────────

    if flag in ("iqr_strong_outlier", "iqr_moderate_outlier"):
        ctx = _get_iqr_context(item, group)
        if ctx:
            s     = ctx["stats"]
            cls   = ctx["cls"]
            price = ctx["price"]
            label = "Цена единицы" if unit_price > 0 else "Расчётная цена (итог ÷ кол-во)"
            direction = "превышает" if price > s["median"] else "ниже"
            fence_name = "жёсткого" if flag == "iqr_strong_outlier" else "мягкого"
            fence_val  = s["fence_hard_hi"] if price > s["median"] else s["fence_hard_lo"]
            if flag == "iqr_moderate_outlier":
                fence_val = s["fence_soft_hi"] if price > s["median"] else s["fence_soft_lo"]

            return {
                "facts": (
                    f"{label}: {_fmt(price)} | "
                    f"Медиана группы: {_fmt(s['median'])} | "
                    f"Q1={_fmt(s['q1'])} Q3={_fmt(s['q3'])} IQR={_fmt(s['iqr'])} | "
                    f"Наблюдений: {s['n']}"
                ),
                "deviation": (
                    f"Цена {direction} медиану на {_fmt_pct(cls['pct_deviation'])} "
                    f"(z-score={cls['z_score']:.1f}), "
                    f"выходит за {fence_name} fence ({_fmt(fence_val)})"
                ),
                "interpretation": (
                    "Возможные причины: иные условия поставки, другой объём партии, "
                    "обновление прайса, ошибка при вводе данных, "
                    "либо нестандартная спецификация товара"
                ),
            }
        # Fallback если IQR контекст недоступен
        return {
            "facts":          f"Цена: {_fmt(eff_price)}",
            "deviation":      "Выход за границы IQR — статистическое отклонение",
            "interpretation": "Проверить соответствие спецификации и условий поставки",
        }

    # ── Legacy ценовые отклонения ────────────────────────────────

    if flag in ("price_deviation_100", "price_deviation_50", "price_deviation_20"):
        if stats and stats["n"] >= 2:
            label = "Цена единицы" if unit_price > 0 else "Расчётная цена (итог ÷ кол-во)"
            dev   = _deviation_pct(eff_price, stats["median"])
            threshold_map = {
                "price_deviation_100": ">100%",
                "price_deviation_50":  ">50%",
                "price_deviation_20":  ">20%",
            }
            return {
                "facts": (
                    f"{label}: {_fmt(eff_price)} | "
                    f"Медиана по группе: {_fmt(stats['median'])} | "
                    f"Диапазон: {_fmt(stats['min'])}–{_fmt(stats['max'])} | "
                    f"Наблюдений: {stats['n']}"
                ),
                "deviation":      f"Отклонение от медианы: {_fmt_pct(dev)} ({threshold_map[flag]})",
                "interpretation": "Возможные причины: разные условия поставки, другой объём партии, ошибка при вводе данных, обновление прайса",
            }
        return {
            "facts":          f"Цена: {_fmt(eff_price)} (недостаточно данных для сравнения)",
            "deviation":      "Менее 2 наблюдений в группе",
            "interpretation": "Проверить вручную — нет базы для автоматического сравнения",
        }

    if flag in ("total_price_deviation_40", "total_price_deviation_15"):
        totals = [
            _to_float(i.get("total_price") or 0)
            for i in items
            if _to_float(i.get("total_price") or 0) > 0
        ]
        if totals:
            med = statistics.median(totals)
            dev = _deviation_pct(total_price, med)
            threshold_label = ">40%" if flag == "total_price_deviation_40" else ">15%"
            return {
                "facts": (
                    f"Итоговая сумма: {_fmt(total_price)} | "
                    f"Медиана по группе: {_fmt(med)} | "
                    f"Диапазон: {_fmt(min(totals))}–{_fmt(max(totals))} | "
                    f"Наблюдений: {len(totals)}"
                ),
                "deviation":      f"Отклонение итоговой суммы от медианы: {_fmt_pct(dev)} ({threshold_label})",
                "interpretation": "Возможные причины: разный объём работ, разные периоды, ошибка в расчёте",
            }

    # ── Целостность данных ───────────────────────────────────────

    if flag == "total_mismatch":
        qty      = _to_float(item.get("quantity", 0))
        expected = round(unit_price * qty, 2) if unit_price > 0 and qty > 0 else 0
        diff     = abs(expected - total_price)
        diff_pct = _deviation_pct(total_price, expected) if expected > 0 else 0
        return {
            "facts": (
                f"Цена ед.: {_fmt(unit_price)} × Кол-во: {qty:g} {unit} = "
                f"{_fmt(expected)} | Указанная сумма: {_fmt(total_price)}"
            ),
            "deviation":      f"Расхождение: {_fmt(diff)} ({_fmt_pct(diff_pct)})",
            "interpretation": "Возможные причины: скидка не отражена в цене, округление, ошибка при заполнении",
        }

    if flag == "volume_without_price":
        qty = _to_float(item.get("quantity", 0))
        return {
            "facts":          f"Количество: {qty:g} {unit} | Цена единицы: не указана",
            "deviation":      "Объём есть, цена единицы отсутствует",
            "interpretation": "Возможные причины: паушальный договор, незаполненная форма документа",
        }

    if flag == "price_without_volume":
        return {
            "facts":          f"Цена единицы: {_fmt(unit_price)} | Количество: не указано",
            "deviation":      "Цена есть, количество отсутствует",
            "interpretation": "Возможные причины: незаполненная форма, позиция-заголовок",
        }

    if flag == "zero_quantity":
        return {
            "facts":          f"Количество = 0 | Цена единицы: {_fmt(unit_price)}",
            "deviation":      "Нулевое количество при ненулевой цене",
            "interpretation": "Возможные причины: ошибка ввода, позиция аннулирована но не удалена",
        }

    if flag == "unit_mismatch":
        units = sorted({_get_unit(i) for i in items if _get_unit(i)})
        return {
            "facts":          f"Единицы измерения в документах: {', '.join(units)}",
            "deviation":      f"{len(units)} различных единиц (ожидается 1)",
            "interpretation": "Возможные причины: разные стандарты оформления, ошибка при вводе",
        }

    # ── Количественные отклонения ────────────────────────────────

    if flag in ("quantity_deviation_50", "quantity_deviation_20"):
        qtys = sorted([
            _to_float(i.get("quantity", 0))
            for i in items
            if _to_float(i.get("quantity", 0)) > 0
        ])
        if qtys and min(qtys) > 0:
            spread = (max(qtys) - min(qtys)) / min(qtys) * 100
            threshold_label = ">30%" if flag == "quantity_deviation_50" else ">10%"
            return {
                "facts": (
                    f"Объёмы по документам: {' / '.join(f'{q:g}' for q in qtys[:6])} {unit}"
                ),
                "deviation":      f"Разброс объёмов: {_fmt_pct(spread)} ({threshold_label})",
                "interpretation": "Возможные причины: разные периоды, частичное исполнение, ошибка в документе",
            }

    # ── Контрагент ───────────────────────────────────────────────

    if flag == "contractor_concentration":
        c = contractors[0] if contractors else "—"
        return {
            "facts":          f"Все {len(items)} вхождений группы — поставщик: «{c}»",
            "deviation":      "Единственный поставщик для всей группы документов",
            "interpretation": "Возможные причины: долгосрочный договор, ограниченный рынок, отсутствие альтернатив",
        }

    if flag == "contractor_blacklist":
        cont   = _get_contractor(item)
        status = item.get("contractor_status", "blacklisted")
        return {
            "facts":          f"Контрагент: «{cont}» | Статус: {status}",
            "deviation":      "Контрагент включён в список для проверки",
            "interpretation": "Требует ручной проверки документов по данному контрагенту",
        }

    # ── Прочие ───────────────────────────────────────────────────

    if flag == "vague_item":
        return {
            "facts":          f"Наименование: «{_get_name(item)}»",
            "deviation":      "Название не содержит уточняющих характеристик",
            "interpretation": "Возможные причины: агрегированная строка, неполное заполнение формы. Рекомендуется детализация",
        }

    if flag == "round_number":
        val = total_price if _is_round(total_price) else unit_price
        return {
            "facts":          f"Сумма: {_fmt(val)}",
            "deviation":      f"Сумма кратна {_fmt(min(d for d in _ROUND_DIVISORS if val % d == 0 and val >= d))}",
            "interpretation": "Возможные причины: плановая оценка, округление при согласовании",
        }

    if flag == "temporal_clustering":
        dates = sorted({_get_date(i) for i in items if _get_date(i)})
        return {
            "facts":          f"Даты закупок: {', '.join(dates[:6])}{'…' if len(dates) > 6 else ''}",
            "deviation":      "Несколько закупок с интервалом ≤3 дней",
            "interpretation": "Возможные причины: срочная закупка, закрытие периода планирования",
        }

    if flag == "graph_central":
        return {
            "facts":          f"Позиция «{_get_name(item)}» — высокая степень связности в сети закупок",
            "deviation":      "Централизованность выше порогового значения (0.1)",
            "interpretation": "Позиция связана со многими поставщиками/подразделениями — рекомендуется детальный просмотр всей цепочки",
        }

    # ── Default ──────────────────────────────────────────────────
    return {
        "facts":          f"Флаг: {flag}",
        "deviation":      "Требует ручной проверки",
        "interpretation": "Автоматическая классификация недоступна",
    }


# ─── Построение объяснений ───────────────────────────────────────

# Порядок приоритетности флагов для краткого резюме
_PRIORITY_ORDER = [
    "iqr_strong_outlier", "price_deviation_100", "contractor_blacklist",
    "iqr_moderate_outlier", "price_deviation_50", "duplicate_3_plus",
    "split_suspected", "total_mismatch", "price_deviation_20",
    "total_price_deviation_40", "vague_item", "zero_quantity",
    "volume_without_price", "price_without_volume",
    "duplicate_2", "contractor_concentration", "unit_mismatch",
    "quantity_deviation_50", "temporal_clustering", "graph_central",
    "total_price_deviation_15", "quantity_deviation_20",
    "round_number", "single_occurrence",
]


def _sort_flags_by_priority(flags: list) -> list:
    priority = {f: i for i, f in enumerate(_PRIORITY_ORDER)}
    return sorted(flags, key=lambda f: priority.get(f, 999))


def build_explanation(flags: list, item: dict, group: dict) -> str:
    """
    Краткое объяснение для таблицы результатов (одна строка).
    Показываем deviation — самое информативное поле.
    Максимум 4 части.
    """
    if not flags:
        return "Отклонений не выявлено"

    parts = []
    for flag in _sort_flags_by_priority(flags):
        explained = _explain_flag(flag, item, group)
        dev = explained.get("deviation", "").strip()
        if dev and dev not in ("—", "Требует ручной проверки"):
            parts.append(dev)
        elif explained.get("facts", "").strip():
            facts = explained["facts"]
            # Берём только первый сегмент до "|"
            short = facts.split("|")[0].strip()
            if short:
                parts.append(short)
        if len(parts) >= 4:
            break

    return " | ".join(parts) if parts else "Требует проверки"


def build_full_explanation(flags: list, item: dict, group: dict) -> dict:
    """
    Полное структурированное объяснение для отчёта.
    """
    if not flags:
        return {
            "summary":         "Отклонений не выявлено",
            "flags_explained": [],
        }

    flags_sorted    = _sort_flags_by_priority(flags)
    flags_explained = []

    for flag in flags_sorted:
        explained = _explain_flag(flag, item, group)
        flags_explained.append({
            "flag":           flag,
            "facts":          explained.get("facts", ""),
            "deviation":      explained.get("deviation", ""),
            "interpretation": explained.get("interpretation", ""),
        })

    # Краткое резюме: топ-3 отклонения
    devs    = [f["deviation"] for f in flags_explained
               if f["deviation"] and f["deviation"] not in ("—", "Требует ручной проверки")]
    summary = " | ".join(devs[:3]) if devs else "Требует проверки"

    return {
        "summary":         summary,
        "flags_explained": flags_explained,
    }


# ─── Публичный API ────────────────────────────────────────────────

def explain_result(scored: dict, group: dict) -> dict:
    """Добавляет объяснение к результату scorer."""
    flags = scored.get("flags", [])
    full  = build_full_explanation(flags, scored, group)
    return {
        **scored,
        "explanation":      build_explanation(flags, scored, group),
        "full_explanation": full,
    }