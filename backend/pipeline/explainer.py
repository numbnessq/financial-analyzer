# backend/pipeline/explainer.py
"""
Формат вывода для каждого флага:

ФАКТЫ:
  - конкретные числа из данных

ОТКЛОНЕНИЕ:
  - отклонение в % с базой (медиана, N наблюдений)

ИНТЕРПРЕТАЦИЯ:
  - возможные причины (без обвинений)
"""

import statistics
from backend.pipeline.scorer import (
    _get_name, _get_department, _get_contractor, _get_source_file,
    _get_unit_price, _get_total_price, _get_unit, _to_float, _get_date,
    _unique_departments, _unique_contractors, _group_price_stats,
    _effective_price, _deviation_pct,
)


# ─── Тексты для каждого флага ─────────────────────────────────────

def _explain_flag(flag: str, item: dict, group: dict) -> dict:
    """
    Возвращает dict с ключами: facts, deviation, interpretation.
    Все формулировки нейтральны — только факты и возможные причины.
    """
    items       = group.get("items", [])
    departments = _unique_departments(group)
    contractors = _unique_contractors(group)
    unit_price  = _get_unit_price(item)
    eff_price   = _effective_price(item, group)
    total_price = _get_total_price(item)
    stats       = _group_price_stats(group)

    if flag == "duplicate_3_plus":
        return {
            "facts":          f"Позиция присутствует в {len(departments)} подразделениях: {', '.join(departments[:5])}",
            "deviation":      f"Количество источников: {len(departments)} (норма: 1–2)",
            "interpretation": "Возможные причины: раздельное планирование, централизованная закупка не произведена, либо одна закупка отражена в нескольких документах",
        }

    if flag == "duplicate_2":
        return {
            "facts":          f"Позиция присутствует в 2 подразделениях: {', '.join(departments)}",
            "deviation":      "Количество источников: 2",
            "interpretation": "Возможные причины: смежные подразделения закупают самостоятельно, или документ скопирован",
        }

    if flag == "single_occurrence":
        return {
            "facts":          "Позиция встречается только в одном документе",
            "deviation":      "Нет базы для сравнения (1 наблюдение)",
            "interpretation": "Возможные причины: разовая закупка, нестандартная номенклатура",
        }

    if flag in ("price_deviation_100", "price_deviation_50", "price_deviation_20"):
        if stats and stats["n"] >= 2:
            label = "Цена единицы" if unit_price > 0 else "Расчётная цена (итог ÷ кол-во)"
            dev   = _deviation_pct(eff_price, stats["median"])
            return {
                "facts":          (
                    f"{label}: {eff_price:,.0f} руб. | "
                    f"Медиана по группе: {stats['median']:,.0f} руб. | "
                    f"Диапазон: {stats['min']:,.0f}–{stats['max']:,.0f} руб. | "
                    f"Наблюдений: {stats['n']}"
                ),
                "deviation":      f"Отклонение от медианы: {dev:.1f}%",
                "interpretation": "Возможные причины: разные условия поставки, другой объём партии, ошибка при вводе данных, обновление прайса",
            }
        return {
            "facts":          f"Цена: {eff_price:,.0f} руб. (недостаточно данных для сравнения)",
            "deviation":      "Менее 2 наблюдений в группе",
            "interpretation": "Проверить вручную — нет базы для автоматического сравнения",
        }

    if flag == "total_price_deviation_40" or flag == "total_price_deviation_15":
        totals = [_to_float(i.get("total_price") or 0) for i in items if _to_float(i.get("total_price") or 0) > 0]
        if totals:
            med = statistics.median(totals)
            dev = _deviation_pct(total_price, med)
            pct_label = ">40%" if flag == "total_price_deviation_40" else ">15%"
            return {
                "facts":          (
                    f"Итоговая сумма по позиции: {total_price:,.0f} руб. | "
                    f"Медиана по группе: {med:,.0f} руб. | "
                    f"Диапазон: {min(totals):,.0f}–{max(totals):,.0f} руб. | "
                    f"Наблюдений: {len(totals)}"
                ),
                "deviation":      f"Отклонение итоговой суммы от медианы: {dev:.1f}% ({pct_label})",
                "interpretation": "Возможные причины: разный объём работ, разные периоды, ошибка в расчёте",
            }

    if flag == "total_mismatch":
        qty      = _to_float(item.get("quantity", 0))
        expected = unit_price * qty if unit_price > 0 and qty > 0 else 0
        return {
            "facts":          f"Цена × кол-во = {expected:,.0f} руб. | Указанная сумма: {total_price:,.0f} руб.",
            "deviation":      f"Расхождение: {abs(expected - total_price):,.0f} руб.",
            "interpretation": "Возможные причины: округление, скидка не отражена в цене, ошибка при заполнении",
        }

    if flag == "volume_without_price":
        qty = _to_float(item.get("quantity", 0))
        return {
            "facts":          f"Количество: {qty:g} {_get_unit(item)} | Цена единицы: не указана",
            "deviation":      "Отсутствует цена единицы при наличии объёма",
            "interpretation": "Возможные причины: паушальный договор, незаполненная форма",
        }

    if flag == "price_without_volume":
        return {
            "facts":          f"Цена единицы: {unit_price:,.0f} руб. | Количество: не указано",
            "deviation":      "Отсутствует количество при наличии цены",
            "interpretation": "Возможные причины: незаполненная форма, позиция-заголовок",
        }

    if flag == "zero_quantity":
        return {
            "facts":          f"Количество = 0 | Цена единицы: {unit_price:,.0f} руб.",
            "deviation":      "Нулевое количество при ненулевой цене",
            "interpretation": "Возможные причины: ошибка ввода, позиция аннулирована но не удалена",
        }

    if flag == "unit_mismatch":
        units = list({_get_unit(i) for i in items if _get_unit(i)})
        return {
            "facts":          f"Единицы измерения в документах: {', '.join(units)}",
            "deviation":      f"Количество различных единиц: {len(units)} (ожидается 1)",
            "interpretation": "Возможные причины: разные стандарты оформления, ошибка при вводе",
        }

    if flag == "quantity_deviation_50" or flag == "quantity_deviation_20":
        qtys = [_to_float(i.get("quantity", 0)) for i in items if _to_float(i.get("quantity", 0)) > 0]
        if qtys:
            spread = (max(qtys) - min(qtys)) / min(qtys) * 100 if min(qtys) > 0 else 0
            pct_label = ">30%" if flag == "quantity_deviation_50" else ">10%"
            return {
                "facts":          f"Объёмы по документам: мин {min(qtys):g} / макс {max(qtys):g} {_get_unit(item)}",
                "deviation":      f"Разброс объёмов: {spread:.1f}% ({pct_label})",
                "interpretation": "Возможные причины: разные периоды, частичное исполнение, ошибка в документе",
            }

    if flag == "split_suspected":
        source      = _get_source_file(item)
        same_source = [i for i in items if _get_source_file(i) == source]
        return {
            "facts":          f"Позиция встречается {len(same_source)} раз в одном документе",
            "deviation":      f"Повторений в одном источнике: {len(same_source)} (норма: 1)",
            "interpretation": "Возможные причины: позиция разбита на этапы, дублирование строк при вводе",
        }

    if flag == "contractor_concentration":
        return {
            "facts":          f"Поставщик: {contractors[0] if contractors else '—'} | Документов в группе: {len(items)}",
            "deviation":      "Единственный поставщик для всех документов группы",
            "interpretation": "Возможные причины: долгосрочный договор, ограниченный рынок, отсутствие альтернатив в регионе",
        }

    if flag == "contractor_blacklist":
        return {
            "facts":          f"Контрагент: {_get_contractor(item)}",
            "deviation":      "Контрагент включён в список для проверки",
            "interpretation": "Требует ручной проверки документов",
        }

    if flag == "vague_item":
        return {
            "facts":          f"Наименование позиции: «{_get_name(item)}»",
            "deviation":      "Наименование не содержит уточняющих характеристик",
            "interpretation": "Возможные причины: агрегированная строка, неполное заполнение формы",
        }

    if flag == "round_number":
        val = total_price if _is_round(total_price) else unit_price
        return {
            "facts":          f"Сумма: {val:,.0f} руб.",
            "deviation":      "Сумма кратна крупному значению (1 000 / 5 000 / 10 000)",
            "interpretation": "Возможные причины: плановая оценка, округление при согласовании",
        }

    if flag == "temporal_clustering":
        dates = [_get_date(i) for i in items if _get_date(i)]
        return {
            "facts":          f"Даты закупок: {', '.join(sorted(set(dates))[:5])}",
            "deviation":      "Несколько закупок в течение 3 дней",
            "interpretation": "Возможные причины: срочная закупка, конец периода планирования",
        }

    if flag == "graph_central":
        return {
            "facts":          f"Позиция «{_get_name(item)}» — высокая степень связности в сети закупок",
            "deviation":      "Централизованность выше порогового значения (0.1)",
            "interpretation": "Позиция связана со многими поставщиками/подразделениями — рекомендуется детальный просмотр",
        }

    return {
        "facts":          f"Флаг: {flag}",
        "deviation":      "—",
        "interpretation": "Ручная проверка",
    }


def _is_round(value: float) -> bool:
    if value <= 0:
        return False
    for d in (10000, 5000, 1000):
        if value >= d and value % d == 0:
            return True
    return False


# ─── Построение объяснения ────────────────────────────────────────

def build_explanation(flags: list, item: dict, group: dict) -> str:
    """
    Краткое объяснение для таблицы результатов (одна строка).
    """
    if not flags:
        return "Отклонений не выявлено"

    parts = []
    for flag in flags:
        explained = _explain_flag(flag, item, group)
        dev = explained.get("deviation", "")
        if dev and dev != "—":
            parts.append(dev)
        elif explained.get("facts"):
            parts.append(explained["facts"])

    return " | ".join(parts[:4]) if parts else "Отклонений не выявлено"


def build_full_explanation(flags: list, item: dict, group: dict) -> dict:
    """
    Полное структурированное объяснение для отчёта.
    """
    if not flags:
        return {
            "summary":         "Отклонений не выявлено",
            "flags_explained": [],
        }

    flags_explained = []
    for flag in flags:
        explained = _explain_flag(flag, item, group)
        flags_explained.append({
            "flag":           flag,
            "facts":          explained.get("facts", ""),
            "deviation":      explained.get("deviation", ""),
            "interpretation": explained.get("interpretation", ""),
        })

    # Краткое резюме
    devs    = [f["deviation"] for f in flags_explained if f["deviation"] and f["deviation"] != "—"]
    summary = " | ".join(devs[:3]) if devs else "Требует проверки"

    return {
        "summary":         summary,
        "flags_explained": flags_explained,
    }


# ─── Публичный API ────────────────────────────────────────────────

def explain_result(scored: dict, group: dict) -> dict:
    """Добавляет полное объяснение к результату scorer."""
    flags = scored.get("flags", [])
    full  = build_full_explanation(flags, scored, group)
    return {
        **scored,
        "explanation":      build_explanation(flags, scored, group),
        "full_explanation": full,
    }