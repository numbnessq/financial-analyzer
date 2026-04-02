# backend/pipeline/explainer.py
# Шаблонные объяснения — без AI, быстро и точно.

from backend.pipeline.scorer import ScoreResult


TEMPLATES = {
    "no_match": (
        "Позиция встречается только в одном документе — "
        "сравнить цену не с чем. Требуется ручная проверка обоснованности."
    ),
    "deviation_20": (
        "Цена отклоняется от средней более чем на 20%. "
        "Возможно завышение стоимости или ошибка в данных."
    ),
    "deviation_50": (
        "Цена отклоняется от средней более чем на 50% — критическое отклонение. "
        "Необходима проверка поставщика и условий договора."
    ),
    "high_spread": (
        "Сильный разброс цен между документами. "
        "Разные подразделения закупают по существенно разным ценам — "
        "возможна манипуляция или отсутствие единого поставщика."
    ),
    "small_sample": (
        "Малая выборка — менее 3 цен. "
        "Недостаточно данных для объективной оценки."
    ),
    "duplicate": (
        "Одинаковая позиция закупается несколькими отделами — "
        "возможно дублирование закупки через разных поставщиков."
    ),
    "split": (
        "Закупка разбита на несколько мелких операций, "
        "что может скрывать общий объём и обходить лимиты согласования."
    ),
    "contractor_concentration": (
        "Один контрагент доминирует в закупках — "
        "высокая концентрация может указывать на аффилированность."
    ),
    "distance_anomaly": (
        "Заявленное расстояние значительно превышает типичное. "
        "Возможно завышение транспортных расходов."
    ),
    "volume_anomaly": (
        "Объём закупки значительно превышает типичный для этой позиции. "
        "Рекомендуется проверить реальную потребность."
    ),
}


def explain(result: ScoreResult, extra_flags: list[str] | None = None) -> str:
    parts = []

    # Маппинг ключевых слов из reasons → шаблон
    REASON_KEYWORDS = {
        "no_match":                  ["no_match", "нет совпадений", "одном документе"],
        "deviation_20":              ["deviation_20", "отклонение", "20%"],
        "deviation_50":              ["deviation_50", "50%", "критическое"],
        "high_spread":               ["high_spread", "разброс", "spread"],
        "small_sample":              ["small_sample", "малая выборка", "мало данных"],
        "duplicate":                 ["duplicate", "дублир"],
        "split":                     ["split", "дробление"],
        "contractor_concentration":  ["contractor_concentration", "концентрация", "один контрагент"],
        "distance_anomaly":          ["distance_anomaly", "расстояние"],
        "volume_anomaly":            ["volume_anomaly", "объём", "объем"],
    }

    for reason in result.reasons:
        reason_lower = reason.lower()
        for key, keywords in REASON_KEYWORDS.items():
            if any(kw in reason_lower for kw in keywords):
                text = TEMPLATES.get(key, "")
                if text and text not in parts:
                    parts.append(text)

    for flag_type in (extra_flags or []):
        if flag_type in TEMPLATES:
            text = TEMPLATES[flag_type]
            if text not in parts:
                parts.append(text)

    if not parts:
        return "Значительных аномалий не обнаружено."

    return " ".join(parts)


def explain_all(
    results: list[ScoreResult],
    flags_by_item: dict[str, list[str]] | None = None,
) -> list[dict]:
    output = []
    for r in results:
        extra = (flags_by_item or {}).get(r.name, [])
        output.append({
            "name":        r.name,
            "score":       r.score,
            "risk_level":  r.risk_level,
            "explanation": explain(r, extra),
        })
    return output