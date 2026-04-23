# backend/pipeline/explainer.py
# Rule-based объяснения. AI не используется.

from backend.pipeline.scorer import ScoreResult


# Маппинг флага → текст объяснения
FLAG_TEXTS = {
    "multi_department":          "Одинаковая позиция закупается в 3+ подразделениях одновременно",
    "two_department":            "Одинаковая позиция закупается в 2 подразделениях",
    "no_match":                  "Позиция встречается только в одном документе — сравнение невозможно",
    "deviation_20":              "Цена отклоняется от средней более чем на 20%",
    "deviation_50":              "Цена отклоняется от средней более чем на 50% — критическое отклонение",
    "high_spread":               "Сильный разброс цен между документами",
    "small_sample":              "Малая выборка — менее 3 цен для сравнения",
    "duplicate":                 "Дублирующая закупка — та же позиция уже закупалась",
    "split":                     "Закупка разбита на несколько мелких частей",
    "contractor_concentration":  "Один контрагент используется слишком часто",
    "distance_anomaly":          "Заявленное расстояние значительно превышает типичное",
    "volume_anomaly":            "Объём закупки значительно превышает типичный",
}

# Ключевые слова в reasons → флаг (для матчинга из scorer)
REASON_TO_FLAG = {
    "multi_department":         ["3+ подразделениях", "multi_department"],
    "two_department":           ["2 подразделениях",  "two_department"],
    "no_match":                 ["не найдена",        "no_match"],
    "deviation_20":             ["20%",               "deviation_20", "отклонение цены > 20"],
    "deviation_50":             ["50%",               "deviation_50", "отклонение цены > 50"],
    "high_spread":              ["разброс",           "high_spread", "cv"],
    "small_sample":             ["малая выборка",     "small_sample", "менее 3"],
    "duplicate":                ["дублир",            "duplicate"],
    "split":                    ["разбит",            "split", "дробление"],
    "contractor_concentration": ["контрагент",        "contractor_concentration"],
    "distance_anomaly":         ["расстояние",        "distance"],
    "volume_anomaly":           ["объём",             "volume"],
}


def _flags_from_reasons(reasons: list[str]) -> list[str]:
    """Определяет флаги из списка reasons scorer'а."""
    flags = []
    for reason in reasons:
        r_lower = reason.lower()
        for flag, keywords in REASON_TO_FLAG.items():
            if any(kw.lower() in r_lower for kw in keywords):
                if flag not in flags:
                    flags.append(flag)
    return flags


def explain(result: ScoreResult, extra_flags: list[str] | None = None) -> str:
    """
    Формирует объяснение из флагов scorer + дополнительных флагов rule_engine.
    Никакого AI — только правила.
    """
    flags = _flags_from_reasons(result.reasons)
    for f in (extra_flags or []):
        if f not in flags:
            flags.append(f)

    parts = [FLAG_TEXTS[f] for f in flags if f in FLAG_TEXTS]

    if not parts:
        return "Аномалий не обнаружено."

    return " | ".join(parts)


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
            "flags":       _flags_from_reasons(r.reasons) + extra,
            "explanation": explain(r, extra),
        })
    return output