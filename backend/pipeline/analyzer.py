# backend/pipeline/analyzer.py
"""
Анализатор групп позиций v2.
Порядок: валидация → кластеризация → скоринг → объяснение
         → анализ поставщиков → паттерны.
"""

from backend.pipeline.scorer    import score_item, _to_float
from backend.pipeline.explainer import explain_result

try:
    from backend.pipeline.supplier_analyzer import analyze_suppliers, analyze_supplier_changes
    _HAS_SUPPLIER = True
except ImportError:
    _HAS_SUPPLIER = False

try:
    from backend.pipeline.pattern_detector import detect_all_patterns
    _HAS_PATTERNS = True
except ImportError:
    _HAS_PATTERNS = False

try:
    from backend.pipeline.clusterer import cluster_groups
    _HAS_CLUSTER = True
except ImportError:
    _HAS_CLUSTER = False

try:
    from backend.pipeline.price_analyzer import analyze_group_prices
    _HAS_PRICE_ANALYZER = True
except ImportError:
    _HAS_PRICE_ANALYZER = False


# ─── Валидация ────────────────────────────────────────────────────

def validate_items(items: list) -> list:
    issues = []
    for i, item in enumerate(items):
        total = _to_float(item.get("total_price") or 0)
        qty   = _to_float(item.get("quantity") or 0)
        up    = _to_float(item.get("unit_price") or item.get("price") or 0)

        if total == 0 and up > 0 and qty > 0:
            item["total_price"] = round(up * qty, 2)
            issues.append({
                "index": i, "name": item.get("name", ""),
                "issue": f"total_price восстановлен: {item['total_price']:,.2f}",
            })
        if total > 0 and qty > 0 and up == 0:
            item["unit_price"] = round(total / qty, 2)
            issues.append({
                "index": i, "name": item.get("name", ""),
                "issue": f"unit_price восстановлен: {item['unit_price']:,.2f}",
            })
    return issues


def check_group_consistency(items: list) -> list:
    errors = []
    for item in items:
        q = _to_float(item.get("quantity") or 0)
        p = _to_float(item.get("unit_price") or item.get("price") or 0)
        t = _to_float(item.get("total_price") or 0)
        if q > 0 and p > 0 and t > 0:
            expected = q * p
            if abs(expected - t) / t > 0.05:
                errors.append({
                    "name":     item.get("name", ""),
                    "expected": round(expected, 2),
                    "actual":   round(t, 2),
                    "diff_pct": round(abs(expected - t) / t * 100, 1),
                })
    return errors


def detect_price_anomalies(items: list) -> list:
    import statistics as _st
    prices = [_to_float(i.get("total_price") or 0) for i in items
              if _to_float(i.get("total_price") or 0) > 0]
    if len(prices) < 3:
        return []
    median = _st.median(prices)
    return [i for i in items if _to_float(i.get("total_price") or 0) > median * 3]


# ─── Основная функция ─────────────────────────────────────────────

def analyze_all_groups(groups: list) -> dict:
    # Кластеризация: объединяем похожие группы до анализа
    if _HAS_CLUSTER:
        groups = cluster_groups(groups)

    analyzed       = []
    flat           = []
    validation_log = []

    for g in groups:
        items = g.get("items", [])
        if not items:
            continue

        issues = validate_items(items)
        validation_log.extend(issues)

        consistency_errors = check_group_consistency(items)
        price_anomalies    = detect_price_anomalies(items)

        # Расширенный ценовой анализ
        price_analysis = analyze_group_prices(g) if _HAS_PRICE_ANALYZER else None

        representative = items[0]
        scored         = score_item(representative, g)
        explained      = explain_result(scored, g)

        result = {
            **g,
            "score":            explained["score"],
            "risk_level":       explained["risk_level"],
            "flags":            explained["flags"],
            "explanation":      explained["explanation"],
            "full_explanation": explained.get("full_explanation", {}),
            "name":             explained["name"],
            "item":             explained["name"],
            "department":       explained.get("department", ""),
            "contractor":       explained.get("contractor", ""),
            "source_file":      explained.get("source_file", ""),
            "unit_price":       explained.get("unit_price", 0),
            "effective_price":  explained.get("effective_price", 0),
            "total_price":      explained.get("total_price", 0),
            "quantity":         explained.get("quantity", 0),
            "unit":             explained.get("unit", ""),
            "price_stats":      explained.get("price_stats"),
            "reference_price":  explained.get("reference_price", 0),
            "deviation_pct":    explained.get("deviation_pct", 0),
            "departments":      explained.get("departments", g.get("departments", [])),
            "contractors":      explained.get("contractors", g.get("contractors", [])),
            # Расширенный анализ
            "price_analysis":   price_analysis,
            "analysis": {
                "consistency_errors": consistency_errors,
                "price_anomalies":    len(price_anomalies),
                "has_anomalies":      bool(consistency_errors or price_anomalies),
            },
            # Поле для user feedback (архитектурно)
            "user_verdict": None,
        }
        analyzed.append(result)
        flat.extend(items)

    # Анализ поставщиков по всем позициям
    supplier_analysis = analyze_suppliers(flat) if _HAS_SUPPLIER else None
    supplier_changes  = analyze_supplier_changes(analyzed) if _HAS_SUPPLIER else []

    # Паттерны по всем позициям
    pattern_analysis = detect_all_patterns(flat) if _HAS_PATTERNS else None

    high_risk   = sum(1 for r in analyzed if r["score"] >= 70)
    medium_risk = sum(1 for r in analyzed if 40 <= r["score"] < 70)

    return {
        "groups":             analyzed,
        "results":            analyzed,
        "flat_results":       flat,
        "total_groups":       len(analyzed),
        "total_anomalies":    sum(1 for g in analyzed if g["analysis"]["has_anomalies"]),
        "high_risk_count":    high_risk,
        "medium_risk_count":  medium_risk,
        "validation_log":     validation_log,
        "supplier_analysis":  supplier_analysis,
        "supplier_changes":   supplier_changes,
        "pattern_analysis":   pattern_analysis,
        "summary": (
            f"Проанализировано {len(analyzed)} позиций. "
            f"Требуют внимания: {high_risk + medium_risk} "
            f"(высокий риск: {high_risk}, средний: {medium_risk})"
        ),
    }