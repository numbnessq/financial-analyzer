# backend/pipeline/analyzer.py
from collections import Counter
from backend.pipeline.scorer import score_item


class Analyzer:
    def check_consistency(self, items: list[dict]) -> list[dict]:
        errors = []
        for i in items:
            q = i.get("quantity")
            p = i.get("unit_price")
            t = i.get("total_price")
            if q and p and t:
                if abs(q * p - t) / (t + 1e-6) > 0.05:
                    errors.append(i)
        return errors

    def detect_price_anomalies(self, items: list[dict]) -> list[dict]:
        prices = [i["total_price"] for i in items if i.get("total_price")]
        if len(prices) < 3:
            return []
        mean = sum(prices) / len(prices)
        return [i for i in items if i.get("total_price") and i["total_price"] > mean * 3]

    def _not_close(self, a, b, tol=0.05):
        if not a or not b:
            return False
        return abs(a - b) / max(a, b) > tol


def analyze_all_groups(groups: list[dict]) -> dict:
    analyzer = Analyzer()
    analyzed = []
    flat = []

    for g in groups:
        items = g.get("items", [])
        if not items:
            continue

        # Берём первый item как представителя группы для scorer
        representative = items[0]

        # scorer ожидает group с полем "items"
        scored = score_item(representative, g)

        consistency_errors = analyzer.check_consistency(items)
        anomalies = analyzer.detect_price_anomalies(items)

        result = {
            **g,
            # Поля от scorer
            "score":       scored["score"],
            "risk_level":  scored["risk_level"],
            "flags":       scored["flags"],
            "explanation": scored["explanation"],
            "name":        scored["name"],
            "item":        scored["name"],
            "department":  scored.get("department", ""),
            "contractor":  scored.get("contractor", ""),
            "source_file": scored.get("source_file", ""),
            "departments": g.get("departments", []),
            "contractors": g.get("contractors", []),
            # Анализ
            "analysis": {
                "consistency_errors": consistency_errors,
                "anomalies":          anomalies,
                "has_anomalies":      len(anomalies) > 0,
            },
        }
        analyzed.append(result)
        flat.extend(items)

    return {
        "groups":          analyzed,
        "results":         analyzed,
        "flat_results":    flat,
        "total_groups":    len(analyzed),
        "total_anomalies": sum(1 for g in analyzed if g["analysis"]["has_anomalies"]),
        "summary":         f"Проанализировано {len(analyzed)} групп",
    }