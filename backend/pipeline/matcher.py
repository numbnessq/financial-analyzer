# backend/pipeline/matcher.py
"""
Матчер позиций между документами.
Использует unit_price / total_price — без устаревшего поля 'price'.
"""

from rapidfuzz import fuzz, process
from backend.pipeline.normalizer import canonicalize, normalize_item

SIMILARITY_THRESHOLD = 90


# ─── Утилиты ──────────────────────────────────────────────────────

def _to_float(x) -> float:
    try:
        return float(str(x).replace(",", ".").replace(" ", "") or 0)
    except (TypeError, ValueError):
        return 0.0


def _similarity(a: str, b: str, **kwargs) -> int:
    if not a or not b:
        return 0
    return max(
        fuzz.ratio(a, b),
        fuzz.token_sort_ratio(a, b),
        fuzz.partial_ratio(a, b),
    )


def find_best_match(name: str, candidates: list[str]) -> tuple[str, float] | None:
    if not candidates:
        return None
    result = process.extractOne(name, candidates, scorer=_similarity)
    if result is None:
        return None
    best_match, score, _ = result
    return (best_match, score) if score >= SIMILARITY_THRESHOLD else None


# ─── Обогащение элемента ──────────────────────────────────────────

def _merge_item_context(
    item: dict,
    doc_department: str = "",
    doc_contractor: str = "",
    doc_source_file: str = "",
) -> dict:
    out = normalize_item(item, source=doc_source_file)

    if not out.get("department") and doc_department:
        out["department"] = doc_department
    if not out.get("contractor") and doc_contractor:
        out["contractor"] = doc_contractor
    if not out.get("source_file") and doc_source_file:
        out["source_file"] = doc_source_file
    if not out.get("source"):
        out["source"] = out.get("source_file", "")
    if not out.get("canonical_name"):
        out["canonical_name"] = canonicalize(out.get("name", ""))

    return out


# ─── Группировка ──────────────────────────────────────────────────

def group_items(items: list[dict]) -> list[dict]:
    if not items:
        return []

    groups: list[dict]  = []
    group_keys: list[str] = []

    for raw_item in items:
        item = _merge_item_context(raw_item)

        item_name  = item.get("canonical_name") or canonicalize(item.get("name", ""))
        contractor = item.get("contractor", "")
        # Группируем ТОЛЬКО по названию позиции — contractor не включаем в ключ
        # чтобы позиции из разных документов (КС-2, КС-6а) объединялись в одну группу
        group_key  = item_name

        if not item_name:
            continue

        match = find_best_match(group_key, group_keys)

        if match:
            matched_key, _ = match
            idx   = group_keys.index(matched_key)
            group = groups[idx]

            group["items"].append(item)
            group["total_quantity"] += _to_float(item.get("quantity", 0))

            # unit_price — для price stats (совместимость со scorer.py)
            up = _to_float(item.get("unit_price") or item.get("price") or 0)
            if up > 0:
                group["prices"].append(up)

            for field, key in (
                ("source_file", "sources"),
                ("department",  "departments"),
                ("contractor",  "contractors"),
                ("date",        "dates"),
            ):
                val = item.get(field, "")
                if val and val not in group[key]:
                    group[key].append(val)
        else:
            up = _to_float(item.get("unit_price") or item.get("price") or 0)
            groups.append({
                "canonical_name": item_name,
                "contractor":     contractor,
                "group_key":      group_key,
                "total_quantity": _to_float(item.get("quantity", 0)),
                "unit":           item.get("unit", ""),
                "sources":        [item.get("source_file", "")] if item.get("source_file") else [],
                "departments":    [item.get("department", "")] if item.get("department") else [],
                "contractors":    [item.get("contractor", "")] if item.get("contractor") else [],
                "dates":          [item.get("date", "")]       if item.get("date") else [],
                "prices":         [up] if up > 0 else [],
                "items":          [item],
            })
            group_keys.append(group_key)

    return groups


# ─── Публичный API ────────────────────────────────────────────────

def match_across_documents(documents: list[dict]) -> list[dict]:
    all_items = []

    for doc in documents or []:
        dept        = doc.get("department", "")
        contractor  = doc.get("contractor", "")
        source_file = doc.get("source_file", "") or doc.get("filename", "")

        for item in doc.get("items", []):
            enriched = _merge_item_context(
                item,
                doc_department=dept,
                doc_contractor=contractor,
                doc_source_file=source_file,
            )
            if enriched.get("name") or enriched.get("contractor") or enriched.get("date"):
                all_items.append(enriched)

    return group_items(all_items)