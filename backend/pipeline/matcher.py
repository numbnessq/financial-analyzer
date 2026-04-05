# backend/pipeline/matcher.py
from rapidfuzz import fuzz, process

from backend.pipeline.normalizer import canonicalize, normalize_item

SIMILARITY_THRESHOLD = 90


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


def _merge_item_context(item: dict, doc_department: str = "", doc_contractor: str = "", doc_source_file: str = "") -> dict:
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


def group_items(items: list[dict]) -> list[dict]:
    """
    Группирует по canonical_name.
    Важно: items внутри группы сохраняются полностью, без потери department.
    """
    if not items:
        return []

    groups = []
    group_names: list[str] = []

    for raw_item in items:
        item = _merge_item_context(raw_item)

        item_name = item.get("canonical_name") or canonicalize(item.get("name", ""))
        if not item_name:
            continue

        match = find_best_match(item_name, group_names)

        if match:
            matched_name, _ = match
            idx = group_names.index(matched_name)
            group = groups[idx]

            group["items"].append(item)
            group["total_quantity"] += float(item.get("quantity", 0) or 0)

            price = float(item.get("price", 0) or 0)
            if price > 0:
                group["prices"].append(price)

            source_file = item.get("source_file", "")
            if source_file and source_file not in group["sources"]:
                group["sources"].append(source_file)

            dept = item.get("department", "")
            if dept and dept not in group["departments"]:
                group["departments"].append(dept)

            contractor = item.get("contractor", "")
            if contractor and contractor not in group["contractors"]:
                group["contractors"].append(contractor)

        else:
            price = float(item.get("price", 0) or 0)
            dept = item.get("department", "")
            contractor = item.get("contractor", "")
            source_file = item.get("source_file", "")

            groups.append({
                "canonical_name": item_name,
                "total_quantity": float(item.get("quantity", 0) or 0),
                "unit": item.get("unit", ""),
                "sources": [source_file] if source_file else [],
                "departments": [dept] if dept else [],
                "contractors": [contractor] if contractor else [],
                "prices": [price] if price > 0 else [],
                "items": [item],
            })
            group_names.append(item_name)

    return groups


def match_across_documents(documents: list[dict]) -> list[dict]:
    all_items = []

    for doc in documents or []:
        dept = doc.get("department", "")
        contractor = doc.get("contractor", "")
        source_file = doc.get("source_file", "") or doc.get("filename", "")

        for item in doc.get("items", []):
            enriched = _merge_item_context(
                item,
                doc_department=dept,
                doc_contractor=contractor,
                doc_source_file=source_file,
            )
            if enriched.get("name"):
                all_items.append(enriched)

    return group_items(all_items)