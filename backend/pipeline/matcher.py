# backend/pipeline/matcher.py
from rapidfuzz import fuzz, process

SIMILARITY_THRESHOLD = 70


def find_best_match(name: str, candidates: list[str]) -> tuple[str, float] | None:
    if not candidates:
        return None
    result = process.extractOne(name, candidates, scorer=fuzz.token_sort_ratio)
    if result is None:
        return None
    best_match, score, _ = result
    return (best_match, score) if score >= SIMILARITY_THRESHOLD else None


def group_items(items: list[dict]) -> list[dict]:
    """
    Группирует по canonical_name, накапливает prices.
    """
    if not items:
        return []

    groups      = []
    group_names = []

    for item in items:
        item_name = item.get("canonical_name") or item.get("name", "")
        if not item_name:
            continue

        match = find_best_match(item_name, group_names)

        if match:
            matched_name, _ = match
            idx   = group_names.index(matched_name)
            group = groups[idx]
            group["items"].append(item)
            group["total_quantity"] += float(item.get("quantity", 0) or 0)

            price = float(item.get("price", 0) or 0)
            if price > 0:
                group["prices"].append(price)

            source = item.get("source", "")
            if source and source not in group["sources"]:
                group["sources"].append(source)
        else:
            price = float(item.get("price", 0) or 0)
            groups.append({
                "canonical_name": item_name,
                "total_quantity": float(item.get("quantity", 0) or 0),
                "unit":           item.get("unit", ""),
                "sources":        [item.get("source", "")] if item.get("source") else [],
                "prices":         [price] if price > 0 else [],
                "items":          [item],
            })
            group_names.append(item_name)

    return groups


def match_across_documents(documents: list[dict]) -> list[dict]:
    all_items = []
    for doc in documents:
        dept       = doc.get("department", "")
        contractor = doc.get("contractor", "")
        for item in doc.get("items", []):
            enriched = dict(item)
            if dept       and not enriched.get("department"):  enriched["department"]  = dept
            if contractor and not enriched.get("contractor"):  enriched["contractor"]  = contractor
            all_items.append(enriched)
    return group_items(all_items)