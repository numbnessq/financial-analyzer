# backend/pipeline/matcher.py

from rapidfuzz import fuzz, process

# Порог схожести — если больше этого числа, считаем позиции одинаковыми
# 85 — хороший баланс между точностью и гибкостью
SIMILARITY_THRESHOLD = 70


def find_best_match(name: str, candidates: list[str]) -> tuple[str, float] | None:
    """
    Ищет наиболее похожее название среди кандидатов.
    Возвращает (найденное_название, процент_схожести) или None.
    """
    if not candidates:
        return None

    result = process.extractOne(
        name,
        candidates,
        scorer=fuzz.token_sort_ratio  # сортирует слова перед сравнением
    )

    if result is None:
        return None

    best_match, score, _ = result

    if score >= SIMILARITY_THRESHOLD:
        return best_match, score

    return None


def group_items(items: list[dict]) -> list[dict]:
    """
    Группирует похожие позиции из разных документов.

    Логика:
    1. Берём первую позицию — создаём группу
    2. Для каждой следующей ищем похожую группу
    3. Если нашли — добавляем в группу
    4. Если нет — создаём новую группу

    Возвращает список групп:
    [
        {
            "canonical_name": "лицензионный платёж",  # стандартное название
            "total_quantity": 2.0,                      # суммарное количество
            "sources": ["file1.pdf", "file2.pdf"],      # откуда взято
            "items": [...]                              # все исходные позиции
        }
    ]
    """
    if not items:
        return []

    groups = []  # список групп
    group_names = []  # названия групп для быстрого поиска

    for item in items:
        item_name = item.get("name", "")

        if not item_name:
            continue

        # Ищем похожую группу
        match = find_best_match(item_name, group_names)

        if match:
            # Нашли похожую группу — добавляем в неё
            matched_name, score = match
            group_index = group_names.index(matched_name)

            group = groups[group_index]
            group["items"].append(item)
            group["total_quantity"] += item.get("quantity", 0)

            # Добавляем источник если его ещё нет
            source = item.get("source", "")
            if source and source not in group["sources"]:
                group["sources"].append(source)

        else:
            # Не нашли похожую группу — создаём новую
            new_group = {
                "canonical_name": item_name,
                "total_quantity": item.get("quantity", 0),
                "unit": item.get("unit", ""),
                "sources": [item.get("source", "")] if item.get("source") else [],
                "items": [item]
            }
            groups.append(new_group)
            group_names.append(item_name)

    return groups


def match_across_documents(documents: list[dict]) -> list[dict]:
    """
    Принимает список результатов по документам,
    собирает все позиции вместе и группирует их.

    documents — список вида:
    [{"filename": "...", "items": [...]}, ...]
    """
    # Собираем все позиции из всех документов в один список
    all_items = []
    for doc in documents:
        items = doc.get("items", [])
        all_items.extend(items)

    # Группируем похожие позиции
    return group_items(all_items)