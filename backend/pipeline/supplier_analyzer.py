# PATCH: добавить в конец backend/pipeline/supplier_analyzer.py

def analyze_supplier_changes(analyzed_groups: list[dict]) -> list[dict]:
    """
    Выявляет резкую смену поставщика между группами.

    Логика:
      - Для каждой группы смотрим contractors[]
      - Если > 1 уникальный контрагент на одну canonical_name → сигнал

    Возвращает список сигналов:
    [
      {
        "item_name":    str,
        "contractors":  list[str],
        "count":        int,
        "severity":     str,
      },
      ...
    ]
    """
    changes = []
    for group in analyzed_groups:
        name        = str(group.get("name") or group.get("item") or "").strip()
        contractors = [
            c for c in group.get("contractors", [])
            if c and c.strip() not in ("", "-", "Не указан")
        ]
        unique_c = list(dict.fromkeys(contractors))   # сохраняем порядок
        if len(unique_c) > 1:
            severity = "high" if len(unique_c) > 2 else "medium"
            changes.append({
                "item_name":   name,
                "contractors": unique_c,
                "count":       len(unique_c),
                "severity":    severity,
            })
    return sorted(changes, key=lambda x: x["count"], reverse=True)