# backend/pipeline/analyzer.py

import statistics


def calculate_deviation(value: float, reference: float) -> float:
    """
    Считает отклонение значения от эталона в процентах.
    Например: эталон 100, значение 120 → отклонение +20%
    """
    if reference == 0:
        return 0.0
    return round(((value - reference) / reference) * 100, 2)


def calculate_spread(prices: list[float]) -> dict:
    """
    Считает разброс цен внутри группы.
    Возвращает: мин, макс, среднее, стандартное отклонение.
    """
    if not prices:
        return {"min": 0, "max": 0, "mean": 0, "std": 0}

    prices_clean = [p for p in prices if p > 0]

    if not prices_clean:
        return {"min": 0, "max": 0, "mean": 0, "std": 0}

    mean = round(statistics.mean(prices_clean), 2)
    std = round(statistics.stdev(prices_clean), 2) if len(prices_clean) > 1 else 0.0

    return {
        "min": round(min(prices_clean), 2),
        "max": round(max(prices_clean), 2),
        "mean": mean,
        "std": std
    }


def find_reference_price(items: list[dict]) -> float:
    """
    Определяет эталонную цену для группы.

    Логика:
    1. Если есть документ типа "смета" — берём цену оттуда
    2. Если нет — берём среднее по всем ценам в группе
    """
    # Ищем позицию из сметы
    for item in items:
        source = item.get("source", "").lower()
        if any(word in source for word in ["смета", "estimate", "budget"]):
            price = item.get("price", 0)
            if price > 0:
                return price

    # Если сметы нет — считаем среднее
    prices = [item.get("price", 0) for item in items if item.get("price", 0) > 0]

    if not prices:
        return 0.0

    return round(statistics.mean(prices), 2)


def analyze_group(group: dict) -> dict:
    """
    Анализирует одну группу позиций.

    Для каждой позиции считает:
    - отклонение цены от эталона
    - флаг аномалии (если отклонение > 20%)

    Возвращает обогащённую группу с полем 'analysis'.
    """
    items = group.get("items", [])

    if not items:
        return {**group, "analysis": {"error": "Нет позиций для анализа"}}

    # Получаем все цены
    prices = [item.get("price", 0) for item in items]

    # Считаем разброс
    spread = calculate_spread(prices)

    # Определяем эталонную цену
    reference_price = find_reference_price(items)

    # Анализируем каждую позицию
    analyzed_items = []
    anomalies = []

    for item in items:
        price = item.get("price", 0)
        deviation = calculate_deviation(price, reference_price) if reference_price > 0 else 0.0

        # Аномалия если отклонение больше 20%
        is_anomaly = abs(deviation) > 20 and price > 0 and reference_price > 0

        analyzed_item = {
            **item,
            "deviation_pct": deviation,
            "is_anomaly": is_anomaly
        }

        analyzed_items.append(analyzed_item)

        if is_anomaly:
            anomalies.append({
                "source": item.get("source", ""),
                "price": price,
                "deviation_pct": deviation
            })

    return {
        **group,
        "items": analyzed_items,
        "analysis": {
            "reference_price": reference_price,
            "spread": spread,
            "anomaly_count": len(anomalies),
            "anomalies": anomalies,
            "has_anomalies": len(anomalies) > 0
        }
    }


def analyze_all_groups(groups: list[dict]) -> dict:
    """
    Анализирует все группы и возвращает общий отчёт.
    """
    if not groups:
        return {
            "groups": [],
            "total_groups": 0,
            "total_anomalies": 0,
            "summary": "Нет данных для анализа"
        }

    analyzed = [analyze_group(group) for group in groups]

    total_anomalies = sum(
        g.get("analysis", {}).get("anomaly_count", 0)
        for g in analyzed
    )

    # Группы с аномалиями
    groups_with_anomalies = [
        g["canonical_name"]
        for g in analyzed
        if g.get("analysis", {}).get("has_anomalies", False)
    ]

    return {
        "groups": analyzed,
        "total_groups": len(analyzed),
        "total_anomalies": total_anomalies,
        "groups_with_anomalies": groups_with_anomalies,
        "summary": (
            f"Проанализировано {len(analyzed)} групп. "
            f"Найдено аномалий: {total_anomalies}."
        )
    }