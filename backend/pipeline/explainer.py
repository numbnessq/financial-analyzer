"""
explainer.py — AI объяснения аномалий через Ollama.
Файл: backend/pipeline/explainer.py
"""

import httpx
from backend.pipeline.scorer import ScoreResult


# ─────────────────────────────────────────────
# Настройки Ollama — измени модель если нужно
# ─────────────────────────────────────────────

OLLAMA_URL   = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "mistral"  # ← вставь сюда название из: ollama list


# ─────────────────────────────────────────────
# Формирование промпта
# ─────────────────────────────────────────────

def build_prompt(result: ScoreResult) -> str:
    stats = result.stats
    reasons_text = "\n".join(f"- {r}" for r in result.reasons) if result.reasons else "- нет"

    return f"""Ты — аналитик закупок. Объясни простым языком почему позиция подозрительна.

Позиция: {result.name}
Риск-скор: {result.score}/100 ({result.risk_level})
Цены в документах: {stats.prices}
Средняя цена: {stats.avg_price}
Минимальная: {stats.min_price}
Максимальная: {stats.max_price}
Отклонение от средней: {stats.deviation_pct}%
Найдена в других документах: {"да" if stats.has_match else "нет"}

Сработавшие правила:
{reasons_text}

Напиши 2–3 предложения: что именно подозрительно и почему это важно проверить.
Без технических терминов. Только на русском языке."""


# ─────────────────────────────────────────────
# Запрос к Ollama
# ─────────────────────────────────────────────

def explain(result: ScoreResult) -> str:
    prompt = build_prompt(result)

    try:
        response = httpx.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
            },
            timeout=60,
        )
        response.raise_for_status()
        return response.json()["message"]["content"].strip()

    except Exception as e:
        return f"Ошибка подключения к Ollama: {e}"


def explain_all(results: list[ScoreResult]) -> list[dict]:
    output = []
    for r in results:
        output.append({
            "name":        r.name,
            "score":       r.score,
            "risk_level":  r.risk_level,
            "explanation": explain(r),
        })
    return output


# ─────────────────────────────────────────────
# Запуск / демо
# ─────────────────────────────────────────────

if __name__ == "__main__":
    from scorer import score_all, ItemStats

    items = [
        ItemStats(name="бетон М300",         prices=[4500, 4600, 4550, 7000], has_match=True),
        ItemStats(name="арматура А500С",      prices=[85000],                  has_match=False),
        ItemStats(name="услуги экскаватора",  prices=[10000, 25000],           has_match=False),
    ]

    results = score_all(items)
    explanations = explain_all(results)

    print("\n=== AI Объяснения аномалий ===\n")
    for e in explanations:
        icons = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴", "CRITICAL": "🚨"}
        print(f"{icons.get(e['risk_level'], '⚪')} [{e['score']}/100] {e['name']}")
        print(f"   {e['explanation']}")
        print()