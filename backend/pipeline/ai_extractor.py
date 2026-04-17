# backend/pipeline/ai_extractor.py
"""
Детерминированный экстрактор таблиц.
AI (Ollama/Mistral) используется ТОЛЬКО как fallback если таблицы не найдены.
"""

import re
import json
import logging
import requests
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

OLLAMA_URL  = "http://localhost:11434/api/generate"
MODEL_NAME  = "mistral"
MAX_RETRIES = 2  # было 3 — снижаем нагрузку

# ─── Числовые утилиты ─────────────────────────────────────────────

def _to_float(x: Any) -> float | None:
    if x is None:
        return None
    s = str(x).strip().replace(" ", "").replace("\xa0", "").replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        v = float(s)
        return v if v >= 0 else None
    except ValueError:
        return None


def _is_numeric(x: Any) -> bool:
    return _to_float(x) is not None


def _looks_like_name(s: str) -> bool:
    s = s.strip()
    if len(s) < 3:
        return False
    # Не число и не единица измерения
    if _is_numeric(s):
        return False
    units = {"шт", "кг", "т", "м", "м2", "м3", "л", "компл", "уп", "пог.м", "ч", "смена"}
    if s.lower() in units:
        return False
    return True


# ─── Детерминированный парсер таблиц ─────────────────────────────

UNIT_RE = re.compile(
    r'\b(шт\.?|кг|тонн[а-я]*|т\.?\b|м2|м3|кв\.?\s*м|куб\.?\s*м|'
    r'л\.?\b|метр[а-я]*|компл\.?|уп\.?|пог\.?\s*м|ч\.?\b|смена)\b',
    re.IGNORECASE
)

DATE_RE = re.compile(
    r'\b(\d{1,2}[./\-]\d{1,2}[./\-]\d{4}|\d{4}[./\-]\d{1,2}[./\-]\d{1,2})\b'
)


def _parse_date(s: str) -> str:
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _extract_dates(text: str) -> list[str]:
    found = []
    for m in DATE_RE.finditer(text):
        d = _parse_date(m.group(1))
        if d and d not in found:
            found.append(d)
    return found


def _extract_unit(cell: str) -> str:
    m = UNIT_RE.search(str(cell))
    return m.group(0).strip().lower() if m else ""


def _classify_columns(header_row: list[str]) -> dict:
    """
    Определяет индексы колонок по ключевым словам в заголовках.
    Возвращает: {name, quantity, unit, unit_price, total_price, contractor, date}
    """
    mapping = {}

    NAME_KW     = {"наименование", "название", "позиция", "описание", "вид работ",
                   "материал", "товар", "услуга", "работа", "номенклатура"}
    QTY_KW      = {"количество", "кол-во", "объём", "объем", "кол.", "qty"}
    UNIT_KW     = {"ед.", "единица", "ед.изм", "единица измерения", "ед"}
    UPRICE_KW   = {"цена", "цена за ед", "стоимость ед", "unit price",
                   "цена ед.", "цена за единицу", "расценка"}
    TPRICE_KW   = {"сумма", "итого", "стоимость", "итоговая", "всего",
                   "общая стоимость", "total", "итог", "итоговая сумма"}
    CONTR_KW    = {"поставщик", "подрядчик", "исполнитель", "контрагент",
                   "организация", "поставщик/подрядчик"}
    DATE_KW     = {"дата", "дата поставки", "дата выполнения", "период", "месяц"}

    for i, h in enumerate(header_row):
        h_low = str(h).lower().strip().rstrip(".")
        if any(kw in h_low for kw in NAME_KW)     and "name"       not in mapping: mapping["name"]        = i
        if any(kw in h_low for kw in QTY_KW)      and "quantity"   not in mapping: mapping["quantity"]    = i
        if any(kw in h_low for kw in UNIT_KW)     and "unit"       not in mapping: mapping["unit"]        = i
        if any(kw in h_low for kw in UPRICE_KW)   and "unit_price" not in mapping: mapping["unit_price"]  = i
        if any(kw in h_low for kw in TPRICE_KW)   and "total_price" not in mapping: mapping["total_price"] = i
        if any(kw in h_low for kw in CONTR_KW)    and "contractor" not in mapping: mapping["contractor"]  = i
        if any(kw in h_low for kw in DATE_KW)     and "date"       not in mapping: mapping["date"]        = i

    return mapping


def _infer_columns_by_numbers(rows: list[list]) -> dict:
    """
    Если заголовков нет — определяем колонки по числовой логике:
    ищем тройку (quantity × unit_price ≈ total_price).
    """
    if not rows:
        return {}

    n_cols = max(len(r) for r in rows)
    if n_cols < 2:
        return {}

    # Собираем числовые значения по колонкам
    col_vals = [[] for _ in range(n_cols)]
    for row in rows:
        for ci, cell in enumerate(row):
            v = _to_float(cell)
            if v is not None and v > 0:
                col_vals[ci].append(v)

    numeric_cols = [i for i in range(n_cols) if len(col_vals[i]) >= max(1, len(rows) // 3)]
    if len(numeric_cols) < 2:
        return {}

    # Ищем тройку quantity × unit_price ≈ total_price
    best_mapping = {}
    best_score   = 0

    for qi in numeric_cols:
        for ui in numeric_cols:
            if ui == qi:
                continue
            for ti in numeric_cols:
                if ti in (qi, ui):
                    continue
                matches = 0
                for row in rows:
                    q = _to_float(row[qi] if qi < len(row) else None)
                    u = _to_float(row[ui] if ui < len(row) else None)
                    t = _to_float(row[ti] if ti < len(row) else None)
                    if q and u and t and t > 0:
                        if abs(q * u - t) / t < 0.03:  # допуск 3%
                            matches += 1
                if matches > best_score:
                    best_score   = matches
                    best_mapping = {"quantity": qi, "unit_price": ui, "total_price": ti}

    if best_score == 0:
        # Минимум: две числовые колонки → unit_price + total_price
        # Большие значения = total, меньшие = unit_price
        sums = [(i, sum(col_vals[i]) / len(col_vals[i])) for i in numeric_cols]
        sums.sort(key=lambda x: x[1])
        if len(sums) >= 2:
            best_mapping = {"unit_price": sums[0][0], "total_price": sums[-1][0]}

    # Первая нечисловая колонка → name
    for i in range(n_cols):
        if i not in best_mapping.values():
            sample = [str(r[i]) for r in rows[:5] if i < len(r) and str(r[i]).strip()]
            if any(_looks_like_name(s) for s in sample):
                best_mapping["name"] = i
                break

    return best_mapping


def _parse_rows_to_items(rows: list[list], mapping: dict, doc_date: str = "") -> list[dict]:
    """Преобразует строки таблицы в items по найденному маппингу."""
    items = []
    for row in rows:
        if not any(str(c).strip() for c in row):
            continue

        def cell(key):
            i = mapping.get(key)
            return row[i] if i is not None and i < len(row) else None

        name       = str(cell("name") or "").strip()
        quantity   = _to_float(cell("quantity"))
        unit       = _extract_unit(str(cell("unit") or "")) or _extract_unit(name)
        unit_price = _to_float(cell("unit_price"))
        total_price = _to_float(cell("total_price"))
        contractor = str(cell("contractor") or "").strip()
        date_raw   = str(cell("date") or "").strip()
        date       = _parse_date(date_raw) if date_raw else doc_date

        # Восстанавливаем отсутствующие поля
        if unit_price and quantity and not total_price:
            total_price = round(unit_price * quantity, 2)
        if total_price and quantity and not unit_price:
            unit_price = round(total_price / quantity, 2) if quantity else None
        if total_price and unit_price and not quantity:
            quantity = round(total_price / unit_price, 4) if unit_price else None

        if not name or not _looks_like_name(name):
            continue
        if not unit_price and not total_price:
            continue

        items.append({
            "name":        name,
            "quantity":    quantity,
            "unit":        unit,
            "unit_price":  unit_price,
            "total_price": total_price,
            "contractor":  contractor if contractor else None,
            "date":        date,
        })

    return items


# ─── Парсинг текста построчно ─────────────────────────────────────

def _split_to_rows(text: str) -> list[list[str]]:
    """Разбивает plain text на строки-строки таблицы."""
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Разделители: | или 2+ пробела/таб
        if "|" in line:
            cells = [c.strip() for c in line.split("|") if c.strip()]
        else:
            cells = re.split(r'\t|  {2,}', line)
            cells = [c.strip() for c in cells if c.strip()]
        if len(cells) >= 2:
            rows.append(cells)
    return rows


def extract_items_deterministic(text: str) -> list[dict]:
    """
    Детерминированный экстрактор.
    1. Разбивает текст на строки-таблицы
    2. Ищет заголовки → маппинг колонок
    3. Если нет заголовков → числовая инференция
    4. Возвращает items
    """
    rows = _split_to_rows(text)
    if not rows:
        return []

    doc_dates = _extract_dates(text)
    doc_date  = doc_dates[0] if doc_dates else ""

    # Ищем заголовочную строку (первая строка без чисел или с ключевыми словами)
    header_idx = None
    mapping    = {}
    for i, row in enumerate(rows[:10]):
        m = _classify_columns(row)
        if len(m) >= 2:
            header_idx = i
            mapping    = m
            break

    if header_idx is not None:
        data_rows = rows[header_idx + 1:]
    else:
        # Пробуем числовую инференцию
        mapping   = _infer_columns_by_numbers(rows)
        data_rows = rows

    if not mapping:
        return []

    return _parse_rows_to_items(data_rows, mapping, doc_date)


# ─── AI fallback ──────────────────────────────────────────────────

PROMPT_TEMPLATE = """Извлеки позиции из документа закупок. Верни ТОЛЬКО JSON без markdown.
Формат: [{{"name":"","quantity":0,"unit":"","unit_price":0,"total_price":0,"contractor":"","date":""}}]
Правила: quantity и unit_price — числа. total_price = quantity * unit_price. date → YYYY-MM-DD.
Если данных нет — верни [].
Текст:
\"\"\"{TEXT}\"\"\"
"""


def _call_ollama(prompt: str) -> str | None:
    try:
        r = requests.post(
            OLLAMA_URL,
            json={"model": MODEL_NAME, "prompt": prompt, "stream": False, "temperature": 0},
            timeout=60,  # было 120 — снижаем
        )
        r.raise_for_status()
        return r.json().get("response", "")
    except requests.exceptions.ConnectionError:
        logger.warning("Ollama недоступна — пропускаем AI fallback.")
        return None
    except requests.exceptions.Timeout:
        logger.warning("Ollama timeout.")
        return None
    except Exception as e:
        logger.warning(f"Ollama error: {e}")
        return None


def _safe_parse_json(s: str) -> list | None:
    for attempt in (s, s[s.find("["):s.rfind("]")+1] if "[" in s else ""):
        try:
            result = json.loads(attempt)
            if isinstance(result, list):
                return result
        except (json.JSONDecodeError, ValueError):
            continue
    return None


def _extract_items_ai(text: str) -> list[dict]:
    prompt = PROMPT_TEMPLATE.format(TEXT=text[:2500])  # было 3000
    for _ in range(MAX_RETRIES):
        raw = _call_ollama(prompt)
        if not raw:
            break
        items = _safe_parse_json(raw)
        if items is None:
            continue
        result = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if len(name) < 3:
                continue
            # Приводим поля к единому формату
            result.append({
                "name":        name,
                "quantity":    _to_float(item.get("quantity")),
                "unit":        str(item.get("unit", "")).strip(),
                "unit_price":  _to_float(item.get("unit_price") or item.get("price")),
                "total_price": _to_float(item.get("total_price") or item.get("amount")),
                "contractor":  str(item.get("contractor", "")).strip() or None,
                "date":        str(item.get("date", "")).strip() or None,
            })
        if result:
            logger.info(f"AI fallback: извлечено {len(result)} позиций.")
            return result
    return []


# ─── Публичный API ────────────────────────────────────────────────

def extract_items(text: str) -> list[dict]:
    """
    Главная функция.
    Сначала детерминированный парсер, при неудаче — AI fallback.
    """
    if not text or len(text.strip()) < 10:
        return []

    items = extract_items_deterministic(text)
    if items:
        logger.info(f"Deterministic extractor: {len(items)} позиций.")
        return items

    logger.info("Deterministic extractor не нашёл таблиц → AI fallback.")
    return _extract_items_ai(text)