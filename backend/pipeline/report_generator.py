# backend/pipeline/report_generator.py
"""
Генератор DOCX-отчёта.
Использует Anthropic Claude для написания аналитического нарратива по каждой позиции.
Fallback — детерминированный текст если API недоступен.
"""

import io
import os
import json
import logging
import requests
from datetime import datetime
from docx import Document
from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

logger = logging.getLogger(__name__)

# ─── Цвета ────────────────────────────────────────────────────────

COLOR_CRITICAL  = RGBColor(0x7C, 0x3A, 0xED)
COLOR_HIGH      = RGBColor(0xCC, 0x33, 0x33)
COLOR_MEDIUM    = RGBColor(0xD4, 0x88, 0x2A)
COLOR_LOW       = RGBColor(0x2E, 0xA8, 0x4A)
COLOR_ACCENT    = RGBColor(0x1F, 0x49, 0x7D)
COLOR_DARK      = RGBColor(0x20, 0x20, 0x30)

RISK_COLORS = {"CRITICAL": COLOR_CRITICAL, "HIGH": COLOR_HIGH, "MEDIUM": COLOR_MEDIUM, "LOW": COLOR_LOW}
RISK_LABELS = {"CRITICAL": "КРИТИЧЕСКИЙ", "HIGH": "ВЫСОКИЙ", "MEDIUM": "СРЕДНИЙ", "LOW": "НИЗКИЙ"}
RISK_BG     = {"CRITICAL": "EDE7F6", "HIGH": "FFEBEE", "MEDIUM": "FFF8E1", "LOW": "F1F8E9"}


# ─── AI нарратив ──────────────────────────────────────────────────

OLLAMA_URL = "http://localhost:11434/api/generate"

def _build_narrative_prompt(results: list[dict], source_files: list[str]) -> str:
    high_risk = [r for r in results if r.get("score", 0) >= 40]
    items_text = []
    for r in high_risk[:12]:
        flags = ", ".join(r.get("flags", []))
        items_text.append(
            f'- "{r.get("name","?")}": скор {r.get("score")}/100 ({r.get("risk_level")}), '
            f'флаги: [{flags}], объяснение: {r.get("explanation","")}'
        )

    return f"""Ты — старший аналитик финансовой безопасности строительной компании.
Тебе предоставлены результаты автоматического анализа закупочных документов на предмет аномалий и потенциального мошенничества.

Документы: {', '.join(source_files or ['не указаны'])}
Всего позиций: {len(results)}
Позиций с высоким риском (скор ≥40): {len(high_risk)}

Детали подозрительных позиций:
{chr(10).join(items_text) if items_text else '— аномалий не обнаружено'}

Напиши профессиональное аналитическое заключение для службы внутреннего контроля.
Структура ответа (строго в таком порядке, используй эти заголовки):

## ИСПОЛНИТЕЛЬНОЕ РЕЗЮМЕ
2-3 абзаца: общая оценка ситуации, ключевые риски, срочность реагирования.

## ДЕТАЛЬНЫЙ АНАЛИЗ АНОМАЛИЙ
По каждой позиции с риском HIGH и CRITICAL — отдельный подраздел.
Для каждой позиции объясни: что именно подозрительно, какова вероятная схема злоупотребления, какие конкретные цифры вызывают вопросы.

## ОЦЕНКА ФИНАНСОВОГО УЩЕРБА
Оцени потенциальный масштаб потерь на основе сумм из данных. Укажи конкретные суммы.

## РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ
Конкретный план действий с приоритетами: что делать в первые 24 часа, в первую неделю, в первый месяц.

## ВЫВОДЫ
1 абзац итогового заключения.

Пиши деловым языком, конкретно, без воды. Используй реальные цифры из данных."""


def _call_ollama_narrative(prompt: str) -> str | None:
    try:
        r = requests.post(
            OLLAMA_URL,
            json={"model": "mistral", "prompt": prompt, "stream": False, "temperature": 0.3},
            timeout=15,  # жёсткий таймаут 15 секунд
        )
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception as e:
        logger.warning(f"Ollama narrative failed: {e}")
        return None


def _call_anthropic_narrative(prompt: str) -> str | None:
    """Вызов через Anthropic API если есть ключ в переменных окружения."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text.strip()
    except Exception as e:
        logger.warning(f"Anthropic narrative failed: {e}")
        return None


def _generate_narrative(results: list[dict], source_files: list[str]) -> str:
    prompt = _build_narrative_prompt(results, source_files)

    # Anthropic API если есть ключ
    text = _call_anthropic_narrative(prompt)
    if text:
        return text

    # Ollama — только если запущена (быстрая проверка)
    try:
        ping = requests.get("http://localhost:11434", timeout=2)
        if ping.ok:
            text = _call_ollama_narrative(prompt)
            if text:
                return text
    except Exception:
        pass

    # Детерминированный fallback — всегда работает
    return _deterministic_narrative(results, source_files)


def _deterministic_narrative(results: list[dict], source_files: list[str]) -> str:
    critical = [r for r in results if r.get("risk_level") == "CRITICAL"]
    high     = [r for r in results if r.get("risk_level") == "HIGH"]
    medium   = [r for r in results if r.get("risk_level") == "MEDIUM"]
    total_sum = sum(r.get("total_price") or 0 for r in results if r.get("total_price"))

    lines = []

    lines.append("## ИСПОЛНИТЕЛЬНОЕ РЕЗЮМЕ")
    if critical or high:
        lines.append(
            f"В ходе анализа {len(source_files or [])} документов выявлены серьёзные признаки нарушений "
            f"в закупочной деятельности. Обнаружено {len(critical)} позиций критического уровня риска "
            f"и {len(high)} позиций высокого уровня риска, требующих немедленного внимания службы "
            f"внутреннего контроля. Общий объём закупок по проанализированным документам составляет "
            f"{total_sum:,.0f} руб."
        )
        lines.append(
            "Выявленные аномалии указывают на возможные схемы завышения стоимости работ, "
            "концентрацию закупок у аффилированных поставщиков и потенциальное дробление "
            "договоров с целью обхода конкурентных процедур. Ситуация требует срочного реагирования."
        )
    else:
        lines.append(
            f"По результатам анализа {len(source_files or [])} документов значимых аномалий "
            f"критического или высокого уровня не выявлено. Рекомендуется плановый мониторинг."
        )

    lines.append("\n## ДЕТАЛЬНЫЙ АНАЛИЗ АНОМАЛИЙ")
    for r in (critical + high)[:8]:
        name  = r.get("name") or r.get("item") or "Позиция"
        score = r.get("score", 0)
        flags = r.get("flags", [])
        expl  = r.get("explanation", "")
        tp    = r.get("total_price") or 0
        up    = r.get("unit_price") or 0

        lines.append(f"\n### {name} (скор: {score}/100)")
        flag_texts = {
            "vague_item":             "Размытая формулировка не позволяет однозначно идентифицировать предмет закупки и верифицировать её обоснованность.",
            "price_deviation_50":     f"Цена единицы ({up:,.0f} руб.) отклоняется от среднерыночной более чем на 50%, что свидетельствует о возможном завышении стоимости.",
            "price_deviation_20":     f"Зафиксировано отклонение цены от средней по группе на 20-50%.",
            "total_mismatch":         "Итоговая сумма не соответствует произведению количества на цену единицы, что указывает на возможную фальсификацию данных.",
            "contractor_concentration": f"Все закупки сосредоточены у единственного поставщика: {', '.join(r.get('contractors', [])[:1])}. Отсутствие конкурентного отбора повышает риск завышения цен.",
            "duplicate_3_plus":       f"Идентичная позиция закупается одновременно в {len(r.get('departments', []))} подразделениях, что может свидетельствовать о задвоении расходов.",
            "split_suspected":        "Признаки искусственного дробления закупки с целью обхода порогов обязательного тендера.",
            "round_number":           f"Итоговая сумма {tp:,.0f} руб. имеет подозрительно круглое значение, характерное для ручной корректировки данных.",
            "quantity_deviation_50":  "Объёмы работ расходятся более чем на 50% между документами одного периода.",
        }
        for flag in flags:
            if flag in flag_texts:
                lines.append(flag_texts[flag])
        if expl and expl != "Без явных аномалий":
            lines.append(f"Системные индикаторы: {expl}")

    lines.append("\n## ОЦЕНКА ФИНАНСОВОГО УЩЕРБА")
    risk_sum = sum((r.get("total_price") or 0) for r in critical + high)
    if risk_sum > 0:
        lines.append(
            f"Суммарный объём закупок по позициям высокого и критического риска составляет "
            f"{risk_sum:,.0f} руб. Исходя из типовых схем завышения стоимости на 15-30%, "
            f"потенциальный ущерб может составить от {risk_sum*0.15:,.0f} до {risk_sum*0.30:,.0f} руб. "
            f"Оценка является предварительной и требует верификации по первичным документам."
        )
    else:
        lines.append("Недостаточно данных для количественной оценки ущерба.")

    lines.append("\n## РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ")
    lines.append("**В течение 24 часов:**")
    lines.append("— Заморозить платежи по позициям с уровнем риска CRITICAL до завершения проверки.")
    lines.append("— Уведомить руководство и службу экономической безопасности.")
    lines.append("— Запросить первичные документы: договоры, счета-фактуры, акты КС-2/КС-3.")
    lines.append("\n**В течение первой недели:**")
    lines.append("— Провести сверку данных с бухгалтерским учётом и банковскими выписками.")
    lines.append("— Запросить письменные объяснения у ответственных сотрудников.")
    lines.append("— Проверить аффилированность поставщиков с сотрудниками компании.")
    lines.append("\n**В течение первого месяца:**")
    lines.append("— Провести полный аудит закупок за отчётный период.")
    lines.append("— Пересмотреть регламент согласования закупок, ввести обязательный конкурентный отбор.")
    lines.append("— Рассмотреть вопрос о привлечении независимого аудитора.")

    lines.append("\n## ВЫВОДЫ")
    if critical:
        lines.append(
            f"Проведённый анализ выявил признаки системных нарушений в закупочной деятельности. "
            f"Наличие {len(critical)} позиций критического риска требует незамедлительного расследования. "
            f"Автоматизированный анализ носит индикативный характер — окончательные выводы должны "
            f"быть сделаны по результатам документальной проверки."
        )
    else:
        lines.append(
            "Выявленные аномалии носят умеренный характер и могут быть объяснены операционными "
            "особенностями деятельности. Рекомендуется включить данные позиции в план "
            "ближайшей внутренней проверки."
        )

    return "\n".join(lines)


# ─── DOCX утилиты ─────────────────────────────────────────────────

def _set_cell_bg(cell, hex_color: str):
    tc   = cell._tc
    tcPr = tc.get_or_add_tcPr()
    shd  = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color)
    tcPr.append(shd)


def _set_cell_border(cell, color="CCCCCC"):
    tc     = cell._tc
    tcPr   = tc.get_or_add_tcPr()
    tcBdr  = OxmlElement("w:tcBorders")
    for side in ("top", "left", "bottom", "right"):
        b = OxmlElement(f"w:{side}")
        b.set(qn("w:val"), "single"); b.set(qn("w:sz"), "4")
        b.set(qn("w:space"), "0");    b.set(qn("w:color"), color)
        tcBdr.append(b)
    tcPr.append(tcBdr)


def _para(doc, text, bold=False, size=11, color=None,
          align=WD_ALIGN_PARAGRAPH.LEFT, space_before=0, space_after=6, italic=False):
    p   = doc.add_paragraph()
    p.alignment = align
    p.paragraph_format.space_before = Pt(space_before)
    p.paragraph_format.space_after  = Pt(space_after)
    run = p.add_run(text)
    run.bold         = bold
    run.italic       = italic
    run.font.size    = Pt(size)
    if color:
        run.font.color.rgb = color
    return p


def _heading(doc, text, level=1):
    sizes = {1: 14, 2: 12, 3: 11}
    p = _para(doc, text, bold=True, size=sizes.get(level, 11),
              color=COLOR_ACCENT, space_before=10, space_after=4)
    if level == 1:
        pPr  = p._p.get_or_add_pPr()
        pBdr = OxmlElement("w:pBdr")
        bot  = OxmlElement("w:bottom")
        bot.set(qn("w:val"), "single"); bot.set(qn("w:sz"), "4")
        bot.set(qn("w:space"), "1");    bot.set(qn("w:color"), "1F497D")
        pBdr.append(bot); pPr.append(pBdr)
    return p


def _set_col_width(cell, dxa: int):
    tc   = cell._tc
    tcPr = tc.get_or_add_tcPr()
    tcW  = OxmlElement("w:tcW")
    tcW.set(qn("w:w"), str(dxa)); tcW.set(qn("w:type"), "dxa")
    tcPr.append(tcW)


# ─── Рендер нарратива в DOCX ──────────────────────────────────────

def _render_narrative(doc, narrative: str):
    """Парсит markdown-подобный нарратив и рендерит его в DOCX."""
    for line in narrative.splitlines():
        line = line.rstrip()
        if not line:
            doc.add_paragraph().paragraph_format.space_after = Pt(2)
            continue

        if line.startswith("## "):
            _heading(doc, line[3:], level=1)
        elif line.startswith("### "):
            _heading(doc, line[4:], level=2)
        elif line.startswith("**") and line.endswith("**"):
            _para(doc, line.strip("*"), bold=True, size=10,
                  color=COLOR_ACCENT, space_before=6, space_after=2)
        elif line.startswith("— ") or line.startswith("- "):
            p = doc.add_paragraph(style="List Bullet")
            p.paragraph_format.space_after = Pt(2)
            run = p.add_run(line[2:])
            run.font.size = Pt(10)
        else:
            _para(doc, line, size=10, space_before=0, space_after=4)


# ─── Сводная таблица ──────────────────────────────────────────────

def _render_summary_table(doc, results: list[dict]):
    total    = len(results)
    critical = sum(1 for r in results if r.get("risk_level") == "CRITICAL")
    high     = sum(1 for r in results if r.get("risk_level") == "HIGH")
    medium   = sum(1 for r in results if r.get("risk_level") == "MEDIUM")
    low      = sum(1 for r in results if r.get("risk_level") == "LOW")
    avg      = round(sum(r.get("score", 0) for r in results) / total, 1) if total else 0

    tbl = doc.add_table(rows=2, cols=6)
    tbl.style = "Table Grid"
    hdrs = ["Позиций", "CRITICAL", "HIGH", "MEDIUM", "LOW", "Средний скор"]
    vals = [str(total), str(critical), str(high), str(medium), str(low), f"{avg}/100"]
    vcols = {"CRITICAL": COLOR_CRITICAL, "HIGH": COLOR_HIGH, "MEDIUM": COLOR_MEDIUM}

    for i, (h, v) in enumerate(zip(hdrs, vals)):
        hc = tbl.rows[0].cells[i]
        vc = tbl.rows[1].cells[i]
        _set_cell_bg(hc, "1F497D"); _set_cell_border(hc, "1F497D"); _set_cell_border(vc)
        hc.paragraphs[0].clear()
        hr = hc.paragraphs[0].add_run(h)
        hr.font.size = Pt(9); hr.font.bold = True
        hr.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        hc.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        vc.paragraphs[0].clear()
        vr = vc.paragraphs[0].add_run(v)
        vr.font.size = Pt(12); vr.font.bold = True
        if h in vcols and int(v) > 0:
            vr.font.color.rgb = vcols[h]
        vc.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER


# ─── Таблица позиций ──────────────────────────────────────────────

def _render_items_table(doc, results: list[dict]):
    sorted_r = sorted(results, key=lambda r: r.get("score", 0), reverse=True)
    widths   = [5200, 1400, 1600, 6200]

    tbl = doc.add_table(rows=1, cols=4)
    tbl.style = "Table Grid"

    hrow  = tbl.rows[0]
    hhdrs = ["Позиция / Поставщик", "Скор", "Риск", "Аналитическое заключение"]
    for i, (cell, hdr, w) in enumerate(zip(hrow.cells, hhdrs, widths)):
        _set_cell_bg(cell, "1F497D"); _set_cell_border(cell, "1F497D"); _set_col_width(cell, w)
        cell.paragraphs[0].clear()
        r = cell.paragraphs[0].add_run(hdr)
        r.font.size = Pt(9); r.font.bold = True
        r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER

    for idx, result in enumerate(sorted_r):
        rl    = result.get("risk_level", "LOW")
        score = result.get("score", 0)
        name  = result.get("item") or result.get("name") or "—"
        depts = result.get("departments") or []
        conts = result.get("contractors") or []
        flags = result.get("flags") or []
        tp    = result.get("total_price") or 0
        up    = result.get("unit_price") or 0
        qty   = result.get("quantity") or 0

        row = tbl.add_row()
        bg  = RISK_BG.get(rl, "FFFFFF") if score >= 20 else ("F8F8F8" if idx % 2 else "FFFFFF")
        for cell, w in zip(row.cells, widths):
            _set_cell_border(cell); _set_cell_bg(cell, bg); _set_col_width(cell, w)

        # Колонка 1: название
        c0 = row.cells[0]
        c0.paragraphs[0].clear()
        r0 = c0.paragraphs[0].add_run(name)
        r0.font.size = Pt(9); r0.font.bold = True
        if tp > 0:
            p2 = c0.add_paragraph()
            p2.paragraph_format.space_before = Pt(1)
            r2 = p2.add_run(f"Сумма: {tp:,.0f} руб.")
            r2.font.size = Pt(8); r2.font.color.rgb = RGBColor(0x40, 0x40, 0x40)
        if conts:
            p3 = c0.add_paragraph()
            r3 = p3.add_run(conts[0][:50] + ("…" if len(conts[0]) > 50 else ""))
            r3.font.size = Pt(7.5); r3.font.color.rgb = RGBColor(0x70, 0x70, 0x70)

        # Колонка 2: скор
        c1 = row.cells[1]
        c1.paragraphs[0].clear()
        rs = c1.paragraphs[0].add_run(str(score))
        rs.font.size = Pt(14); rs.font.bold = True
        rs.font.color.rgb = RISK_COLORS.get(rl, COLOR_LOW)
        c1.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        c1.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

        # Колонка 3: уровень риска
        c2 = row.cells[2]
        c2.paragraphs[0].clear()
        rr = c2.paragraphs[0].add_run(RISK_LABELS.get(rl, rl))
        rr.font.size = Pt(8); rr.font.bold = True
        rr.font.color.rgb = RISK_COLORS.get(rl, COLOR_LOW)
        c2.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        c2.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

        # Колонка 4: развёрнутое объяснение
        c3 = row.cells[3]
        c3.paragraphs[0].clear()

        # Генерируем развёрнутый текст по флагам
        explanation_parts = _build_item_explanation(result)
        first = True
        for part in explanation_parts:
            if first:
                r4 = c3.paragraphs[0].add_run(part)
                r4.font.size = Pt(8.5)
                first = False
            else:
                p4 = c3.add_paragraph()
                p4.paragraph_format.space_before = Pt(2)
                r4 = p4.add_run(part)
                r4.font.size = Pt(8.5)


def _build_item_explanation(result: dict) -> list[str]:
    """Строит список развёрнутых объяснений по флагам позиции."""
    flags = result.get("flags", [])
    name  = result.get("item") or result.get("name") or "позиция"
    score = result.get("score", 0)
    tp    = result.get("total_price") or 0
    up    = result.get("unit_price") or 0
    qty   = result.get("quantity") or 0
    conts = result.get("contractors") or []
    depts = result.get("departments") or []
    ref   = result.get("reference_price") or 0
    dev   = result.get("deviation_pct") or 0

    parts = []

    # Вводная фраза по уровню риска
    rl = result.get("risk_level", "LOW")
    if rl == "CRITICAL":
        parts.append(f"ВНИМАНИЕ: выявлено несколько критических индикаторов мошенничества.")
    elif rl == "HIGH":
        parts.append(f"Позиция демонстрирует признаки финансовых злоупотреблений.")

    flag_map = {
        "vague_item": (
            "Наименование позиции сформулировано размыто и не позволяет однозначно "
            "идентифицировать предмет закупки. Это типичный признак «фиктивных» или "
            "завышенных позиций, когда размытость формулировки затрудняет проверку."
        ),
        "price_deviation_50": (
            f"Цена единицы ({up:,.0f} руб.) превышает среднюю по группе ({ref:,.0f} руб.) "
            f"на {dev:.0f}%. Отклонение свыше 50% является признаком возможного завышения "
            f"стоимости или использования нерыночных расценок."
        ) if ref > 0 else (
            f"Зафиксировано критическое отклонение цены (>{50}%) от среднего значения по группе."
        ),
        "price_deviation_20": (
            f"Цена ({up:,.0f} руб.) отклоняется от средней ({ref:,.0f} руб.) на {dev:.0f}%. "
            f"Требует обоснования актуальным прайс-листом поставщика."
        ) if ref > 0 else "Зафиксировано отклонение цены на 20-50% от средней по группе.",
        "total_mismatch": (
            f"Итоговая сумма не соответствует расчётной ({up:,.0f} × {qty:g} = "
            f"{up*qty:,.0f} руб. ≠ {tp:,.0f} руб.). Возможна ручная корректировка данных."
        ) if up > 0 and qty > 0 else (
            "Итоговая сумма не соответствует произведению цены на количество."
        ),
        "contractor_concentration": (
            f"Все закупки по данной позиции сосредоточены у единственного поставщика"
            f"{': ' + conts[0] if conts else ''}. "
            f"Отсутствие конкурентного отбора создаёт условия для сговора и завышения цен."
        ),
        "duplicate_3_plus": (
            f"Идентичная позиция закупается одновременно в {len(depts)} подразделениях "
            f"({', '.join(depts[:3])}). Высокая вероятность задвоения расходов или "
            f"фиктивных закупок."
        ),
        "duplicate_2": (
            f"Позиция закупается в двух подразделениях ({', '.join(depts[:2])}). "
            f"Необходима проверка на обоснованность раздельных закупок."
        ),
        "split_suspected": (
            "Зафиксированы признаки искусственного дробления закупки — множество "
            "однотипных позиций в одном документе. Возможная цель: обход порога "
            "обязательного тендера."
        ),
        "round_number": (
            f"Итоговая сумма {tp:,.0f} руб. имеет подозрительно круглое значение. "
            f"Округлённые суммы без расчётного основания — характерный признак "
            f"ручного завышения данных."
        ),
        "quantity_deviation_50": (
            "Объём работ расходится более чем на 50% между документами одного периода. "
            "Возможно двойное выставление счетов за одни и те же работы."
        ),
        "quantity_deviation_20": (
            "Объём работ расходится на 20-50% между документами. "
            "Рекомендуется сверка с фактически выполненными работами."
        ),
        "temporal_clustering": (
            "Несколько закупок одной позиции совершены в очень короткий срок. "
            "Признак искусственного создания срочности для обхода согласовательных процедур."
        ),
        "contractor_blacklist": (
            f"Поставщик{' ' + conts[0] if conts else ''} находится в списке подозрительных контрагентов. "
            f"Все расчёты с данным поставщиком подлежат немедленной проверке."
        ),
        "volume_without_price": (
            "Указан объём работ, однако цена единицы отсутствует. "
            "Невозможно верифицировать обоснованность итоговой стоимости."
        ),
        "zero_quantity": (
            f"Указана цена {up:,.0f} руб. при нулевом количестве. "
            f"Возможна попытка провести платёж без документального подтверждения объёма."
        ),
    }

    for flag in flags:
        if flag in flag_map:
            parts.append(flag_map[flag])

    if not parts:
        parts.append(result.get("explanation") or "Аномалий не обнаружено.")

    return parts


# ─── Основная функция ─────────────────────────────────────────────

def generate_report(results: list[dict], source_files: list[str] = None) -> bytes:
    doc = Document()

    # Страница A4
    section = doc.sections[0]
    section.page_width    = Cm(21);   section.page_height   = Cm(29.7)
    section.left_margin   = Cm(2.5);  section.right_margin  = Cm(2.0)
    section.top_margin    = Cm(2.0);  section.bottom_margin = Cm(2.0)

    now      = datetime.now()
    date_str = now.strftime("%d.%m.%Y %H:%M")

    # ── Шапка ──
    _para(doc, "АНАЛИТИЧЕСКИЙ ОТЧЁТ", bold=True, size=18,
          color=COLOR_ACCENT, align=WD_ALIGN_PARAGRAPH.CENTER,
          space_before=0, space_after=2)
    _para(doc, "Анализ финансовых документов на предмет аномалий и признаков мошенничества",
          size=10, color=RGBColor(0x60, 0x60, 0x60),
          align=WD_ALIGN_PARAGRAPH.CENTER, space_before=0, space_after=2)
    _para(doc, f"Дата формирования: {date_str}", size=9,
          color=RGBColor(0x80, 0x80, 0x80),
          align=WD_ALIGN_PARAGRAPH.CENTER, space_before=0, space_after=12)

    # Дисклеймер
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(14)
    r = p.add_run(
        "⚠  Данный отчёт сформирован автоматически. Все выявленные позиции носят характер "
        "индикаторов и требуют дополнительной проверки. Система не делает окончательных "
        "выводов о нарушениях."
    )
    r.font.size = Pt(8.5); r.font.italic = True
    r.font.color.rgb = RGBColor(0x80, 0x80, 0x80)

    # ── 1. Документы ──
    _heading(doc, "1. Проанализированные документы", level=1)
    if source_files:
        for f in source_files:
            p = doc.add_paragraph(style="List Bullet")
            p.paragraph_format.space_after = Pt(2)
            p.add_run(f).font.size = Pt(10)
    else:
        _para(doc, "Список файлов не передан.", size=10)

    # ── 2. Сводка ──
    _heading(doc, "2. Сводная статистика", level=1)
    _render_summary_table(doc, results)
    doc.add_paragraph()

    # ── 3. AI нарратив ──
    _heading(doc, "3. Аналитическое заключение", level=1)
    narrative = _generate_narrative(results, source_files or [])
    _render_narrative(doc, narrative)
    doc.add_paragraph()

    # ── 4. Таблица позиций ──
    _heading(doc, "4. Детальный разбор позиций", level=1)
    _render_items_table(doc, results)
    doc.add_paragraph()

    # ── Подпись ──
    _para(doc, "─" * 55, size=8, color=RGBColor(0xCC, 0xCC, 0xCC),
          space_before=14, space_after=3)
    _para(doc,
          f"Отчёт сформирован системой анализа финансовых документов. "
          f"{date_str}. Данные обработаны локально.",
          size=7.5, color=RGBColor(0x90, 0x90, 0x90))

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.read()