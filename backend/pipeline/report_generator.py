# backend/pipeline/report_generator.py
"""
Генератор DOCX-отчёта по результатам анализа финансовых документов.
Использует python-docx (уже есть в проекте).
"""

from datetime import datetime
from pathlib import Path
from docx import Document
from docx.shared import Pt, RGBColor, Inches, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import copy


# ─── Цвета ────────────────────────────────────────────────────────────────────

COLOR_CRITICAL = RGBColor(0x7C, 0x3A, 0xED)   # фиолетовый
COLOR_HIGH     = RGBColor(0xCC, 0x33, 0x33)   # красный
COLOR_MEDIUM   = RGBColor(0xD4, 0x88, 0x2A)   # оранжевый
COLOR_LOW      = RGBColor(0x2E, 0xA8, 0x4A)   # зелёный
COLOR_ACCENT   = RGBColor(0x1F, 0x49, 0x7D)   # тёмно-синий (заголовки)
COLOR_HEADER_BG = "1F497D"                     # фон шапки таблицы (hex без #)
COLOR_ALT_ROW   = "F2F2F2"                     # чередующиеся строки

RISK_COLORS = {
    "CRITICAL": COLOR_CRITICAL,
    "HIGH":     COLOR_HIGH,
    "MEDIUM":   COLOR_MEDIUM,
    "LOW":      COLOR_LOW,
}

RISK_LABELS = {
    "CRITICAL": "КРИТИЧЕСКИЙ",
    "HIGH":     "ВЫСОКИЙ",
    "MEDIUM":   "СРЕДНИЙ",
    "LOW":      "НИЗКИЙ",
}


# ─── Рекомендации по флагам ───────────────────────────────────────────────────

FLAG_RECOMMENDATIONS = {
    "duplicate_3_plus": (
        "Закупка одной позиции в трёх и более подразделениях требует проверки на "
        "обоснованность. Рекомендуется консолидировать закупки через единого "
        "ответственного или согласовать раздельные потребности документально."
    ),
    "duplicate_2": (
        "Позиция закупается в двух подразделениях. Рекомендуется уточнить, не "
        "является ли это дублированием, и при необходимости объединить заявки."
    ),
    "vague_item": (
        "Размытая формулировка позиции затрудняет контроль расходования средств. "
        "Рекомендуется детализировать наименование с указанием конкретных "
        "характеристик, объёмов и единиц измерения."
    ),
    "price_deviation_50": (
        "Цена отклоняется от средней по группе более чем на 50%. Рекомендуется "
        "запросить у ответственного лица обоснование стоимости и сравнить с "
        "рыночными ценами на аналогичные товары/услуги."
    ),
    "price_deviation_20": (
        "Цена отклоняется от средней более чем на 20%. Рекомендуется проверить "
        "актуальность прайс-листов поставщика и наличие обоснования стоимости."
    ),
    "split_suspected": (
        "Возможное дробление закупки на несколько позиций в одном документе. "
        "Рекомендуется проверить, не направлено ли это на обход порогов "
        "обязательного тендера или конкурентных процедур."
    ),
    "contractor_concentration": (
        "Все закупки сосредоточены у единственного поставщика. Рекомендуется "
        "обеспечить конкурентный отбор или зафиксировать обоснование работы "
        "с единственным источником."
    ),
    "contractor_blacklist": (
        "Контрагент находится в списке подозрительных поставщиков. "
        "Рекомендуется провести проверку юридического лица и приостановить "
        "расчёты до получения результатов проверки."
    ),
    "temporal_clustering": (
        "Несколько закупок одной позиции совершены в очень короткий срок. "
        "Рекомендуется проверить экономическую обоснованность срочности и "
        "исключить искусственное создание дефицита."
    ),
    "graph_central": (
        "Позиция занимает центральное место в сети закупок, связывая множество "
        "подразделений и поставщиков. Рекомендуется уделить особое внимание "
        "этой позиции при аудите."
    ),
}

GENERAL_RECOMMENDATIONS = {
    "CRITICAL": [
        "Немедленно уведомить службу экономической безопасности и руководство.",
        "Приостановить операции по выявленным позициям до завершения проверки.",
        "Запросить первичные документы: договоры, счета, акты выполненных работ.",
        "Рассмотреть вопрос о привлечении внешнего аудитора.",
    ],
    "HIGH": [
        "Провести внутреннее расследование по каждой позиции с уровнем HIGH.",
        "Запросить у ответственных лиц письменные объяснения.",
        "Сверить данные с бухгалтерским учётом и первичной документацией.",
        "Усилить контроль закупок в выявленных подразделениях.",
    ],
    "MEDIUM": [
        "Включить выявленные позиции в план ближайшей внутренней проверки.",
        "Провести выборочный запрос первичных документов.",
        "Рассмотреть возможность ужесточения регламента согласования закупок.",
    ],
    "LOW": [
        "Зафиксировать выявленные позиции для мониторинга в будущих периодах.",
        "Рекомендуется провести профилактическую беседу с ответственными сотрудниками.",
    ],
}


# ─── Вспомогательные функции ──────────────────────────────────────────────────

def _set_cell_bg(cell, hex_color: str):
    """Устанавливает цвет фона ячейки."""
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color)
    tcPr.append(shd)


def _set_cell_borders(cell, color="CCCCCC"):
    """Добавляет границы ячейке."""
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()
    tcBorders = OxmlElement("w:tcBorders")
    for side in ("top", "left", "bottom", "right"):
        border = OxmlElement(f"w:{side}")
        border.set(qn("w:val"), "single")
        border.set(qn("w:sz"), "4")
        border.set(qn("w:space"), "0")
        border.set(qn("w:color"), color)
        tcBorders.append(border)
    tcPr.append(tcBorders)


def _add_para(doc, text, bold=False, size=11, color=None, align=WD_ALIGN_PARAGRAPH.LEFT,
              space_before=0, space_after=6):
    p = doc.add_paragraph()
    p.alignment = align
    p.paragraph_format.space_before = Pt(space_before)
    p.paragraph_format.space_after = Pt(space_after)
    run = p.add_run(text)
    run.bold = bold
    run.font.size = Pt(size)
    if color:
        run.font.color.rgb = color
    return p


def _add_heading(doc, text, level=1):
    sizes = {1: 16, 2: 13, 3: 11}
    p = _add_para(doc, text, bold=True, size=sizes.get(level, 11),
                  color=COLOR_ACCENT, space_before=12, space_after=4)
    # Подчёркивающая линия для уровня 1
    if level == 1:
        pPr = p._p.get_or_add_pPr()
        pBdr = OxmlElement("w:pBdr")
        bottom = OxmlElement("w:bottom")
        bottom.set(qn("w:val"), "single")
        bottom.set(qn("w:sz"), "6")
        bottom.set(qn("w:space"), "1")
        bottom.set(qn("w:color"), "1F497D")
        pBdr.append(bottom)
        pPr.append(pBdr)
    return p


def _score_to_hex(score: int, risk_level: str) -> str:
    """Возвращает hex-цвет фона строки по уровню риска."""
    mapping = {
        "CRITICAL": "EDE7F6",
        "HIGH":     "FFEBEE",
        "MEDIUM":   "FFF8E1",
        "LOW":      "F1F8E9",
    }
    return mapping.get(risk_level, "FFFFFF")


# ─── Основная функция генерации ───────────────────────────────────────────────

def generate_report(results: list[dict], source_files: list[str] = None) -> bytes:
    """
    Генерирует DOCX-отчёт и возвращает его как bytes.

    Args:
        results: список агрегированных результатов из /results
        source_files: список имён проанализированных файлов

    Returns:
        bytes: содержимое .docx файла
    """
    doc = Document()

    # ── Настройки страницы ────────────────────────────────────────────────────
    section = doc.sections[0]
    section.page_width  = Cm(21)    # A4
    section.page_height = Cm(29.7)
    section.left_margin   = Cm(2.5)
    section.right_margin  = Cm(2.0)
    section.top_margin    = Cm(2.0)
    section.bottom_margin = Cm(2.0)

    # ── Шапка документа ──────────────────────────────────────────────────────
    now = datetime.now()
    date_str = now.strftime("%d.%m.%Y %H:%M")

    _add_para(doc, "АНАЛИТИЧЕСКИЙ ОТЧЁТ", bold=True, size=18,
              color=COLOR_ACCENT, align=WD_ALIGN_PARAGRAPH.CENTER,
              space_before=0, space_after=2)
    _add_para(doc, "Анализ финансовых документов на предмет аномалий", size=11,
              color=RGBColor(0x60, 0x60, 0x60), align=WD_ALIGN_PARAGRAPH.CENTER,
              space_before=0, space_after=2)
    _add_para(doc, f"Дата формирования: {date_str}", size=10,
              color=RGBColor(0x80, 0x80, 0x80), align=WD_ALIGN_PARAGRAPH.CENTER,
              space_before=0, space_after=16)

    # ── Дисклеймер ────────────────────────────────────────────────────────────
    disc = doc.add_paragraph()
    disc.alignment = WD_ALIGN_PARAGRAPH.LEFT
    disc.paragraph_format.space_after = Pt(12)
    run = disc.add_run(
        "⚠ Данный отчёт сформирован автоматически на основе алгоритмического анализа. "
        "Все выявленные позиции носят характер индикаторов и требуют дополнительной "
        "проверки специалистом. Система не делает окончательных выводов о нарушениях."
    )
    run.font.size = Pt(9)
    run.font.italic = True
    run.font.color.rgb = RGBColor(0x80, 0x80, 0x80)

    # ── 1. Исходные документы ─────────────────────────────────────────────────
    _add_heading(doc, "1. Проанализированные документы", level=1)

    if source_files:
        for fname in source_files:
            p = doc.add_paragraph(style="List Bullet")
            p.paragraph_format.space_after = Pt(2)
            run = p.add_run(fname)
            run.font.size = Pt(10)
    else:
        _add_para(doc, "Список файлов не передан.", size=10)

    # ── 2. Сводка ─────────────────────────────────────────────────────────────
    _add_heading(doc, "2. Сводка результатов", level=1)

    total     = len(results)
    critical  = sum(1 for r in results if r.get("risk_level") == "CRITICAL")
    high      = sum(1 for r in results if r.get("risk_level") == "HIGH")
    medium    = sum(1 for r in results if r.get("risk_level") == "MEDIUM")
    low       = sum(1 for r in results if r.get("risk_level") == "LOW")
    anomalies = sum(1 for r in results if r.get("score", 0) >= 20)
    avg_score = round(sum(r.get("score", 0) for r in results) / total, 1) if total else 0
    max_score = max((r.get("score", 0) for r in results), default=0)

    # Таблица сводки
    tbl = doc.add_table(rows=2, cols=6)
    tbl.style = "Table Grid"
    headers = ["Позиций", "Аномалий", "КРИТИЧЕСКИХ", "ВЫСОКИХ", "СРЕДНИХ", "Средний скор"]
    values  = [str(total), str(anomalies), str(critical), str(high), str(medium), f"{avg_score}/100"]

    for i, (h, v) in enumerate(zip(headers, values)):
        hcell = tbl.rows[0].cells[i]
        vcell = tbl.rows[1].cells[i]
        _set_cell_bg(hcell, COLOR_HEADER_BG)
        _set_cell_borders(hcell)
        _set_cell_borders(vcell)
        hcell.paragraphs[0].clear()
        hr = hcell.paragraphs[0].add_run(h)
        hr.font.size = Pt(9); hr.font.bold = True
        hr.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        hcell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER

        vcell.paragraphs[0].clear()
        vr = vcell.paragraphs[0].add_run(v)
        vr.font.size = Pt(11); vr.font.bold = True
        # Цвет значения критических/высоких
        if h == "КРИТИЧЕСКИХ" and critical > 0:
            vr.font.color.rgb = COLOR_CRITICAL
        elif h == "ВЫСОКИХ" and high > 0:
            vr.font.color.rgb = COLOR_HIGH
        vcell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER

    doc.add_paragraph()  # отступ

    # ── 3. Детальная таблица аномалий ─────────────────────────────────────────
    _add_heading(doc, "3. Детальный анализ позиций", level=1)

    # Фильтруем: сначала аномалии, затем остальные
    sorted_results = sorted(results, key=lambda r: r.get("score", 0), reverse=True)

    col_widths = [5800, 1600, 1800, 5600]   # DXA
    tbl2 = doc.add_table(rows=1, cols=4)
    tbl2.style = "Table Grid"

    # Установка ширин столбцов через XML
    tbl2_xml = tbl2._tbl
    tblPr = tbl2_xml.find(qn("w:tblPr")) or OxmlElement("w:tblPr")
    tblW = OxmlElement("w:tblW")
    tblW.set(qn("w:w"), str(sum(col_widths)))
    tblW.set(qn("w:type"), "dxa")
    tblPr.append(tblW)

    # Заголовок таблицы
    hrow = tbl2.rows[0]
    col_headers = ["Позиция / Подразделения", "Скор", "Уровень риска", "Объяснение / Рекомендация"]
    for i, (cell, header, w) in enumerate(zip(hrow.cells, col_headers, col_widths)):
        _set_cell_bg(cell, COLOR_HEADER_BG)
        _set_cell_borders(cell, "1F497D")
        cell.paragraphs[0].clear()
        r = cell.paragraphs[0].add_run(header)
        r.font.size = Pt(9)
        r.font.bold = True
        r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        # Ширина столбца
        tc = cell._tc
        tcPr = tc.get_or_add_tcPr()
        tcW = OxmlElement("w:tcW")
        tcW.set(qn("w:w"), str(w))
        tcW.set(qn("w:type"), "dxa")
        tcPr.append(tcW)

    # Строки данных
    for idx, result in enumerate(sorted_results):
        risk_level = result.get("risk_level", "LOW")
        score      = result.get("score", 0)
        name       = result.get("item") or result.get("name") or "—"
        departments = result.get("departments") or []
        contractors = result.get("contractors") or []
        explanation = result.get("explanation") or "—"
        flags       = result.get("flags") or []

        row_bg = _score_to_hex(score, risk_level)

        row = tbl2.add_row()
        for cell, w in zip(row.cells, col_widths):
            _set_cell_borders(cell)
            if idx % 2 == 0:
                _set_cell_bg(cell, row_bg if score >= 20 else "FFFFFF")
            else:
                _set_cell_bg(cell, row_bg if score >= 20 else COLOR_ALT_ROW)
            tc = cell._tc
            tcPr = tc.get_or_add_tcPr()
            tcW = OxmlElement("w:tcW")
            tcW.set(qn("w:w"), str(w))
            tcW.set(qn("w:type"), "dxa")
            tcPr.append(tcW)

        # Столбец 1: Название + отделы
        c0 = row.cells[0]
        c0.paragraphs[0].clear()
        r_name = c0.paragraphs[0].add_run(name)
        r_name.font.size = Pt(9)
        r_name.font.bold = True

        if departments:
            p_dept = c0.add_paragraph()
            p_dept.paragraph_format.space_before = Pt(2)
            r_dept = p_dept.add_run("Отделы: " + ", ".join(departments[:4]))
            r_dept.font.size = Pt(8)
            r_dept.font.color.rgb = RGBColor(0x60, 0x60, 0x60)

        if contractors:
            p_cont = c0.add_paragraph()
            p_cont.paragraph_format.space_before = Pt(1)
            r_cont = p_cont.add_run("Поставщик: " + ", ".join(contractors[:2]))
            r_cont.font.size = Pt(8)
            r_cont.font.color.rgb = RGBColor(0x60, 0x60, 0x60)

        # Столбец 2: Скор
        c1 = row.cells[1]
        c1.paragraphs[0].clear()
        r_score = c1.paragraphs[0].add_run(str(score))
        r_score.font.size = Pt(12)
        r_score.font.bold = True
        r_score.font.color.rgb = RISK_COLORS.get(risk_level, COLOR_LOW)
        c1.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        c1.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

        # Столбец 3: Уровень риска
        c2 = row.cells[2]
        c2.paragraphs[0].clear()
        r_risk = c2.paragraphs[0].add_run(RISK_LABELS.get(risk_level, risk_level))
        r_risk.font.size = Pt(9)
        r_risk.font.bold = True
        r_risk.font.color.rgb = RISK_COLORS.get(risk_level, COLOR_LOW)
        c2.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        c2.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

        # Столбец 4: Объяснение + рекомендации
        c3 = row.cells[3]
        c3.paragraphs[0].clear()
        r_expl = c3.paragraphs[0].add_run(explanation)
        r_expl.font.size = Pt(8)

        # Рекомендации по флагам
        recs = [FLAG_RECOMMENDATIONS[f] for f in flags if f in FLAG_RECOMMENDATIONS]
        if recs:
            p_rec_hdr = c3.add_paragraph()
            p_rec_hdr.paragraph_format.space_before = Pt(4)
            rr = p_rec_hdr.add_run("Рекомендации:")
            rr.font.size = Pt(8)
            rr.font.bold = True
            rr.font.color.rgb = COLOR_ACCENT

            for rec in recs[:2]:  # не более 2 рекомендаций на строку
                p_rec = c3.add_paragraph()
                p_rec.paragraph_format.left_indent = Pt(8)
                p_rec.paragraph_format.space_before = Pt(1)
                rr2 = p_rec.add_run("— " + rec)
                rr2.font.size = Pt(7.5)
                rr2.font.color.rgb = RGBColor(0x40, 0x40, 0x40)

    doc.add_paragraph()  # отступ

    # ── 4. Общие рекомендации ─────────────────────────────────────────────────
    _add_heading(doc, "4. Общие рекомендации", level=1)

    # Определяем наивысший уровень риска в документе
    risk_priority = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    detected_levels = {r.get("risk_level") for r in results if r.get("score", 0) >= 20}

    if not detected_levels:
        _add_para(doc, "Значимых аномалий не обнаружено. Рекомендуется плановый мониторинг.", size=10)
    else:
        for level in risk_priority:
            if level not in detected_levels:
                continue
            count = sum(1 for r in results if r.get("risk_level") == level and r.get("score", 0) >= 20)
            if count == 0:
                continue

            _add_heading(doc, f"{RISK_LABELS[level]} уровень риска — {count} позиц{'ия' if count == 1 else 'ии' if 1 < count < 5 else 'ий'}:", level=2)
            for rec_text in GENERAL_RECOMMENDATIONS.get(level, []):
                p = doc.add_paragraph(style="List Bullet")
                p.paragraph_format.space_after = Pt(2)
                run = p.add_run(rec_text)
                run.font.size = Pt(10)

    doc.add_paragraph()

    # ── Подпись ───────────────────────────────────────────────────────────────
    _add_para(doc, "─" * 60, size=9, color=RGBColor(0xCC, 0xCC, 0xCC),
              space_before=16, space_after=4)
    _add_para(
        doc,
        f"Отчёт сформирован автоматически системой анализа финансовых документов. "
        f"Дата: {date_str}. Все данные обработаны локально и не передавались третьим сторонам.",
        size=8, color=RGBColor(0x80, 0x80, 0x80), space_before=0, space_after=0
    )

    # ── Сохраняем в bytes ─────────────────────────────────────────────────────
    import io
    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.read()