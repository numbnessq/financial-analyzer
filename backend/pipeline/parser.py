# backend/pipeline/parser.py

import pdfplumber
import docx
import openpyxl
from pathlib import Path


def parse_pdf(file_path: Path) -> str:
    """
    Извлекает текст из PDF файла.
    pdfplumber открывает каждую страницу и достаёт текст.
    """
    text_parts = []

    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages):
            page_text = page.extract_text()

            # Некоторые страницы могут быть пустыми или картинками
            if page_text:
                text_parts.append(f"[Страница {i + 1}]\n{page_text}")

    return "\n\n".join(text_parts)


def parse_docx(file_path: Path) -> str:
    """
    Извлекает текст из Word документа (.docx).
    Проходит по каждому параграфу и собирает текст.
    """
    doc = docx.Document(file_path)
    text_parts = []

    for paragraph in doc.paragraphs:
        # Пропускаем пустые параграфы
        if paragraph.text.strip():
            text_parts.append(paragraph.text.strip())

    # Также извлекаем текст из таблиц
    for table in doc.tables:
        for row in table.rows:
            row_text = []
            for cell in row.cells:
                if cell.text.strip():
                    row_text.append(cell.text.strip())
            if row_text:
                text_parts.append(" | ".join(row_text))

    return "\n".join(text_parts)


def parse_xlsx(file_path: Path) -> str:
    """
    Извлекает текст из Excel файла (.xlsx).
    Проходит по всем листам и всем ячейкам.
    """
    wb = openpyxl.load_workbook(file_path, data_only=True)
    text_parts = []

    for sheet_name in wb.sheetnames:
        sheet = wb[sheet_name]
        text_parts.append(f"[Лист: {sheet_name}]")

        for row in sheet.iter_rows():
            row_values = []
            for cell in row:
                # Пропускаем пустые ячейки
                if cell.value is not None:
                    row_values.append(str(cell.value).strip())
            if row_values:
                text_parts.append(" | ".join(row_values))

    return "\n".join(text_parts)


def parse_file(file_path: str | Path) -> dict:
    """
    Единый метод для парсинга любого файла.
    Определяет тип файла по расширению и вызывает нужный парсер.
    Возвращает словарь с текстом и метаданными.
    """
    file_path = Path(file_path)

    # Проверяем что файл существует
    if not file_path.exists():
        return {
            "success": False,
            "file": file_path.name,
            "error": "Файл не найден",
            "text": ""
        }

    extension = file_path.suffix.lower()

    # Выбираем парсер по расширению файла
    parsers = {
        ".pdf": parse_pdf,
        ".docx": parse_docx,
        ".xlsx": parse_xlsx,
    }

    if extension not in parsers:
        return {
            "success": False,
            "file": file_path.name,
            "error": f"Неподдерживаемый формат: {extension}",
            "text": ""
        }

    # Запускаем нужный парсер
    try:
        text = parsers[extension](file_path)

        return {
            "success": True,
            "file": file_path.name,
            "extension": extension,
            "characters": len(text),
            "text": text
        }

    except Exception as e:
        return {
            "success": False,
            "file": file_path.name,
            "error": str(e),
            "text": ""
        }