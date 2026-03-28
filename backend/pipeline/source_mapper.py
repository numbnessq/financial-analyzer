# backend/pipeline/source_mapper.py

import re

# Словарь ключевых слов для определения отдела по имени файла
# Если в имени файла есть слово — присваиваем отдел
DEPARTMENT_KEYWORDS = {
    "закупка": "Отдел закупок",
    "purchase": "Отдел закупок",
    "склад": "Склад",
    "warehouse": "Склад",
    "бухгалтер": "Бухгалтерия",
    "accounting": "Бухгалтерия",
    "finance": "Бухгалтерия",
    "финанс": "Бухгалтерия",
    "payment": "Бухгалтерия",
    "платёж": "Бухгалтерия",
    "invoice": "Бухгалтерия",
    "счёт": "Бухгалтерия",
    "смета": "Отдел строительства",
    "estimate": "Отдел строительства",
    "договор": "Юридический отдел",
    "contract": "Юридический отдел",
    "поставк": "Отдел снабжения",
    "supply": "Отдел снабжения",
    "продаж": "Отдел продаж",
    "sales": "Отдел продаж",
}


def detect_department(filename: str) -> str:
    """
    Определяет отдел по имени файла.
    Ищет ключевые слова в имени файла.
    Если ничего не найдено — возвращает 'Не определён'.
    """
    filename_lower = filename.lower()

    for keyword, department in DEPARTMENT_KEYWORDS.items():
        if keyword in filename_lower:
            return department

    return "Не определён"


def clean_filename(filename: str) -> str:
    """
    Очищает имя файла от UUID префикса если он есть.
    Например: '889af213-2daa_doc.pdf' → 'doc.pdf'
    """
    # Убираем UUID префикс (формат: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx_)
    cleaned = re.sub(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_',
        '',
        filename
    )
    return cleaned


def attach_source(items: list, filename: str) -> list:
    """
    Добавляет к каждой позиции:
    - source: оригинальное имя файла (без UUID)
    - department: определённый отдел
    """
    clean_name = clean_filename(filename)
    department = detect_department(clean_name)

    result = []
    for item in items:
        item["source"] = clean_name
        item["department"] = department
        result.append(item)

    return result