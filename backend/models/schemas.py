# backend/models/schemas.py

from pydantic import BaseModel, field_validator
from typing import Optional


class Item(BaseModel):
    """
    Единая модель позиции из документа.
    Pydantic автоматически валидирует все поля при создании объекта.
    """

    name: str                        # название позиции
    quantity: float = 0.0            # количество (по умолчанию 0)
    unit: str = ""                   # единица измерения (шт, кг, м2...)
    price: float = 0.0               # цена за единицу
    source: str = ""                 # из какого файла взята позиция
    department: str = "Не определён"
    contractor: str = ""             # контрагент (новое поле!)
    date: str = ""                   # дата закупки (новое поле!)

    @field_validator("name")
    @classmethod
    def name_must_not_be_empty(cls, v):
        """Название не может быть пустым"""
        if not v or not v.strip():
            raise ValueError("Название позиции не может быть пустым")
        return v.strip()

    @field_validator("quantity", "price")
    @classmethod
    def must_be_non_negative(cls, v):
        """Количество и цена не могут быть отрицательными"""
        if v < 0:
            raise ValueError("Значение не может быть отрицательным")
        return v

    @field_validator("unit")
    @classmethod
    def clean_unit(cls, v):
        """Очищаем единицу измерения от лишних пробелов"""
        return v.strip().lower()

    @field_validator("contractor")
    @classmethod
    def clean_contractor(cls, v):
        """Очищаем контрагента"""
        return v.strip()

    @field_validator("date")
    @classmethod
    def clean_date(cls, v):
        """Очищаем дату"""
        return v.strip()


class DocumentResult(BaseModel):
    """
    Результат обработки одного документа.
    """
    filename: str                    # имя файла
    success: bool                    # успешно ли обработан
    items: list[Item] = []          # список найденных позиций
    error: Optional[str] = None     # сообщение об ошибке если есть


class UploadResponse(BaseModel):
    """
    Ответ на запрос загрузки файлов.
    """
    uploaded: int                           # количество загруженных файлов
    results: list[DocumentResult] = []      # результаты по каждому файлу