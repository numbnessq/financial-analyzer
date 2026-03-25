# backend/main.py

import uuid
import shutil
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException

# Папка для временного хранения загруженных файлов
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)  # создаём папку если её нет

# Максимальное количество файлов за один запрос
MAX_FILES = 15

# Создаём экземпляр FastAPI приложения
app = FastAPI(
    title="Financial Document Analyzer",
    description="Система анализа финансовых документов",
    version="0.1.0"
)


# Тестовый endpoint — проверяет что сервер живой
@app.get("/ping")
def ping():
    return {"status": "ok"}


# Endpoint для загрузки файлов
@app.post("/upload")
def upload_files(files: list[UploadFile] = File(...)):

    # Проверяем количество файлов
    if len(files) > MAX_FILES:
        raise HTTPException(
            status_code=400,
            detail=f"Максимум {MAX_FILES} файлов за один раз. Получено: {len(files)}"
        )

    # Проверяем что файлы вообще есть
    if len(files) == 0:
        raise HTTPException(
            status_code=400,
            detail="Файлы не переданы"
        )

    saved_files = []

    for file in files:
        # Генерируем уникальное имя чтобы файлы не перезаписывали друг друга
        unique_name = f"{uuid.uuid4()}_{file.filename}"
        save_path = UPLOAD_DIR / unique_name

        # Сохраняем файл на диск
        with save_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        saved_files.append({
            "original_name": file.filename,
            "saved_as": unique_name,
            "size_bytes": save_path.stat().st_size
        })

    return {
        "uploaded": len(saved_files),
        "files": saved_files
    }