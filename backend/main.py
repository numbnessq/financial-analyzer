# backend/main.py

import uuid
import shutil
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from backend.pipeline.parser import parse_file
from backend.pipeline.ai_extractor import extract_items

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
MAX_FILES = 15

app = FastAPI(
    title="Financial Document Analyzer",
    description="Система анализа финансовых документов",
    version="0.1.0"
)


@app.get("/ping")
def ping():
    return {"status": "ok"}


@app.post("/upload")
def upload_files(files: list[UploadFile] = File(...)):

    if len(files) > MAX_FILES:
        raise HTTPException(
            status_code=400,
            detail=f"Максимум {MAX_FILES} файлов. Получено: {len(files)}"
        )

    if len(files) == 0:
        raise HTTPException(
            status_code=400,
            detail="Файлы не переданы"
        )

    saved_files = []

    for file in files:
        unique_name = f"{uuid.uuid4()}_{file.filename}"
        save_path = UPLOAD_DIR / unique_name

        with save_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Парсим файл
        parsed = parse_file(save_path)

        # Извлекаем позиции через AI
        ai_items = []
        if parsed["success"] and parsed["text"]:
            ai_items = extract_items(parsed["text"])

        saved_files.append({
            "original_name": file.filename,
            "saved_as": unique_name,
            "size_bytes": save_path.stat().st_size,
            "parsed": parsed,
            "items": ai_items
        })

    return {
        "uploaded": len(saved_files),
        "files": saved_files
    }