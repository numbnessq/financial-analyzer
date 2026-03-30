# backend/main.py

import uuid
import shutil
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from backend.pipeline.parser import parse_file
from backend.pipeline.ai_extractor import extract_items
from backend.models.schemas import Item, DocumentResult, UploadResponse
from backend.pipeline.normalizer import normalize_items
from backend.pipeline.source_mapper import attach_source
from backend.pipeline.matcher import match_across_documents
from backend.pipeline.analyzer import analyze_all_groups
from backend.pipeline.scorer import score_all, ItemStats
from backend.pipeline.explainer import explain_all
from backend.pipeline.graph_builder import build_graph, export_json, PurchaseRecord
from fastapi.middleware.cors import CORSMiddleware

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
MAX_FILES = 15

app = FastAPI(
    title="Financial Document Analyzer",
    description="Система анализа финансовых документов",
    version="0.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# Хранилище в памяти (до перезапуска сервера)
# ─────────────────────────────────────────────

_stored_results: list[dict] = []   # результаты анализа
_stored_graph:   dict       = {}   # граф (nodes + edges)


# ─────────────────────────────────────────────
# Существующие endpoints
# ─────────────────────────────────────────────

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
            ai_items = normalize_items(ai_items, source=file.filename)
            ai_items = attach_source(ai_items, unique_name)

        saved_files.append({
            "original_name": file.filename,
            "saved_as": unique_name,
            "size_bytes": save_path.stat().st_size,
            "parsed": parsed,
            "items": ai_items
        })

    # Собираем все документы для сопоставления
    documents = [
        {"filename": f["original_name"], "items": f["items"]}
        for f in saved_files
    ]

    # Группируем похожие позиции между документами
    matched_groups = match_across_documents(documents)

    # Анализируем группы — ищем аномалии
    analysis = analyze_all_groups(matched_groups)

    return {
        "uploaded": len(saved_files),
        "files": saved_files,
        "matched_groups": matched_groups,
        "analysis": analysis
    }


# ─────────────────────────────────────────────
# Новые endpoints
# ─────────────────────────────────────────────

@app.post("/analyze")
def analyze(documents: list[dict]):
    """
    Запускает весь pipeline: matching → analysis → scoring → AI объяснения → граф.

    Принимает:
        [
          {
            "filename": "doc1.pdf",
            "department": "Отдел строительства",
            "contractor": "ООО СтройСнаб",
            "items": [ { "name": "бетон М300", "price": 4500, "quantity": 50, "unit": "м3" } ]
          }
        ]
    """
    global _stored_results, _stored_graph

    if not documents:
        raise HTTPException(status_code=400, detail="Список документов пуст")

    # 1. Matching
    matched_groups = match_across_documents(documents)

    # 2. Анализ
    analysis = analyze_all_groups(matched_groups)

    # 3. Скоринг
    item_stats = []
    for group in analysis.get("groups", []):
        items = group.get("items", [])
        prices = [i.get("price", 0) for i in items if i.get("price", 0) > 0]
        item_stats.append(ItemStats(
            name=group.get("canonical_name", "unknown"),
            prices=prices,
            has_match=len(items) > 1,
        ))
    score_results = score_all(item_stats)

    # 4. AI объяснения через Ollama
    explanations = explain_all(score_results)

    # 5. Граф
    records = []
    for doc in documents:
        department = doc.get("department", "Неизвестный отдел")
        contractor = doc.get("contractor", "Неизвестный контрагент")
        for item in doc.get("items", []):
            # ищем риск для этой позиции
            risk_score = 0
            risk_level = "LOW"
            for r in score_results:
                if r.name == item.get("name", ""):
                    risk_score = r.score
                    risk_level = r.risk_level
                    break
            records.append(PurchaseRecord(
                department=department,
                contractor=contractor,
                item_name=item.get("name", ""),
                price=item.get("price", 0),
                quantity=item.get("quantity", 1),
                unit=item.get("unit", ""),
                risk_score=risk_score,
                risk_level=risk_level,
            ))

    G = build_graph(records)
    graph_json = export_json(G)

    # Сохраняем в память
    _stored_results = explanations
    _stored_graph   = graph_json

    return {
        "status":       "ok",
        "items_scored": len(score_results),
        "graph_nodes":  len(graph_json["nodes"]),
        "graph_edges":  len(graph_json["edges"]),
    }


@app.get("/results")
def get_results():
    """Возвращает таблицу: позиции со скором и AI объяснением."""
    if not _stored_results:
        raise HTTPException(
            status_code=404,
            detail="Нет данных. Сначала запусти POST /analyze"
        )
    return {"results": _stored_results}


@app.get("/graph")
def get_graph():
    """Возвращает граф в формате { nodes: [...], edges: [...] } для фронтенда."""
    if not _stored_graph:
        raise HTTPException(
            status_code=404,
            detail="Граф не построен. Сначала запусти POST /analyze"
        )
    return _stored_graph
