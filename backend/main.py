# backend/main.py

import uuid
import shutil
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from backend.pipeline.parser import parse_file
from backend.pipeline.ai_extractor import extract_items
from backend.pipeline.normalizer import normalize_items
from backend.pipeline.source_mapper import attach_source
from backend.pipeline.matcher import match_across_documents
from backend.pipeline.analyzer import analyze_all_groups
from backend.pipeline.graph_builder import build_graph_from_aggregated, export_json
from backend.pipeline.report_generator import generate_report

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
MAX_FILES = 15

app = FastAPI(title="Financial Document Analyzer", version="0.5.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_stored_results: list[dict] = []
_stored_graph: dict = {}
_stored_analysis: dict = {}
_stored_filenames: list[str] = []   # имена загруженных файлов для отчёта


@app.get("/ping")
def ping():
    return {"status": "ok"}


@app.post("/upload")
def upload_files(files: list[UploadFile] = File(...)):
    global _stored_filenames

    if len(files) > MAX_FILES:
        raise HTTPException(400, f"Максимум {MAX_FILES} файлов.")
    if not files:
        raise HTTPException(400, "Файлы не переданы")

    saved_files = []
    _stored_filenames = []

    for file in files:
        unique_name = f"{uuid.uuid4()}_{file.filename}"
        save_path = UPLOAD_DIR / unique_name
        with save_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        parsed = parse_file(save_path)
        ai_items = []
        if parsed["success"] and parsed.get("text"):
            ai_items = extract_items(parsed["text"])
            ai_items = normalize_items(ai_items, source=file.filename)
            ai_items = attach_source(ai_items, unique_name)

        _stored_filenames.append(file.filename)

        saved_files.append({
            "original_name": file.filename,
            "saved_as": unique_name,
            "size_bytes": save_path.stat().st_size,
            "parsed": parsed,
            "items": ai_items,
        })

    return {"uploaded": len(saved_files), "files": saved_files}


@app.post("/analyze")
def analyze(documents: list[dict]):
    global _stored_results, _stored_graph, _stored_analysis

    if not documents:
        raise HTTPException(400, "Список документов пуст")

    for doc in documents:
        dept = (doc.get("department") or "").strip()
        contractor = (doc.get("contractor") or "").strip()
        source_file = (doc.get("source_file") or doc.get("filename") or "").strip()
        date = (doc.get("date") or "").strip()

        enriched = []
        for item in doc.get("items", []):
            it = dict(item)
            if source_file and not it.get("source_file"):
                it["source_file"] = source_file
            if dept and not it.get("department"):
                it["department"] = dept
            if contractor and not it.get("contractor"):
                it["contractor"] = contractor
            if date and not it.get("date"):
                it["date"] = date
            enriched.append(it)

        doc["items"] = normalize_items(enriched, source=source_file)

    matched = match_across_documents(documents)
    analysis = analyze_all_groups(matched)

    _stored_analysis = analysis
    aggregated = analysis.get("results", [])

    for r in aggregated:
        r["departments"] = [d for d in r.get("departments", [])
                            if d and d.strip() not in ("Не определён", "")]

    _stored_results = aggregated

    G = build_graph_from_aggregated(aggregated)
    graph_json = export_json(G)
    _stored_graph = graph_json

    return {
        "status": "ok",
        "items_analyzed": len(aggregated),
        "total_anomalies": analysis.get("total_anomalies", 0),
        "graph_nodes": len(graph_json["nodes"]),
        "graph_edges": len(graph_json["edges"]),
        "summary": analysis.get("summary", ""),
        "high_risk_count": sum(1 for r in aggregated if r["score"] >= 70),
        "medium_risk_count": sum(1 for r in aggregated if 40 <= r["score"] < 70),
    }


@app.get("/results")
def get_results():
    if not _stored_results:
        raise HTTPException(404, "Нет данных. Сначала запусти POST /analyze")
    return {"results": _stored_results}


@app.get("/graph")
def get_graph():
    if not _stored_graph:
        raise HTTPException(404, "Граф не построен. Сначала запусти POST /analyze")
    return _stored_graph


@app.get("/analysis")
def get_full_analysis():
    if not _stored_analysis:
        raise HTTPException(404, "Анализ не выполнен. Сначала запусти POST /analyze")
    return _stored_analysis


@app.get("/report")
def get_report():
    """
    Генерирует и возвращает DOCX-отчёт по последнему анализу.
    Браузер предложит скачать файл.
    """
    if not _stored_results:
        raise HTTPException(404, "Нет данных для отчёта. Сначала запусти POST /analyze")

    try:
        docx_bytes = generate_report(
            results=_stored_results,
            source_files=_stored_filenames or None,
        )
    except Exception as e:
        raise HTTPException(500, f"Ошибка генерации отчёта: {e}")

    from datetime import datetime
    filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"

    return Response(
        content=docx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )