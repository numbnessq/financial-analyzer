# backend/main.py

import uuid
import shutil
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from backend.pipeline.parser         import parse_file
from backend.pipeline.ai_extractor   import extract_items
from backend.models.schemas          import Item, DocumentResult, UploadResponse
from backend.pipeline.normalizer     import normalize_items
from backend.pipeline.source_mapper  import attach_source
from backend.pipeline.matcher        import match_across_documents
from backend.pipeline.analyzer       import analyze_all_groups
from backend.pipeline.scorer         import score_all, ItemStats
from backend.pipeline.explainer      import explain_all
from backend.pipeline.graph_builder  import build_graph, export_json, PurchaseRecord

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
MAX_FILES = 15

app = FastAPI(title="Financial Document Analyzer", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_stored_results: list[dict] = []
_stored_graph:   dict       = {}


@app.get("/ping")
def ping():
    return {"status": "ok"}


@app.post("/upload")
def upload_files(files: list[UploadFile] = File(...)):
    if len(files) > MAX_FILES:
        raise HTTPException(400, f"Максимум {MAX_FILES} файлов. Получено: {len(files)}")
    if not files:
        raise HTTPException(400, "Файлы не переданы")

    saved_files = []
    for file in files:
        unique_name = f"{uuid.uuid4()}_{file.filename}"
        save_path   = UPLOAD_DIR / unique_name
        with save_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        parsed   = parse_file(save_path)
        ai_items = []
        if parsed["success"] and parsed["text"]:
            ai_items = extract_items(parsed["text"])
            ai_items = normalize_items(ai_items, source=file.filename)
            ai_items = attach_source(ai_items, unique_name)

        saved_files.append({
            "original_name": file.filename,
            "saved_as":      unique_name,
            "size_bytes":    save_path.stat().st_size,
            "parsed":        parsed,
            "items":         ai_items,
        })

    documents      = [{"filename": f["original_name"], "items": f["items"]} for f in saved_files]
    matched_groups = match_across_documents(documents)
    analysis       = analyze_all_groups(matched_groups)

    return {
        "uploaded":       len(saved_files),
        "files":          saved_files,
        "matched_groups": matched_groups,
        "analysis":       analysis,
    }


@app.post("/analyze")
def analyze(documents: list[dict]):
    global _stored_results, _stored_graph

    if not documents:
        raise HTTPException(400, "Список документов пуст")

    # Нормализация с enrichment
    for doc in documents:
        dept       = doc.get("department", "")
        contractor = doc.get("contractor", "")
        enriched   = []
        for item in doc.get("items", []):
            it = dict(item)
            if dept       and not it.get("department"): it["department"]  = dept
            if contractor and not it.get("contractor"): it["contractor"]  = contractor
            enriched.append(it)
        doc["items"] = normalize_items(enriched, source=doc.get("filename", ""))

    # Matching
    matched_groups = match_across_documents(documents)

    # Анализ
    analysis = analyze_all_groups(matched_groups)

    # Скоринг — с department_count
    item_stats = []
    for group in analysis.get("groups", []):
        prices = group.get("prices", [])
        if not prices:
            prices = [
                float(i.get("price", 0) or 0)
                for i in group.get("items", [])
                if float(i.get("price", 0) or 0) > 0
            ]

        # Считаем уникальные подразделения в группе
        departments = set(
            i.get("department", "")
            for i in group.get("items", [])
            if i.get("department")
        )
        dept_count = len(departments) if departments else 1

        item_stats.append(ItemStats(
            name=group.get("canonical_name", "unknown"),
            prices=prices,
            has_match=len(group.get("sources", [])) > 1,
            department_count=dept_count,
        ))

    score_results = score_all(item_stats)
    explanations  = explain_all(score_results)

    # Граф
    records = []
    for doc in documents:
        department = doc.get("department", "Неизвестный отдел")
        contractor = doc.get("contractor", "Неизвестный контрагент")
        for item in doc.get("items", []):
            canon = item.get("canonical_name") or item.get("name", "")
            risk_score, risk_level = 0, "LOW"
            for r in score_results:
                if r.name == canon:
                    risk_score = r.score
                    risk_level = r.risk_level
                    break
            records.append(PurchaseRecord(
                department=department,
                contractor=contractor,
                item_name=canon,
                price=float(item.get("price", 0) or 0),
                quantity=float(item.get("quantity", 1) or 1),
                unit=item.get("unit", ""),
                risk_score=risk_score,
                risk_level=risk_level,
            ))

    G          = build_graph(records)
    graph_json = export_json(G)

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
    if not _stored_results:
        raise HTTPException(404, "Нет данных. Сначала запусти POST /analyze")
    return {"results": _stored_results}


@app.get("/graph")
def get_graph():
    if not _stored_graph:
        raise HTTPException(404, "Граф не построен. Сначала запусти POST /analyze")
    return _stored_graph