# backend/main.py
import uuid
import shutil
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
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

app = FastAPI(title="Financial Document Analyzer", version="0.7.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_stored_results:   list[dict] = []
_stored_graph:     dict       = {}
_stored_analysis:  dict       = {}
_stored_filenames: list[str]  = []
_jobs: dict[str, dict]        = {}


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

    saved_files      = []
    _stored_filenames = []

    for file in files:
        unique_name = f"{uuid.uuid4()}_{file.filename}"
        save_path   = UPLOAD_DIR / unique_name
        with save_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        parsed = parse_file(save_path)
        _stored_filenames.append(file.filename)

        # Нормализуем items из парсера
        parsed_items = parsed.get("items", [])
        norm_items   = normalize_items(parsed_items, source=file.filename) if parsed_items else []

        saved_files.append({
            "original_name": file.filename,
            "saved_as":      unique_name,
            "size_bytes":    save_path.stat().st_size,
            "parsed":        parsed,
            # Передаём структурированные items из парсера
            "items":         norm_items,
            "raw_text":      parsed.get("text", "") if parsed.get("success") else "",
            "metadata":      parsed.get("metadata", {}),
        })

    return {"uploaded": len(saved_files), "files": saved_files}


def _run_analysis_job(job_id: str, documents: list[dict], filenames: list[str]):
    global _stored_results, _stored_graph, _stored_analysis

    try:
        _jobs[job_id] = {"status": "extracting", "progress": 10, "message": "Извлечение данных..."}

        enriched_docs = []
        for i, doc in enumerate(documents):
            source_file = (doc.get("source_file") or doc.get("filename") or "").strip()
            dept        = (doc.get("department") or "").strip()
            contractor  = (doc.get("contractor") or "").strip()
            date        = (doc.get("date") or "").strip()
            metadata    = doc.get("metadata") or {}

            # Подрядчик из метаданных парсера если не передан явно
            if not contractor:
                contractor = metadata.get("contractor", "")
            if not date:
                date = metadata.get("date", "")

            # Берём items из парсера (уже структурированные)
            existing_items = doc.get("items", [])

            # AI fallback только если парсер ничего не нашёл
            raw_text = doc.get("raw_text", "")
            if not existing_items and raw_text:
                ai_items = extract_items(raw_text)
                ai_items = normalize_items(ai_items, source=source_file)
                ai_items = attach_source(ai_items, source_file)
                existing_items = ai_items

            # Обогащаем метаданными документа
            enriched = []
            for item in existing_items:
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

            enriched_docs.append({
                **doc,
                "items":      enriched,   # убрать normalize_items отсюда
                "contractor": contractor,
                "department": dept,
            })

            progress = 10 + int((i + 1) / len(documents) * 50)
            _jobs[job_id] = {
                "status":   "extracting",
                "progress": progress,
                "message":  f"Обработано файлов: {i+1}/{len(documents)}",
            }

        _jobs[job_id] = {"status": "analyzing", "progress": 65, "message": "Анализ аномалий..."}

        matched  = match_across_documents(enriched_docs)
        analysis = analyze_all_groups(matched)
        _stored_analysis = analysis

        aggregated = analysis.get("results", [])
        for r in aggregated:
            r["departments"] = [
                d for d in r.get("departments", [])
                if d and d.strip() not in ("Не определён", "")
            ]
        _stored_results = aggregated

        _jobs[job_id] = {"status": "building_graph", "progress": 85, "message": "Построение графа..."}

        G          = build_graph_from_aggregated(aggregated)
        graph_json = export_json(G)
        _stored_graph = graph_json

        _jobs[job_id] = {
            "status":   "done",
            "progress": 100,
            "message":  "Готово",
            "result": {
                "status":            "ok",
                "items_analyzed":    len(aggregated),
                "total_anomalies":   analysis.get("total_anomalies", 0),
                "graph_nodes":       len(graph_json["nodes"]),
                "graph_edges":       len(graph_json["edges"]),
                "summary":           analysis.get("summary", ""),
                "high_risk_count":   sum(1 for r in aggregated if r["score"] >= 70),
                "medium_risk_count": sum(1 for r in aggregated if 40 <= r["score"] < 70),
            },
        }

    except Exception as e:
        import traceback
        _jobs[job_id] = {"status": "error", "progress": 0, "message": str(e) + "\n" + traceback.format_exc()}


@app.post("/analyze")
def analyze(documents: list[dict], background_tasks: BackgroundTasks):
    if not documents:
        raise HTTPException(400, "Список документов пуст")
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "queued", "progress": 0, "message": "В очереди..."}
    background_tasks.add_task(_run_analysis_job, job_id, documents, _stored_filenames)
    return {"job_id": job_id, "status": "queued"}


@app.get("/job/{job_id}")
def get_job_status(job_id: str):
    if job_id not in _jobs:
        raise HTTPException(404, "Задача не найдена")
    return _jobs[job_id]


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


@app.get("/report/save")
def save_report():
    if not _stored_results:
        raise HTTPException(404, "Нет данных для отчёта.")
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(
                generate_report,
                results=_stored_results,
                source_files=_stored_filenames or None,
            )
            docx_bytes = future.result(timeout=20)  # максимум 20 секунд
    except concurrent.futures.TimeoutError:
        raise HTTPException(504, "Генерация отчёта заняла слишком много времени.")
    except Exception as e:
        raise HTTPException(500, f"Ошибка генерации отчёта: {e}")

    from pathlib import Path
    downloads = Path.home() / "Downloads"
    downloads.mkdir(exist_ok=True)
    from datetime import datetime as _dt
    filename  = f"report_{_dt.now().strftime('%Y%m%d_%H%M%S')}.docx"
    save_path = downloads / filename
    save_path.write_bytes(docx_bytes)
    return {"path": str(save_path), "filename": filename}


@app.get("/report")
def get_report():
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