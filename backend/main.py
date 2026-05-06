# backend/main.py  v2.1
import sys
import os
import argparse

if getattr(sys, 'frozen', False):
    sys.path.insert(0, os.path.dirname(sys.executable))

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--app-dir", default=None)
args, _ = parser.parse_known_args()

if args.app_dir:
    APP_DIR = args.app_dir
elif getattr(sys, 'frozen', False):
    APP_DIR = os.path.dirname(sys.executable)
else:
    APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import logging
import uuid
import shutil
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel, Field

from backend.pipeline.parser           import parse_file
from backend.pipeline.ai_extractor     import extract_items
from backend.pipeline.normalizer       import normalize_items, parse_quality_report
from backend.pipeline.source_mapper    import attach_source
from backend.pipeline.matcher          import match_across_documents
from backend.pipeline.analyzer         import analyze_all_groups
from backend.pipeline.graph_builder    import (
    build_graph_from_aggregated,
    export_json,
    build_graph_context,
)
from backend.pipeline.report_generator import generate_report
from backend.pipeline.entity_resolver  import resolve_contractors, apply_resolution
from backend.pipeline.context_manager  import ContextManager
from backend.pipeline.feedback_store   import FeedbackStore
from backend.pipeline.case_manager     import CaseManager, CaseFilter

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

UPLOAD_DIR = Path(APP_DIR) / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MAX_FILES = 15

# ── Глобальные менеджеры (singleton) ─────────────────────────────

context_manager = ContextManager(str(Path(APP_DIR) / "data" / "context_data.json"))
feedback_store  = FeedbackStore(str(Path(APP_DIR) / "data" / "feedback.json"))
case_manager    = CaseManager(str(Path(APP_DIR) / "data" / "cases.json"))


def _init_managers() -> None:
    (Path(APP_DIR) / "data").mkdir(parents=True, exist_ok=True)
    context_manager.load()
    feedback_store.load()
    case_manager.load()
    feedback_store.apply_to_context(context_manager)
    log.info("Managers initialized. Context: %s", context_manager.summary())


# ── App ──────────────────────────────────────────────────────────

app = FastAPI(title="Financial Document Analyzer", version="0.9.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    _init_managers()


# ── In-memory хранилище ──────────────────────────────────────────

_stored_results:   list = []
_stored_graph:     dict = {}
_stored_analysis:  dict = {}
_stored_filenames: list = []
_stored_session:   str  = ""
_jobs:             dict = {}


# ─────────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────────────────

def _score_to_risk(score: int) -> str:
    if score >= 70: return "CRITICAL"
    if score >= 40: return "HIGH"
    if score >= 20: return "MEDIUM"
    return "LOW"


def _sync_blacklist_to_scorer() -> None:
    """Синхронизирует blacklist из context_manager → scorer.CONTRACTOR_BLACKLIST."""
    try:
        from backend.pipeline import scorer
        scorer.CONTRACTOR_BLACKLIST = context_manager.get_blacklist()
    except Exception as e:
        log.warning("Failed to sync blacklist: %s", e)


def _count_node_types(G, nodes: list) -> dict:
    counts: dict[str, int] = {}
    for n in nodes:
        t = G.nodes[n].get("type", "unknown") if n in G.nodes else "unknown"
        counts[t] = counts.get(t, 0) + 1
    return counts


def _apply_graph_centrality(aggregated: list, graph_ctx: dict) -> None:
    """
    Пост-обработка: доначисляет флаг graph_central и корректирует score/risk_level
    для позиций с высокой централизованностью.

    Вызывается ПОСЛЕ build_graph_from_aggregated — граф строится после scorer,
    поэтому централизованность применяется отдельным проходом.
    """
    from backend.pipeline.scorer import FLAG_WEIGHTS, get_risk_level
    from backend.pipeline.graph_builder import CENTRALITY_THRESHOLD

    for result in aggregated:
        item_name  = result.get("name") or result.get("item", "")
        item_key   = f"item:{item_name}"
        ctx        = graph_ctx.get(item_key, {})
        centrality = ctx.get("centrality", 0.0)

        if centrality <= CENTRALITY_THRESHOLD:
            continue
        if "graph_central" in result.get("flags", []):
            continue

        result.setdefault("flags", []).append("graph_central")
        result["centrality"] = round(centrality, 4)

        addition             = FLAG_WEIGHTS.get("graph_central", 8)
        new_score            = min(100, result["score"] + addition)
        result["score"]      = new_score
        result["risk_level"] = get_risk_level(new_score)

        # Пересчитываем объяснение с новым флагом
        try:
            from backend.pipeline.explainer import (
                build_explanation,
                build_full_explanation,
            )
            result["explanation"]      = build_explanation(result["flags"], result, result)
            result["full_explanation"] = build_full_explanation(result["flags"], result, result)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────────────────────────

@app.get("/ping")
def ping():
    return {
        "status":   "ok",
        "version":  "0.9.1",
        "context":  context_manager.summary(),
        "cases":    case_manager.stats(),
        "feedback": feedback_store.stats(),
    }


# ─────────────────────────────────────────────────────────────────
# UPLOAD
# ─────────────────────────────────────────────────────────────────

@app.post("/upload")
def upload_files(files: list[UploadFile] = File(...)):
    global _stored_filenames

    if len(files) > MAX_FILES:
        raise HTTPException(400, f"Максимум {MAX_FILES} файлов.")
    if not files:
        raise HTTPException(400, "Файлы не переданы")

    saved_files       = []
    _stored_filenames = []

    for file in files:
        unique_name = f"{uuid.uuid4()}_{file.filename}"
        save_path   = UPLOAD_DIR / unique_name
        with save_path.open("wb") as buf:
            shutil.copyfileobj(file.file, buf)

        parsed       = parse_file(save_path)
        _stored_filenames.append(file.filename)

        parsed_items = parsed.get("items", [])
        norm_items   = normalize_items(parsed_items, source=file.filename) if parsed_items else []
        quality      = parse_quality_report(norm_items)

        if quality["low_confidence"] > 0:
            log.warning(
                "%s: %d позиций с низким confidence (<0.5), avg=%.2f",
                file.filename, quality["low_confidence"], quality["avg_confidence"],
            )

        saved_files.append({
            "original_name": file.filename,
            "saved_as":      unique_name,
            "size_bytes":    save_path.stat().st_size,
            "parsed":        parsed,
            "items":         norm_items,
            "raw_text":      parsed.get("text", "") if parsed.get("success") else "",
            "metadata":      parsed.get("metadata", {}),
            "parse_quality": quality,
        })

    return {"uploaded": len(saved_files), "files": saved_files}


# ─────────────────────────────────────────────────────────────────
# ANALYSIS PIPELINE
# ─────────────────────────────────────────────────────────────────

def _run_analysis_job(job_id: str, documents: list, filenames: list) -> None:
    global _stored_results, _stored_graph, _stored_analysis, _stored_session

    try:
        _jobs[job_id]   = {"status": "extracting", "progress": 10, "message": "Извлечение данных..."}
        _stored_session = job_id

        # ── Шаг 1: Обогащение документов ─────────────────────────
        enriched_docs: list[dict] = []
        for i, doc in enumerate(documents):
            source_file = (doc.get("source_file") or doc.get("filename") or "").strip()
            dept        = (doc.get("department") or "").strip()
            contractor  = (doc.get("contractor") or "").strip()
            date        = (doc.get("date") or "").strip()
            metadata    = doc.get("metadata") or {}

            if not contractor:
                contractor = metadata.get("contractor", "")
            if not date:
                date = metadata.get("date", "")

            existing_items = doc.get("items", [])
            raw_text       = doc.get("raw_text", "")

            if not existing_items and raw_text:
                ai_items       = extract_items(raw_text)
                ai_items       = normalize_items(ai_items, source=source_file)
                ai_items       = attach_source(ai_items, source_file)
                existing_items = ai_items

            enriched: list[dict] = []
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
                "items":      enriched,
                "contractor": contractor,
                "department": dept,
            })

            progress = 10 + int((i + 1) / len(documents) * 25)
            _jobs[job_id] = {
                "status":   "extracting",
                "progress": progress,
                "message":  f"Обработано файлов: {i + 1}/{len(documents)}",
            }

        # ── Шаг 2: Entity Resolution ──────────────────────────────
        _jobs[job_id] = {
            "status":   "resolving_entities",
            "progress": 38,
            "message":  "Разрешение контрагентов...",
        }

        all_items_flat = [
            item
            for doc in enriched_docs
            for item in doc.get("items", [])
        ]
        entity_registry = resolve_contractors(all_items_flat)
        log.info("Entity resolution: %s", entity_registry.stats())

        for doc in enriched_docs:
            doc["items"] = apply_resolution(doc["items"], entity_registry)

        _sync_blacklist_to_scorer()

        # ── Шаг 3: Matching + Analysis ────────────────────────────
        _jobs[job_id] = {
            "status":   "analyzing",
            "progress": 45,
            "message":  "Анализ аномалий...",
        }

        matched          = match_across_documents(enriched_docs)
        analysis         = analyze_all_groups(matched, context_manager=context_manager)
        _stored_analysis = analysis

        aggregated: list[dict] = analysis.get("results", [])
        for r in aggregated:
            r["departments"] = [
                d for d in r.get("departments", [])
                if d and d.strip() not in ("Не определён", "")
            ]

        # ── Шаг 4: Feedback suppression ───────────────────────────
        _jobs[job_id] = {
            "status":   "applying_feedback",
            "progress": 72,
            "message":  "Применение обратной связи...",
        }

        for r in aggregated:
            item_name = r.get("name") or r.get("item", "")
            new_score, suppressed = feedback_store.get_suppressed_score(
                item_name, r["score"]
            )
            if suppressed:
                r["score"]            = new_score
                r["score_suppressed"] = True
                r["risk_level"]       = _score_to_risk(new_score)
            r["user_verdict"] = feedback_store.get_last_verdict(item_name)

        _stored_results = aggregated

        # Сохраняем исторические цены для следующего анализа
        context_manager.ingest_analysis_results(aggregated)
        context_manager.save()

        # ── Шаг 5: Граф + централизованность ─────────────────────
        _jobs[job_id] = {
            "status":   "building_graph",
            "progress": 85,
            "message":  "Построение графа...",
        }

        G         = build_graph_from_aggregated(aggregated)
        graph_ctx = build_graph_context(G)

        # Доначисляем graph_central флаг теперь когда граф готов
        _apply_graph_centrality(aggregated, graph_ctx)

        graph_json    = export_json(G, include_stats=True)
        _stored_graph = graph_json

        # entity_registry сохраняем отдельно (большой объект)
        _stored_analysis["entity_registry"] = entity_registry.to_dict()

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
                "high_risk_count":   analysis.get("high_risk_count", 0),
                "medium_risk_count": analysis.get("medium_risk_count", 0),
                "has_supplier_risk": (
                    analysis.get("supplier_analysis") or {}
                ).get("has_supplier_risk", False),
                "pattern_count": (
                    analysis.get("pattern_analysis") or {}
                ).get("pattern_count", 0),
                "entity_stats": entity_registry.stats(),
                "graph_stats":  graph_json.get("stats", {}),
            },
        }

    except Exception as exc:
        import traceback
        _jobs[job_id] = {
            "status":   "error",
            "progress": 0,
            "message":  str(exc) + "\n" + traceback.format_exc(),
        }


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


# ─────────────────────────────────────────────────────────────────
# RESULTS
# ─────────────────────────────────────────────────────────────────

@app.get("/results")
def get_results(
    risk_level:  Optional[str]  = Query(None, description="LOW / MEDIUM / HIGH / CRITICAL"),
    contractor:  Optional[str]  = Query(None, description="Частичное совпадение по поставщику"),
    department:  Optional[str]  = Query(None, description="Частичное совпадение по подразделению"),
    min_score:   int            = Query(0,   description="Минимальный score"),
    max_score:   int            = Query(100, description="Максимальный score"),
    has_verdict: Optional[bool] = Query(None, description="True = только с вердиктом"),
    limit:       int            = Query(500, le=1000),
    offset:      int            = Query(0),
):
    if not _stored_results:
        raise HTTPException(404, "Нет данных. Сначала запустите POST /analyze")

    data = _stored_results

    if risk_level:
        rl   = risk_level.upper()
        data = [r for r in data if r.get("risk_level") == rl]
    if contractor:
        cl   = contractor.lower()
        data = [
            r for r in data
            if cl in str(r.get("contractor") or "").lower()
            or any(cl in str(c).lower() for c in r.get("contractors", []))
        ]
    if department:
        dl   = department.lower()
        data = [
            r for r in data
            if dl in str(r.get("department") or "").lower()
            or any(dl in str(d).lower() for d in r.get("departments", []))
        ]
    if min_score > 0:
        data = [r for r in data if r.get("score", 0) >= min_score]
    if max_score < 100:
        data = [r for r in data if r.get("score", 0) <= max_score]
    if has_verdict is True:
        data = [r for r in data if r.get("user_verdict") is not None]
    if has_verdict is False:
        data = [r for r in data if r.get("user_verdict") is None]

    total = len(data)
    return {
        "total":   total,
        "offset":  offset,
        "limit":   limit,
        "results": data[offset: offset + limit],
    }


@app.get("/results/top")
def get_top_risks(n: int = Query(10, le=50)):
    """Top-N позиций по score — стартовая точка расследования."""
    if not _stored_results:
        raise HTTPException(404, "Нет данных")
    top = sorted(_stored_results, key=lambda r: r.get("score", 0), reverse=True)[:n]
    return {"results": top, "total": len(top)}


@app.get("/results/{item_name}")
def get_result_detail(item_name: str):
    """Полные данные по одной позиции (drill-down)."""
    if not _stored_results:
        raise HTTPException(404, "Нет данных")
    for r in _stored_results:
        if r.get("name") == item_name or r.get("item") == item_name:
            return r
    raise HTTPException(404, f"Позиция '{item_name}' не найдена")


# ─────────────────────────────────────────────────────────────────
# FEEDBACK / VERDICT
# ─────────────────────────────────────────────────────────────────

class VerdictBody(BaseModel):
    verdict:  str
    comment:  str = ""
    analyst:  str = ""


@app.patch("/results/{item_name}/verdict")
def set_user_verdict(item_name: str, body: VerdictBody):
    """
    Устанавливает вердикт: valid | false_positive | confirmed_fraud | null.
    Сохраняется в FeedbackStore и влияет на следующий анализ.
    """
    allowed = {"valid", "false_positive", "confirmed_fraud", "null"}
    if body.verdict not in allowed:
        raise HTTPException(400, f"verdict должен быть одним из: {allowed}")

    verdict   = body.verdict if body.verdict != "null" else None
    item_data = next(
        (r for r in _stored_results
         if r.get("name") == item_name or r.get("item") == item_name),
        None,
    )

    if verdict:
        feedback_store.record_verdict(
            item_name        = item_name,
            verdict          = verdict,
            flags            = item_data.get("flags", [])       if item_data else [],
            score            = item_data.get("score", 0)        if item_data else 0,
            department       = item_data.get("department", "")  if item_data else "",
            contractor       = item_data.get("contractor", "")  if item_data else "",
            source_file      = item_data.get("source_file", "") if item_data else "",
            comment          = body.comment,
            analyst          = body.analyst,
            analysis_session = _stored_session,
        )

    if item_data:
        item_data["user_verdict"] = verdict

    feedback_store.apply_to_context(context_manager)

    return {
        "name":           item_name,
        "user_verdict":   verdict,
        "feedback_stats": feedback_store.stats(),
    }


@app.get("/feedback")
def get_feedback(
    verdict: Optional[str] = Query(None),
    limit:   int           = Query(100, le=500),
    offset:  int           = Query(0),
):
    return {
        "records": feedback_store.get_records(verdict=verdict, limit=limit, offset=offset),
        "stats":   feedback_store.stats(),
    }


@app.get("/feedback/recommendations")
def get_feedback_recommendations():
    """Рекомендации по корректировке весов на основе накопленного feedback."""
    return {
        "recommendations":     feedback_store.get_flag_recommendations(),
        "current_adjustments": context_manager._feedback_adjustments,
    }


# ─────────────────────────────────────────────────────────────────
# CASES
# ─────────────────────────────────────────────────────────────────

class CreateCaseBody(BaseModel):
    title:           str
    item_names:      list[str] = Field(default_factory=list)
    priority:        str       = "medium"
    assigned_to:     str       = ""
    tags:            list[str] = Field(default_factory=list)
    initial_comment: str       = ""
    analyst:         str       = ""


class UpdateStatusBody(BaseModel):
    status:  str
    note:    str = ""
    analyst: str = ""


class AddCommentBody(BaseModel):
    text:    str
    analyst: str = ""


class AssignBody(BaseModel):
    analyst: str
    note:    str = ""


@app.post("/cases")
def create_case(body: CreateCaseBody):
    items = []
    if body.item_names:
        items = [
            r for r in _stored_results
            if r.get("name") in body.item_names or r.get("item") in body.item_names
        ]
    case = case_manager.create_case(
        title            = body.title,
        items            = items,
        priority         = body.priority,
        assigned_to      = body.assigned_to,
        tags             = body.tags,
        analysis_session = _stored_session,
        initial_comment  = body.initial_comment,
        analyst          = body.analyst,
    )
    return case.to_dict()


@app.get("/cases")
def list_cases(
    status:      Optional[str] = Query(None),
    priority:    Optional[str] = Query(None),
    assigned_to: Optional[str] = Query(None),
    tag:         Optional[str] = Query(None),
    min_score:   int           = Query(0),
    limit:       int           = Query(50, le=200),
    offset:      int           = Query(0),
):
    f = CaseFilter(
        status      = status,
        priority    = priority,
        assigned_to = assigned_to,
        tag         = tag,
        min_score   = min_score,
        limit       = limit,
        offset      = offset,
    )
    cases = case_manager.list_cases(f)
    total = len(case_manager.list_cases(
        CaseFilter(
            status=status, priority=priority, assigned_to=assigned_to,
            tag=tag, min_score=min_score, limit=10_000,
        )
    ))
    return {
        "total": total,
        "cases": [c.to_dict() for c in cases],
        "stats": case_manager.stats(),
    }


@app.get("/cases/stats")
def get_cases_stats():
    return case_manager.stats()


@app.get("/cases/{case_id}")
def get_case(case_id: str):
    case = case_manager.get_case(case_id)
    if not case:
        raise HTTPException(404, f"Дело {case_id} не найдено")
    return case.to_dict()


@app.patch("/cases/{case_id}/status")
def update_case_status(case_id: str, body: UpdateStatusBody):
    try:
        return case_manager.update_status(
            case_id, body.status, note=body.note, analyst=body.analyst,
        ).to_dict()
    except KeyError:
        raise HTTPException(404, f"Дело {case_id} не найдено")
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.post("/cases/{case_id}/comment")
def add_case_comment(case_id: str, body: AddCommentBody):
    try:
        return case_manager.add_comment(case_id, body.text, analyst=body.analyst).to_dict()
    except KeyError:
        raise HTTPException(404, f"Дело {case_id} не найдено")
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.post("/cases/{case_id}/assign")
def assign_case(case_id: str, body: AssignBody):
    try:
        return case_manager.assign(case_id, body.analyst, note=body.note).to_dict()
    except KeyError:
        raise HTTPException(404, f"Дело {case_id} не найдено")


@app.post("/cases/auto-create")
def auto_create_cases(
    min_score: int = Query(40),
    analyst:   str = Query(""),
):
    """
    Автоматически создаёт дела из текущих результатов анализа.
    CRITICAL (score >= 70) → отдельное дело на позицию.
    HIGH (40–69) → группировка по контрагенту.
    Позиции с уже открытыми делами пропускаются.
    """
    if not _stored_results:
        raise HTTPException(404, "Нет результатов анализа")
    cases = case_manager.auto_create_from_results(
        results          = _stored_results,
        analysis_session = _stored_session,
        min_score        = min_score,
        analyst          = analyst,
    )
    return {"created": len(cases), "cases": [c.to_dict() for c in cases]}


@app.delete("/cases/{case_id}")
def delete_case(case_id: str):
    if not case_manager.delete_case(case_id):
        raise HTTPException(404, f"Дело {case_id} не найдено")
    return {"deleted": case_id}


# ─────────────────────────────────────────────────────────────────
# CONTEXT MANAGEMENT
# ─────────────────────────────────────────────────────────────────

class ContractorBody(BaseModel):
    name:   str
    status: str       = "neutral"   # trusted / neutral / suspicious / blacklisted
    inn:    str       = ""
    tags:   list[str] = Field(default_factory=list)
    note:   str       = ""


class FlagWeightBody(BaseModel):
    flag:   str
    weight: int


class MarketPriceBody(BaseModel):
    item_name: str
    price_ref: float
    price_min: float = 0.0
    price_max: float = 0.0
    unit:      str   = ""
    category:  str   = "default"


@app.get("/context")
def get_context_summary():
    return context_manager.summary()


@app.get("/context/contractors")
def list_contractors():
    return {
        "contractors": [rec.to_dict() for rec in context_manager._contractors.values()]
    }


@app.post("/context/contractors")
def add_contractor(body: ContractorBody):
    rec = context_manager.add_contractor(
        name   = body.name,
        status = body.status,
        inn    = body.inn or None,
        tags   = body.tags,
        note   = body.note,
    )
    context_manager.save()
    return rec.to_dict()


@app.post("/context/flag-weight")
def update_flag_weight(body: FlagWeightBody):
    context_manager.update_flag_weight(body.flag, body.weight)
    context_manager.save()
    return {"flag": body.flag, "weight": body.weight}


@app.post("/context/market-price")
def set_market_price(body: MarketPriceBody):
    mp = context_manager.set_market_price(
        item_name = body.item_name,
        price_ref = body.price_ref,
        price_min = body.price_min,
        price_max = body.price_max,
        unit      = body.unit,
        category  = body.category,
    )
    context_manager.save()
    return mp.to_dict()


@app.get("/context/historical/{item_name}")
def get_historical_price(item_name: str):
    h = context_manager.get_historical_ref(item_name)
    if not h:
        raise HTTPException(404, f"Исторических данных по '{item_name}' нет")
    return h


# ─────────────────────────────────────────────────────────────────
# GRAPH
# ─────────────────────────────────────────────────────────────────

@app.get("/graph")
def get_graph(min_score: int = 0, types: str = None):
    if not _stored_graph:
        raise HTTPException(404, "Граф не построен. Сначала запустите POST /analyze")
    if min_score == 0 and not types:
        return _stored_graph
    G          = build_graph_from_aggregated(_stored_results)
    node_types = [t.strip() for t in types.split(",")] if types else None
    return export_json(G, min_score=min_score, node_types=node_types, include_stats=True)


@app.get("/graph/communities")
def get_graph_communities():
    """Community detection (Greedy Modularity) — кластеры поставщиков/позиций."""
    if not _stored_results:
        raise HTTPException(404, "Нет данных")
    try:
        import networkx.algorithms.community as nx_comm
        G  = build_graph_from_aggregated(_stored_results)
        Gu = G.to_undirected()
        if len(Gu.nodes) < 3:
            return {"communities": [], "count": 0}
        communities = list(nx_comm.greedy_modularity_communities(Gu))
        result = [
            {
                "community_id": i,
                "size":         len(comm),
                "nodes":        list(comm)[:50],
                "node_types":   _count_node_types(G, list(comm)),
            }
            for i, comm in enumerate(communities)
        ]
        return {
            "communities": result,
            "count":       len(result),
            "total_nodes": len(Gu.nodes),
        }
    except Exception as e:
        raise HTTPException(500, f"Community detection failed: {e}")


@app.get("/graph/path")
def get_graph_path(from_node: str, to_node: str):
    """
    Кратчайший путь между двумя узлами.
    Пример: ?from_node=item:Цемент М400&to_node=contractor:ООО Ромашка
    """
    if not _stored_results:
        raise HTTPException(404, "Нет данных")
    try:
        import networkx as nx
        G  = build_graph_from_aggregated(_stored_results)
        Gu = G.to_undirected()
        if from_node not in Gu or to_node not in Gu:
            raise HTTPException(404, f"Узел не найден: {from_node!r} или {to_node!r}")
        path = nx.shortest_path(Gu, from_node, to_node)
        return {
            "path":   path,
            "length": len(path) - 1,
            "nodes":  [dict(Gu.nodes[n]) | {"id": n} for n in path],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Path finding failed: {e}")


# ─────────────────────────────────────────────────────────────────
# SUPPLIERS / PATTERNS / ANALYSIS / ENTITIES
# ─────────────────────────────────────────────────────────────────

@app.get("/suppliers")
def get_supplier_analysis():
    if not _stored_analysis:
        raise HTTPException(404, "Анализ не выполнен")
    sa = _stored_analysis.get("supplier_analysis")
    if not sa:
        raise HTTPException(404, "Данные поставщиков недоступны")
    return sa


@app.get("/patterns")
def get_patterns():
    if not _stored_analysis:
        raise HTTPException(404, "Анализ не выполнен")
    pa = _stored_analysis.get("pattern_analysis")
    if not pa:
        raise HTTPException(404, "Данные паттернов недоступны")
    return pa


@app.get("/analysis")
def get_full_analysis():
    if not _stored_analysis:
        raise HTTPException(404, "Анализ не выполнен")
    return {k: v for k, v in _stored_analysis.items() if k != "entity_registry"}


@app.get("/entities")
def get_entities():
    """Результаты entity resolution — canonical контрагенты и статистика dedup."""
    if not _stored_analysis or "entity_registry" not in _stored_analysis:
        raise HTTPException(404, "Entity resolution не выполнен")
    return _stored_analysis["entity_registry"]


# ─────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────

@app.get("/report/save")
def save_report():
    if not _stored_results:
        raise HTTPException(404, "Нет данных для отчёта.")
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future     = executor.submit(
                generate_report,
                results=_stored_results,
                source_files=_stored_filenames or None,
            )
            docx_bytes = future.result(timeout=30)
    except concurrent.futures.TimeoutError:
        raise HTTPException(504, "Генерация отчёта заняла слишком много времени.")
    except Exception as e:
        raise HTTPException(500, f"Ошибка генерации отчёта: {e}")

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
        raise HTTPException(404, "Нет данных для отчёта. Сначала запустите POST /analyze")
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


# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)