"""
app/main.py
FastAPI backend for Benchling Data Importer.
Connects the React UI to all AI pipeline modules.
"""

from fastapi import FastAPI, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import uvicorn
import shutil
import os
import json
import asyncio
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─── App setup ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Benchling Data Importer",
    description="AI-assisted ERD-driven data ingestion pipeline",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173",
                   os.getenv("ALLOWED_ORIGINS","*")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs("reports", exist_ok=True)
os.makedirs("ai", exist_ok=True)


# ─── Health check ─────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "time":   datetime.now().isoformat(),
        "benchling_key": bool(os.getenv("BENCHLING_API_KEY")),
        "anthropic_key": bool(os.getenv("ANTHROPIC_API_KEY")),
        "erd_cached":    os.path.exists("ai/benchling_erd.json"),
    }


# ─── ROUTE 1: Upload harmonized file ─────────────────────────────────────────

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Accept uploaded harmonized dataset (xlsx or csv).
    Saves to uploads/ and returns column list.
    """
    if not file.filename.endswith((".xlsx", ".csv")):
        return JSONResponse(status_code=400,
            content={"error": "Only .xlsx and .csv files are supported"})

    dest = os.path.join(UPLOAD_DIR, "harmonized_upload" +
                        os.path.splitext(file.filename)[1])
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # Read column names
    try:
        import pandas as pd
        if dest.endswith(".xlsx"):
            df = pd.read_excel(dest, nrows=3)
        else:
            df = pd.read_csv(dest, nrows=3)

        return {
            "status":   "uploaded",
            "filename": file.filename,
            "rows":     int(len(pd.read_excel(dest) if dest.endswith(".xlsx")
                             else pd.read_csv(dest))),
            "columns":  df.columns.tolist(),
            "preview":  df.head(3).to_dict(orient="records"),
            "path":     dest,
        }
    except Exception as e:
        return JSONResponse(status_code=500,
            content={"error": f"Could not read file: {str(e)}"})


# ─── ROUTE 2: Fetch ERD from Benchling ───────────────────────────────────────

@app.get("/api/erd")
def get_erd(refresh: bool = False):
    """
    Return cached ERD or fetch fresh from Benchling.
    Pass ?refresh=true to force a new fetch.
    """
    erd_path = "ai/benchling_erd.json"

    if refresh or not os.path.exists(erd_path):
        try:
            from ai.schema_fetcher import fetch_and_build_erd
            erd = fetch_and_build_erd()
            return {"status": "fetched", "schema_count": erd.get("schema_count", 0),
                    "schemas": erd.get("schemas", []),
                    "generated_at": erd.get("generated_at", "")}
        except Exception as e:
            return JSONResponse(status_code=500,
                content={"error": f"ERD fetch failed: {str(e)}"})

    with open(erd_path) as f:
        erd = json.load(f)
    return {"status": "cached", "schema_count": erd.get("schema_count", 0),
            "schemas": erd.get("schemas", []),
            "generated_at": erd.get("generated_at", "")}


# ─── ROUTE 3: Generate AI mapping ────────────────────────────────────────────

@app.post("/api/mapping")
async def generate_mapping():
    """
    Run the mapping assistant against the uploaded file + live ERD.
    Returns per-schema field mapping suggestions with confidence scores.
    """
    upload = _find_upload()
    if not upload:
        return JSONResponse(status_code=400,
            content={"error": "No uploaded file found. Upload a file first."})

    try:
        import pandas as pd
        from ai.mapping_assistant import (
            load_erd, get_benchling_fields,
            mock_claude_suggest, get_harmonized_columns,
            SCHEMAS, SCHEMA_TO_BENCHLING
        )

        erd = load_erd()
        df  = pd.read_excel(upload) if upload.endswith(".xlsx") \
              else pd.read_csv(upload)
        harmonized_cols = df.columns.tolist()

        result = {}
        for schema in SCHEMAS:
            benchling_name = SCHEMA_TO_BENCHLING.get(schema)
            if not benchling_name:
                continue
            benchling_fields_def = get_benchling_fields(schema, erd)
            if not benchling_fields_def:
                continue
            input_fields = list(benchling_fields_def.keys())
            suggestions  = mock_claude_suggest(input_fields, harmonized_cols, schema)
            # Enrich with ERD type info
            for s in suggestions:
                fd = benchling_fields_def.get(s["benchling_field"], {})
                s["benchling_type"]     = fd.get("type", "unknown")
                s["benchling_required"] = fd.get("required", False)
                s["benchling_id"]       = SCHEMA_TO_BENCHLING.get(schema, "")
            result[schema] = suggestions

        return {"status": "ok", "mapping": result,
                "harmonized_columns": harmonized_cols}

    except Exception as e:
        return JSONResponse(status_code=500,
            content={"error": f"Mapping failed: {str(e)}"})


# ─── ROUTE 4: Save approved mapping ──────────────────────────────────────────

@app.post("/api/mapping/approve")
async def approve_mapping(payload: dict):
    """
    Save user-confirmed mapping to ai/approved_mapping.json.
    Accepts the full mapping dict from the UI after user review.
    """
    try:
        with open("ai/approved_mapping.json", "w") as f:
            json.dump(payload.get("mapping", {}), f, indent=2)
        return {"status": "saved", "path": "ai/approved_mapping.json"}
    except Exception as e:
        return JSONResponse(status_code=500,
            content={"error": f"Could not save mapping: {str(e)}"})


# ─── ROUTE 5: Validate data ───────────────────────────────────────────────────

@app.post("/api/validate")
async def validate_data():
    """
    Run data validator against the uploaded harmonized file.
    Returns per-schema validation results.
    """
    upload = _find_upload()
    if not upload:
        return JSONResponse(status_code=400,
            content={"error": "No uploaded file found."})

    # Temporarily point validator at uploaded file
    _patch_env("HARMONIZED_FILE", upload)

    try:
        from ai.validator import SCHEMA_RULES, load_harmonized_data, \
            run_rule_based_checks, mock_claude_validate
        import pandas as pd

        df = pd.read_excel(upload) if upload.endswith(".xlsx") \
             else pd.read_csv(upload)

        results = {}
        for schema in SCHEMA_RULES.keys():
            issues, warnings = run_rule_based_checks(df, schema)
            ai_insights      = mock_claude_validate(df, schema)
            results[schema]  = {
                "passed":      len(issues) == 0,
                "issues":      issues,
                "warnings":    warnings,
                "ai_insights": ai_insights,
            }

        overall = all(r["passed"] for r in results.values())
        return {"status": "ok", "overall_passed": overall, "results": results}

    except Exception as e:
        return JSONResponse(status_code=500,
            content={"error": f"Validation failed: {str(e)}"})


# ─── ROUTE 6: WebSocket ingestion ────────────────────────────────────────────

@app.websocket("/ws/ingest")
async def ingest_websocket(ws: WebSocket):
    """
    WebSocket endpoint that streams live ingestion logs to the UI.
    Connect from frontend: new WebSocket('ws://localhost:8000/ws/ingest')
    """
    await ws.accept()

    async def send(msg: str, tag: str = "INFO", status: str = "running"):
        await ws.send_json({
            "msg":       msg,
            "tag":       tag,
            "status":    status,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })

    try:
        await send("Connecting to Benchling API...", "AUTH")
        await asyncio.sleep(0.5)

        await send("Loading ERD — schemas confirmed", "ERD")
        await asyncio.sleep(0.4)

        # Run the actual pipeline in a thread so it doesn't block
        import concurrent.futures
        loop = asyncio.get_event_loop()

        log_messages = []

        def run_pipeline():
            import logging
            class WSHandler(logging.Handler):
                def emit(self, record):
                    log_messages.append(record.getMessage())

            from Importer import main as importer_main
            from ai.error_handler import handle_benchling_error

            logger = logging.getLogger()
            handler = WSHandler()
            logger.addHandler(handler)
            try:
                importer_main()
                return {"success": True}
            except Exception as e:
                result = handle_benchling_error(e, {}, "Pipeline")
                return {"success": False, "error": str(e),
                        "explanation": result.get("explanation", {})}
            finally:
                logger.removeHandler(handler)

        # Stream log messages while pipeline runs
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = loop.run_in_executor(pool, run_pipeline)

            # Poll log messages and stream to UI while pipeline runs
            last_idx = 0
            while not future.done():
                while last_idx < len(log_messages):
                    msg = log_messages[last_idx]
                    tag = ("DNA"    if "DNA"       in msg else
                           "SAMPLE" if "Sample"    in msg else
                           "FOLDER" if "folder"    in msg else
                           "ENTRY"  if "Entry"     in msg else
                           "INV"    if "Container" in msg else
                           "RESULT" if "result"    in msg else "INFO")
                    await send(msg, tag)
                    last_idx += 1
                await asyncio.sleep(0.3)

            result = await future

        if result["success"]:
            await send("Pipeline complete — all records created", "DONE", "done")
            await ws.send_json({"status": "complete", "success": True})
        else:
            err = result.get("explanation", {})
            await send(f"Error: {err.get('title','Unknown error')}", "ERROR", "error")
            await send(f"Fix: {err.get('fix','Check error log')}", "ERROR", "error")
            await ws.send_json({"status": "complete", "success": False,
                                "error": result.get("error")})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await ws.send_json({"status": "error", "msg": str(e)})
        except:
            pass


# ─── ROUTE 7: Download report ─────────────────────────────────────────────────

@app.get("/api/report/latest")
def get_latest_report():
    """Return the most recent pipeline run report."""
    reports = sorted([
        f for f in os.listdir("reports")
        if f.startswith("pipeline_run_")
    ], reverse=True)
    if not reports:
        return JSONResponse(status_code=404,
            content={"error": "No reports found yet"})
    path = os.path.join("reports", reports[0])
    with open(path, encoding="utf-8") as f:
        content = f.read()
    return {"filename": reports[0], "content": content}


@app.get("/api/report/download/{filename}")
def download_report(filename: str):
    path = os.path.join("reports", filename)
    if not os.path.exists(path):
        return JSONResponse(status_code=404, content={"error": "File not found"})
    return FileResponse(path, filename=filename)


@app.get("/api/mapping/download")
def download_mapping():
    """Download the approved mapping as JSON."""
    path = "ai/approved_mapping.json"
    if not os.path.exists(path):
        return JSONResponse(status_code=404,
            content={"error": "No approved mapping found"})
    return FileResponse(path, filename="approved_mapping.json",
                        media_type="application/json")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _find_upload() -> str | None:
    """Find most recently uploaded file."""
    for ext in [".xlsx", ".csv"]:
        p = os.path.join(UPLOAD_DIR, f"harmonized_upload{ext}")
        if os.path.exists(p):
            return p
    return None

def _patch_env(key: str, val: str):
    os.environ[key] = val


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", 8000)),
        reload=True,
    )