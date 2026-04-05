"""
app/main.py
FastAPI backend — fully dynamic file handling + notebook selection.
Only shows lib_ folders as valid ingestion targets.
"""

from fastapi import FastAPI, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import uvicorn
import shutil
import os
import json
import asyncio
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Benchling Data Importer",
    description="AI-assisted ERD-driven ingestion — dynamic file + notebook selection",
    version="3.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:5174",
        os.getenv("ALLOWED_ORIGINS", "*")
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs("reports", exist_ok=True)
os.makedirs("ai", exist_ok=True)


# ─── Core helpers ─────────────────────────────────────────────────────────────

def get_upload_path() -> str | None:
    """Find the file uploaded by the user via the UI."""
    for ext in [".xlsx", ".csv"]:
        p = os.path.join(UPLOAD_DIR, f"harmonized_upload{ext}")
        if os.path.exists(p):
            return p
    return None


def set_active_file(path: str):
    """Tell all AI modules which file to use."""
    os.environ["HARMONIZED_FILE"] = path


def get_selected_notebook() -> dict:
    """Get the notebook/folder selected by the user."""
    if os.path.exists("ai/selected_notebook.json"):
        with open("ai/selected_notebook.json") as f:
            return json.load(f)
    return {}


# ─── ROUTE 1: Health check ────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    upload   = get_upload_path()
    notebook = get_selected_notebook()
    return {
        "status":            "ok",
        "time":              datetime.now().isoformat(),
        "benchling_key":     bool(os.getenv("BENCHLING_API_KEY")),
        "anthropic_key":     bool(os.getenv("ANTHROPIC_API_KEY")),
        "erd_cached":        os.path.exists("ai/benchling_erd.json"),
        "file_uploaded":     upload is not None,
        "active_file":       os.path.basename(upload) if upload else None,
        "notebook_selected": bool(notebook),
        "notebook_name":     notebook.get("folder_name", None),
    }


# ─── ROUTE 2: Fetch notebooks from Benchling ─────────────────────────────────

@app.get("/api/notebooks")
def get_notebooks():
    """
    Fetch all folders from Benchling that are valid ingestion targets.
    Only returns folders with lib_ prefix — these are the only valid
    parentFolderId values accepted by Benchling API.
    Projects (src_/prj_ IDs) are shown as context only, not selectable.
    """
    try:
        import requests
        api_key  = os.getenv("BENCHLING_API_KEY")
        base_url = "https://excelra.benchling.com/api/v2"

        if not api_key:
            return JSONResponse(status_code=400,
                content={"error": "BENCHLING_API_KEY not set in .env"})

        # Step 1: Fetch projects for display names
        project_map = {}
        resp_proj = requests.get(
            f"{base_url}/projects",
            auth=(api_key, ""),
            timeout=30
        )
        if resp_proj.status_code == 200:
            for p in resp_proj.json().get("projects", []):
                project_map[p.get("id", "")] = p.get("name", "")

        # Step 2: Fetch folders — only lib_ ones are valid targets
        folders = []
        resp_fold = requests.get(
            f"{base_url}/folders",
            auth=(api_key, ""),
            timeout=30
        )
        if resp_fold.status_code == 200:
            for f in resp_fold.json().get("folders", []):
                folder_id = f.get("id", "")

                # Only include folders with lib_ prefix
                if not folder_id.startswith("lib_"):
                    continue

                proj_id   = f.get("projectId", "")
                proj_name = project_map.get(proj_id, "")

                folders.append({
                    "id":           folder_id,
                    "name":         f.get("name", ""),
                    "type":         "Folder",
                    "projectId":    proj_id,
                    "project_name": proj_name,
                    # Display as "Project / Folder" for easy identification
                    "display_name": (
                        f"{proj_name} / {f.get('name', '')}"
                        if proj_name else f.get("name", "")
                    ),
                })

        return {
            "status":    "ok",
            "notebooks": [],        # Projects not selectable — folders only
            "folders":   folders,
            "total":     len(folders),
        }

    except Exception as e:
        return JSONResponse(status_code=500,
            content={"error": f"Could not fetch notebooks: {str(e)}"})


# ─── ROUTE 3: Save selected notebook ─────────────────────────────────────────

@app.post("/api/notebook/select")
async def select_notebook(payload: dict):
    """Save the user's selected folder for this ingestion run."""
    folder_id   = payload.get("folder_id")
    folder_name = payload.get("folder_name")

    if not folder_id:
        return JSONResponse(status_code=400,
            content={"error": "folder_id is required"})

    if not folder_id.startswith("lib_"):
        return JSONResponse(status_code=400,
            content={"error": f"Invalid folder ID '{folder_id}'. Must start with lib_"})

    os.environ["SELECTED_FOLDER_ID"]   = folder_id
    os.environ["SELECTED_FOLDER_NAME"] = folder_name or ""

    with open("ai/selected_notebook.json", "w") as f:
        json.dump({
            "folder_id":   folder_id,
            "folder_name": folder_name,
            "selected_at": datetime.now().isoformat(),
        }, f, indent=2)

    return {"status": "ok", "folder_id": folder_id, "folder_name": folder_name}


# ─── ROUTE 4: Upload data file ────────────────────────────────────────────────

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Accept any data file uploaded by the user (xlsx or csv).
    Reads columns dynamically — no assumed structure.
    """
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in [".xlsx", ".csv"]:
        return JSONResponse(status_code=400,
            content={"error": "Only .xlsx and .csv files are supported"})

    dest = os.path.join(UPLOAD_DIR, f"harmonized_upload{ext}")
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)

    set_active_file(dest)

    try:
        import pandas as pd
        df = pd.read_excel(dest) if ext == ".xlsx" else pd.read_csv(dest)

        col_types = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
            col_types[col] = (
                "numeric" if "int" in dtype or "float" in dtype else
                "date"    if "datetime" in dtype else
                "text"
            )

        return {
            "status":    "uploaded",
            "filename":  file.filename,
            "rows":      len(df),
            "columns":   df.columns.tolist(),
            "col_types": col_types,
            "preview":   df.head(3).fillna("").to_dict(orient="records"),
            "path":      dest,
        }

    except Exception as e:
        return JSONResponse(status_code=500,
            content={"error": f"Could not read file: {str(e)}"})


# ─── ROUTE 5: Fetch ERD ───────────────────────────────────────────────────────

@app.get("/api/erd")
def get_erd(refresh: bool = False):
    """Return cached ERD or fetch fresh from Benchling."""
    erd_path = "ai/benchling_erd.json"

    if refresh or not os.path.exists(erd_path):
        try:
            from ai.schema_fetcher import fetch_and_build_erd
            erd = fetch_and_build_erd()
            return {
                "status":       "fetched",
                "schema_count": erd.get("schema_count", 0),
                "schemas":      erd.get("schemas", []),
                "generated_at": erd.get("generated_at", ""),
            }
        except Exception as e:
            return JSONResponse(status_code=500,
                content={"error": f"ERD fetch failed: {str(e)}"})

    with open(erd_path) as f:
        erd = json.load(f)
    return {
        "status":       "cached",
        "schema_count": erd.get("schema_count", 0),
        "schemas":      erd.get("schemas", []),
        "generated_at": erd.get("generated_at", ""),
    }


# ─── ROUTE 6: AI mapping ──────────────────────────────────────────────────────

@app.post("/api/mapping")
async def generate_mapping():
    """
    Dynamically maps uploaded file columns to live Benchling ERD fields.
    Works with any file structure.
    """
    upload = get_upload_path()
    if not upload:
        return JSONResponse(status_code=400,
            content={"error": "No file uploaded. Please upload your data file first."})

    set_active_file(upload)

    try:
        import pandas as pd
        df = pd.read_excel(upload) if upload.endswith(".xlsx") else pd.read_csv(upload)
        uploaded_cols = df.columns.tolist()

        erd = {}
        if os.path.exists("ai/benchling_erd.json"):
            with open("ai/benchling_erd.json") as f:
                erd = json.load(f)

        from ai.mapping_assistant import (
            mock_claude_suggest, SCHEMAS, SCHEMA_TO_BENCHLING
        )

        result = {}
        for schema in SCHEMAS:
            benchling_name = SCHEMA_TO_BENCHLING.get(schema)
            if not benchling_name:
                continue

            benchling_fields = {}
            for s in erd.get("schemas", []):
                if s["name"] == benchling_name:
                    benchling_fields = {
                        f["name"]: {
                            "type":     f.get("type", "unknown"),
                            "required": f.get("required", False),
                        }
                        for f in s.get("fields", [])
                        if not f.get("archived", False)
                    }
                    break

            if not benchling_fields:
                continue

            suggestions = mock_claude_suggest(
                list(benchling_fields.keys()), uploaded_cols, schema
            )

            for s in suggestions:
                fd = benchling_fields.get(s["benchling_field"], {})
                s["benchling_type"]     = fd.get("type", "unknown")
                s["benchling_required"] = fd.get("required", False)

            result[schema] = suggestions

        return {
            "status":             "ok",
            "mapping":            result,
            "harmonized_columns": uploaded_cols,
            "file_used":          os.path.basename(upload),
            "total_rows":         len(df),
        }

    except Exception as e:
        return JSONResponse(status_code=500,
            content={"error": f"Mapping failed: {str(e)}"})


# ─── ROUTE 7: Save approved mapping ──────────────────────────────────────────

@app.post("/api/mapping/approve")
async def approve_mapping(payload: dict):
    """Save the user-confirmed mapping."""
    try:
        with open("ai/approved_mapping.json", "w") as f:
            json.dump(payload.get("mapping", {}), f, indent=2)
        return {"status": "saved", "path": "ai/approved_mapping.json"}
    except Exception as e:
        return JSONResponse(status_code=500,
            content={"error": f"Could not save mapping: {str(e)}"})


# ─── ROUTE 8: Validate data ───────────────────────────────────────────────────

@app.post("/api/validate")
async def validate_data():
    """
    Validates the uploaded data file using the approved mapping.
    Fully dynamic — checks only columns present in the file.
    """
    upload = get_upload_path()
    if not upload:
        return JSONResponse(status_code=400,
            content={"error": "No file uploaded."})

    set_active_file(upload)

    try:
        import pandas as pd
        df   = pd.read_excel(upload) if upload.endswith(".xlsx") else pd.read_csv(upload)
        cols = set(df.columns.tolist())

        mapping = {}
        if os.path.exists("ai/approved_mapping.json"):
            with open("ai/approved_mapping.json") as f:
                mapping = json.load(f)

        results = {}

        for schema, fields in mapping.items():
            issues      = []
            warnings    = []
            ai_insights = []

            mapped_cols = [
                f.get("suggested_column") or f.get("mapped")
                for f in fields
                if f.get("suggested_column") or f.get("mapped")
            ]
            mapped_cols = [c for c in mapped_cols if c]

            # Check 1: mapped columns exist
            for col in mapped_cols:
                if col not in cols:
                    issues.append(
                        f"Column '{col}' mapped for {schema} "
                        f"but not found in uploaded file"
                    )

            # Check 2: null values
            for col in mapped_cols:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    if null_count > 0:
                        warnings.append(
                            f"'{col}': {null_count} empty values "
                            f"({null_count/len(df)*100:.0f}% of rows)"
                        )

            # Check 3: duplicates
            id_cols = [c for c in mapped_cols if c and
                       any(x in c.lower() for x in ["id","name","construct","batch"])]
            for col in id_cols:
                if col in df.columns:
                    dupes = df[col].duplicated().sum()
                    if dupes > 0:
                        warnings.append(f"'{col}': {dupes} duplicate values detected")

            # Check 4: date logic
            date_cols = [c for c in df.columns if "date" in c.lower()]
            mfg_col = next((c for c in date_cols if "manuf" in c.lower()), None)
            exp_col = next((c for c in date_cols if "expir" in c.lower()), None)
            if mfg_col and exp_col:
                try:
                    mfg = pd.to_datetime(df[mfg_col], errors="coerce")
                    exp = pd.to_datetime(df[exp_col],  errors="coerce")
                    bad = (exp < mfg).sum()
                    if bad > 0:
                        issues.append(f"{bad} rows have {exp_col} before {mfg_col}")
                    expired = (exp < pd.Timestamp.now()).sum()
                    if expired > 0:
                        warnings.append(f"{expired} rows have {exp_col} in the past")
                except Exception:
                    pass

            # Check 5: numeric columns
            num_cols = [c for c in mapped_cols if c and c in df.columns and
                        any(x in c.lower() for x in
                            ["quantity","concentration","purity","dar",
                             "weight","length","gc","result"])]
            for col in num_cols:
                non_num = pd.to_numeric(df[col], errors="coerce").isna().sum()
                if non_num > 0:
                    issues.append(f"'{col}': {non_num} non-numeric values")

            # Check 6: sequence length mismatch
            seq_col = next((c for c in df.columns if c.lower() == "sequence"), None)
            len_col = next((c for c in df.columns
                            if "sequence_length" in c.lower()), None)
            if seq_col and len_col:
                try:
                    actual   = df[seq_col].dropna().apply(len)
                    stated   = pd.to_numeric(df[len_col], errors="coerce")
                    mismatch = (actual != stated).sum()
                    if mismatch > 0:
                        ai_insights.append({
                            "severity":      "error",
                            "column":        len_col,
                            "issue":         f"{mismatch} rows where {len_col} "
                                             f"doesn't match actual sequence length",
                            "affected_rows": int(mismatch),
                            "suggestion":    f"Recalculate {len_col} from {seq_col}",
                        })
                except Exception:
                    pass

            results[schema] = {
                "passed":      len(issues) == 0,
                "issues":      issues,
                "warnings":    warnings,
                "ai_insights": ai_insights,
                "mapped_cols": mapped_cols,
            }

        return {
            "status":         "ok",
            "overall_passed": all(r["passed"] for r in results.values()),
            "results":        results,
            "file_used":      os.path.basename(upload),
            "total_rows":     len(df),
            "total_columns":  len(df.columns),
        }

    except Exception as e:
        return JSONResponse(status_code=500,
            content={"error": f"Validation failed: {str(e)}"})


# ─── ROUTE 9: WebSocket live ingestion ────────────────────────────────────────

@app.websocket("/ws/ingest")
async def ingest_websocket(ws: WebSocket):
    """
    Live ingestion using the uploaded file and selected notebook.
    Streams real-time logs to the UI via WebSocket.
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
        upload   = get_upload_path()
        notebook = get_selected_notebook()

        if not upload:
            await send("No file uploaded — please upload your data file first",
                       "ERROR", "error")
            await ws.send_json({"status":"complete","success":False,
                                "error":"No file uploaded"})
            return

        if not notebook:
            await send("No notebook selected — please select a destination folder",
                       "ERROR", "error")
            await ws.send_json({"status":"complete","success":False,
                                "error":"No notebook selected"})
            return

        folder_id = notebook.get("folder_id", "")
        if not folder_id.startswith("lib_"):
            await send(
                f"Invalid folder ID '{folder_id}' — must be a lib_ folder, not a project",
                "ERROR", "error"
            )
            await ws.send_json({"status":"complete","success":False,
                                "error":"Invalid folder ID"})
            return

        await send(f"File: {os.path.basename(upload)}", "FILE")
        await asyncio.sleep(0.2)
        await send(f"Destination: {notebook.get('folder_name','Unknown')}", "FOLDER")
        await asyncio.sleep(0.2)
        await send("Connecting to Benchling API...", "AUTH")
        await asyncio.sleep(0.3)
        await send("Loading ERD — schemas confirmed", "ERD")
        await asyncio.sleep(0.2)

        log_messages = []
        loop = asyncio.get_event_loop()

        def run_pipeline():
            import logging

            set_active_file(upload)
            os.environ["SELECTED_FOLDER_ID"]   = notebook.get("folder_id", "")
            os.environ["SELECTED_FOLDER_NAME"] = notebook.get("folder_name", "")

            class WSHandler(logging.Handler):
                def emit(self, record):
                    log_messages.append(record.getMessage())

            from Importer import main as importer_main
            from ai.error_handler import handle_benchling_error

            root_logger = logging.getLogger()
            handler = WSHandler()
            root_logger.addHandler(handler)

            try:
                importer_main(mapping_file_path=upload)
                return {"success": True}
            except Exception as e:
                result = handle_benchling_error(e, {}, "Pipeline")
                return {
                    "success":     False,
                    "error":       str(e),
                    "explanation": result.get("explanation", {}),
                }
            finally:
                root_logger.removeHandler(handler)

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future   = loop.run_in_executor(pool, run_pipeline)
            last_idx = 0

            while not future.done():
                while last_idx < len(log_messages):
                    msg = log_messages[last_idx]
                    tag = (
                        "DNA"    if "DNA"       in msg or "dna"    in msg.lower() else
                        "SAMPLE" if "Sample"    in msg or "custom" in msg.lower() else
                        "FOLDER" if "folder"    in msg.lower()                    else
                        "ENTRY"  if "Entry"     in msg                            else
                        "INV"    if "Container" in msg or "Box"    in msg         else
                        "RESULT" if "result"    in msg.lower()                    else
                        "INFO"
                    )
                    await send(msg, tag)
                    last_idx += 1
                await asyncio.sleep(0.25)

            while last_idx < len(log_messages):
                await send(log_messages[last_idx], "INFO")
                last_idx += 1

            result = await future

        if result["success"]:
            await send("Pipeline complete — all records created", "DONE", "done")
            await ws.send_json({"status":"complete","success":True})
        else:
            exp = result.get("explanation", {})
            await send(f"Error: {exp.get('title','Unknown error')}", "ERROR", "error")
            await send(f"Fix: {exp.get('fix','Check error log')}", "ERROR", "error")
            await ws.send_json({"status":"complete","success":False,
                                "error": result.get("error")})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await ws.send_json({"status":"error","msg":str(e)})
        except Exception:
            pass


# ─── ROUTE 10: Latest report ──────────────────────────────────────────────────

@app.get("/api/report/latest")
def get_latest_report():
    try:
        reports = sorted([
            f for f in os.listdir("reports")
            if f.startswith("pipeline_run_")
        ], reverse=True)
    except Exception:
        reports = []

    if not reports:
        return JSONResponse(status_code=404,
            content={"error": "No reports found. Run the pipeline first."})

    path = os.path.join("reports", reports[0])
    with open(path, encoding="utf-8") as f:
        content = f.read()
    return {"filename": reports[0], "content": content}


# ─── ROUTE 11: Download mapping ───────────────────────────────────────────────

@app.get("/api/mapping/download")
def download_mapping():
    path = "ai/approved_mapping.json"
    if not os.path.exists(path):
        return JSONResponse(status_code=404,
            content={"error": "No approved mapping found."})
    return FileResponse(path, filename="approved_mapping.json",
                        media_type="application/json")


# ─── ROUTE 12: Download report file ──────────────────────────────────────────

@app.get("/api/report/download/{filename}")
def download_report(filename: str):
    path = os.path.join("reports", filename)
    if not os.path.exists(path):
        return JSONResponse(status_code=404,
            content={"error": "Report file not found."})
    return FileResponse(path, filename=filename)


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", 8000)),
        reload=True,
    )