"""
Importer.py  —  Benchling ingestion pipeline  (FINAL)
======================================================
Confirmed schema types from Benchling API errors + ERD:
  ts_JB4gsaH8D4   = DNA Sequence schema  → create_dna_sequence() + bases field
  ts_bi9do6KL1Z   = Custom Entity (Sample) → create_custom_entity()
  assaysch_*      = Assay Result           → create_assay_results_bulk()
  consch_*        = Container              → create_container_direct()

Key rules:
  - DNA entity name  = Sample_Name (e.g. ADC-Sample-1), NOT Construct_Name
  - Sample name      = Sample_Name
  - Entity linked    = DNA sequence ID (dna_sequence_link field)
  - Inventory linked = sample entity ID (transfer)
  - Results linked   = sample entity ID (linked_sample field)
  - schemaId always set on assay results (required by Benchling)
  - Empty Excel rows dropped before processing
  - Folders reused if they already exist (no duplicates on re-run)
"""

import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests as _requests

from benchling_client import (
    create_assay_results_bulk,
    create_custom_entity,
    create_dna_sequence,
    create_entry,
    create_folder,
    get_result_table_id_from_entry,
    create_location,
    create_box,
    create_container_direct,
    transfer_into_container_direct,
    find_storage_by_name,
)
from config_loader import load_config

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("output.log", mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA IDs  — fallback when selected_schemas.json is missing
# These are confirmed from your Benchling tenant ERD + API responses
# ══════════════════════════════════════════════════════════════════════════════
_SCHEMAS = {
    "sample":         "ts_bi9do6KL1Z",        # Custom Entity — Sample
    "dna":            "ts_JB4gsaH8D4",        # DNA Sequence  — DNA_Sequence_POC
    "results":        "assaysch_cETPFdfLCJ",  # Assay Result  — Results-Demo
    "inventory":      "consch_Gt7eLA5MZd",    # Container     — SV Test Tubes
    "location":       "locsch_285RvBkf5p",
    "box":            "boxsch_uZ1ZkIuFY3",
    "entry_template": "tmpl_4fdaFvrFMZ",
}

# Fields that are set at runtime or represent folder structure — never sent as field values
_SKIP_FIELDS = {
    "entity linked", "entity_linked", "linked_sample",
    "cro", "folderid", "projectid",
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_data_file(override=None):
    if override and os.path.exists(override):
        return override
    env = os.getenv("HARMONIZED_FILE")
    if env and os.path.exists(env):
        return env
    for ext in [".xlsx", ".csv"]:
        p = os.path.join("uploads", f"harmonized_upload{ext}")
        if os.path.exists(p):
            return p
    return load_config().get("excel", {}).get("data_workbook", "")


def get_selected_folder(benchling_cfg):
    if os.path.exists("ai/selected_notebook.json"):
        with open("ai/selected_notebook.json") as f:
            nb = json.load(f)
        fid = nb.get("folder_id")
        if fid:
            logger.info(f"Destination: {nb.get('folder_name', fid)}")
            return fid
    env = os.getenv("SELECTED_FOLDER_ID")
    if env:
        return env
    fid = benchling_cfg.get("parent_folder_id")
    if fid:
        return fid
    raise ValueError("No destination folder — select a notebook in the UI.")


def load_approved_mapping():
    path = "ai/approved_mapping.json"
    if not os.path.exists(path):
        raise FileNotFoundError("No approved mapping. Complete the mapping step in the UI.")
    with open(path) as f:
        return json.load(f)


def resolve_schema(key):
    """Return schema ID: user selection (Step 3 UI) → hardcoded fallback."""
    sel_path = "ai/selected_schemas.json"
    if os.path.exists(sel_path):
        with open(sel_path) as f:
            sel = json.load(f)
        s = sel.get(key, {})
        sid = s.get("id") or s.get("schema", {}).get("id")
        if sid:
            return sid
    return _SCHEMAS[key]


def _api_creds():
    cfg = load_config()
    bc  = cfg["benchling"]
    raw = bc.get("api_key_env_var", "BENCHLING_API_KEY")
    key = os.getenv(raw, raw) if not raw.startswith("sk_") else raw
    return key, bc["base_url"]


def find_existing_folder(name, parent_id):
    """Prevent duplicate folder creation on re-runs."""
    try:
        key, base = _api_creds()
        r = _requests.get(f"{base}/folders", auth=(key, ""), timeout=15)
        if r.status_code == 200:
            for f in r.json().get("folders", []):
                if f.get("name") == name and f.get("parentFolderId") == parent_id:
                    return f["id"]
    except Exception as e:
        logger.warning(f"  Folder lookup error: {e}")
    return None


def get_project_id_for_folder(folder_id):
    """Return the projectId for a folder — required for assay results."""
    try:
        key, base = _api_creds()
        r = _requests.get(f"{base}/folders/{folder_id}", auth=(key, ""), timeout=15)
        if r.status_code == 200:
            return r.json().get("projectId")
    except Exception:
        pass
    return None


def _id(obj):
    """Extract id from SDK object or dict."""
    return getattr(obj, "id", None) or (obj.get("id") if isinstance(obj, dict) else None)


# ══════════════════════════════════════════════════════════════════════════════
# FIELD BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _coerce(raw, btype="text"):
    """Convert raw Excel value to the type Benchling expects."""
    if isinstance(raw, pd.Timestamp):
        return raw.date().isoformat()
    try:
        if pd.isna(raw):
            return None
    except Exception:
        pass
    if hasattr(raw, "item"):
        raw = raw.item()

    if btype == "text":
        return str(raw).strip()
    if btype in ("integer", "int"):
        try:
            return int(float(str(raw)))
        except (ValueError, TypeError):
            return None
    if btype == "float":
        try:
            return float(str(raw))
        except (ValueError, TypeError):
            return None
    if btype == "date":
        return str(raw).strip() if raw else None
    # fallback
    if isinstance(raw, float) and raw == int(raw):
        return int(raw)
    return raw


def build_fields(schema_key, mapping, row):
    """{ FieldName: {value: typed_value} } from approved_mapping + data row."""
    out = {}
    for entry in mapping.get(schema_key, []):
        bf    = entry.get("benchling_field", "")
        col   = entry.get("suggested_column") or entry.get("mapped")
        btype = entry.get("benchling_type", "text")

        if entry.get("status") == "ignored":
            continue
        if bf.lower().replace(" ", "_") in _SKIP_FIELDS or bf.lower() in _SKIP_FIELDS:
            continue
        if not col or col not in row.index:
            continue

        val = _coerce(row[col], btype)
        if val is None:
            continue
        if btype == "integer" and not isinstance(val, int):
            logger.warning(f"  Skip '{bf}': '{val}' not integer")
            continue
        if btype == "float" and not isinstance(val, (int, float)):
            logger.warning(f"  Skip '{bf}': '{val}' not float")
            continue

        out[bf] = {"value": val}
    return out


def pos_to_well(p):
    try:
        i = int(str(p).strip())
        return f"{chr(64 + ((i-1)//12 + 1))}{(i-1)%12 + 1}"
    except Exception:
        return str(p).strip()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(file_path=None, mapping_file_path=None):

    config        = load_config()
    benchling_cfg = config.get("benchling", {})

    mapping = load_approved_mapping()
    logger.info(f"Mapping loaded: {list(mapping.keys())}")

    # Resolve all schema IDs
    sample_schema    = resolve_schema("sample")
    dna_schema       = resolve_schema("dna")
    results_schema   = resolve_schema("results")
    container_schema = resolve_schema("inventory")
    location_schema  = resolve_schema("location")
    box_schema       = resolve_schema("box")
    entry_template   = _SCHEMAS["entry_template"]

    logger.info(f"Schemas — Sample:{sample_schema} | DNA:{dna_schema} | "
                f"Results:{results_schema} | Container:{container_schema}")

    # ── Load + clean data ─────────────────────────────────────────────────────
    data_file = get_data_file(mapping_file_path)
    if not data_file or not os.path.exists(data_file):
        raise FileNotFoundError(f"Data file not found: {data_file}")

    df = pd.read_excel(data_file) if data_file.endswith(".xlsx") else pd.read_csv(data_file)
    n  = len(df)
    df = df.dropna(how="all")
    if "CRO-Name" in df.columns:
        df = df[df["CRO-Name"].notna() & (df["CRO-Name"].astype(str).str.strip() != "")]
    if "Sample_ID" in df.columns:
        df = df[df["Sample_ID"].notna()]
    df = df.reset_index(drop=True)
    logger.info(f"Data: {n} rows → {len(df)} valid rows")

    parent_folder_id = get_selected_folder(benchling_cfg)
    unique_cros      = df["CRO-Name"].dropna().unique() if "CRO-Name" in df.columns else ["Default"]

    # ── 1. CRO folders ────────────────────────────────────────────────────────
    logger.info("Creating CRO folders...")
    folder_ids:  Dict[str, str] = {}
    project_ids: Dict[str, Optional[str]] = {}

    for cro in unique_cros:
        name     = str(cro).strip()
        existing = find_existing_folder(name, parent_folder_id)
        if existing:
            logger.info(f"  Reusing '{name}': {existing}")
            folder_ids[cro]  = existing
            # Fetch projectId for this folder — needed for assay results
            project_ids[cro] = get_project_id_for_folder(existing)
        else:
            obj = create_folder({"name": name, "parentFolderId": parent_folder_id})
            fid = _id(obj)
            pid = getattr(obj, "project_id", None) or (obj.get("projectId") if isinstance(obj, dict) else None)
            folder_ids[cro]  = fid
            project_ids[cro] = pid
            logger.info(f"  Created '{name}': {fid}")

    # ── 2. Notebook entries (one per CRO) ─────────────────────────────────────
    logger.info("Creating notebook entries...")
    entry_ids: Dict[str, str] = {}

    for cro in unique_cros:
        obj = create_entry({
            "entryTemplateId": entry_template,
            "folderId":        folder_ids.get(cro, parent_folder_id),
            "name":            str(cro).strip(),
        })
        eid = _id(obj)
        if not eid:
            raise RuntimeError(f"Entry creation failed for CRO: {cro}")
        entry_ids[cro] = eid
        logger.info(f"  Entry '{cro}': {eid}")

    # ── 3. DNA sequences ──────────────────────────────────────────────────────
    # ts_JB4gsaH8D4 IS a DNA Sequence schema (confirmed by Benchling API).
    # Name = Sample_Name (ADC-Sample-1) so it appears correctly in the folder.
    # Construct_Name (mAb-Construct) is stored only as a field value.
    logger.info("Creating DNA sequences...")
    created_dna: List[Tuple[str, int, str]] = []

    for idx, row in df.iterrows():
        cro  = str(row.get("CRO-Name", "Default")).strip() if "CRO-Name" in df.columns else "Default"
        # Name = Sample_Name so Benchling shows "ADC-Sample-1", not "mAb-Construct"
        name = str(row.get("Sample_Name") or row.get("Sample_ID") or f"DNA-{idx}").strip()
        bases = str(row["Sequence"]) if "Sequence" in row.index and pd.notna(row.get("Sequence")) else ""

        fields = build_fields("DNA Sequence", mapping, row)

        payload = {
            "name":       name,
            "bases":      bases,
            "isCircular": False,
            "schemaId":   dna_schema,
            "folderId":   folder_ids.get(cro, parent_folder_id),
            "fields":     fields,
        }
        if cro in entry_ids:
            payload["entryId"] = entry_ids[cro]

        try:
            logger.info(f"  DNA: {name}")
            obj    = create_dna_sequence(payload)
            dna_id = _id(obj)
            created_dna.append((dna_id, int(idx), cro))
            logger.info(f"  DNA created: {dna_id}")
        except Exception as e:
            logger.error(f"  DNA error row {idx}: {e}")
            raise

    # ── 4. Samples ────────────────────────────────────────────────────────────
    logger.info("Creating samples...")
    created_entities: List[Tuple[str, int, str]] = []

    for dna_id, idx, cro in created_dna:
        row    = df.iloc[idx]
        name   = str(row.get("Sample_Name") or row.get("Sample_ID") or f"Sample-{idx}").strip()
        fields = build_fields("Sample", mapping, row)

        # Link DNA sequence to this sample
        if dna_id:
            fields["Entity linked"] = {"value": dna_id}
            logger.info(f"  Entity linked: {name} → {dna_id}")

        payload = {
            "name":     name,
            "schemaId": sample_schema,
            "folderId": folder_ids.get(cro, parent_folder_id),
            "fields":   fields,
        }
        if cro in entry_ids:
            payload["entryId"] = entry_ids[cro]

        try:
            logger.info(f"  Sample: {name}")
            obj       = create_custom_entity(payload)
            entity_id = _id(obj)
            created_entities.append((entity_id, idx, cro))
            logger.info(f"  Sample created: {entity_id}")
        except Exception as e:
            logger.error(f"  Sample error row {idx}: {e}")
            raise

    # ── 5. Inventory ──────────────────────────────────────────────────────────
    logger.info("Creating inventory...")
    storage_cache: Dict[str, str] = {}

    for entity_id, idx, cro in created_entities:
        row          = df.iloc[idx]
        loc_name     = str(row.get("Storage_Location", "")).strip()
        box_name     = str(row.get("Box", "")).strip()
        position     = str(row.get("Position", "A1")).strip()
        barcode      = str(row.get("Sample_ID", "")).strip()
        cont_name    = str(row.get("Sample_Name", barcode)).strip()
        qty          = row.get("Quantity_mg",  0.0)
        conc         = row.get("Concentration", 0.0)
        cond         = str(row.get("Storage_Condition", "")).strip()

        if not loc_name or not box_name:
            logger.warning(f"  Row {idx}: no Storage_Location/Box — skip inventory")
            continue

        # Location
        loc_key = f"loc_{loc_name}"
        if loc_key not in storage_cache:
            loc_id = find_storage_by_name(loc_name, location_schema)
            if not loc_id:
                obj    = create_location({"name": loc_name, "schemaId": location_schema})
                loc_id = _id(obj)
                logger.info(f"  Created location: {loc_name} ({loc_id})")
            else:
                logger.info(f"  Reusing location: {loc_name} ({loc_id})")
            storage_cache[loc_key] = loc_id
        location_id = storage_cache[loc_key]

        # Box
        box_key = f"box_{box_name}_{location_id}"
        if box_key not in storage_cache:
            box_id = find_storage_by_name(box_name, box_schema)
            if not box_id:
                obj    = create_box({"name": box_name, "schemaId": box_schema, "parentStorageId": location_id})
                box_id = _id(obj)
                logger.info(f"  Created box: {box_name} ({box_id})")
            else:
                logger.info(f"  Reusing box: {box_name} ({box_id})")
            storage_cache[box_key] = box_id
        box_id = storage_cache[box_key]

        # Container — barcode = Sample_ID, entity transferred in
        well = pos_to_well(position)
        try:
            cont_result = create_container_direct({
                "name":            cont_name,
                "barcode":         barcode,
                "schemaId":        container_schema,
                "parentStorageId": f"{box_id}:{well}",
                "fields": {
                    "Quantity_mg":       {"value": float(qty)  if qty  else 0.0},
                    "Concentration":     {"value": float(conc) if conc else 0.0},
                    "Storage_Condition": {"value": cond},
                },
            })
            cont_id = _id(cont_result) or cont_result.get("id")
            logger.info(f"  Container: {cont_id} barcode:{barcode} {box_name}:{well}")

            transfer_into_container_direct(cont_id, {
                "contents": [{
                    "entityId":      entity_id,
                    "concentration": {"value": float(conc) if conc else 0.0, "units": "mg/mL"},
                }]
            })
            logger.info(f"  Transferred sample {entity_id} → {cont_id}")

        except Exception as e:
            logger.warning(f"  Inventory error row {idx}: {e}")

    # ── 6. Assay results ──────────────────────────────────────────────────────
    logger.info("Uploading assay results...")

    cro_entities: Dict[str, list] = {}
    for entity_id, idx, cro in created_entities:
        cro_entities.setdefault(cro, []).append((entity_id, idx))

    for cro, entity_list in cro_entities.items():
        rows_results = []

        for entity_id, idx in entity_list:
            row    = df.iloc[idx]
            fields = build_fields("Results", mapping, row)
            fields = {k.lower(): v for k, v in fields.items()}
            fields["linked_sample"] = {"value": entity_id}

            rows_results.append({
                "schemaId": results_schema,   # REQUIRED by Benchling
                "fields":   fields,
                **({"projectId": project_ids[cro]} if project_ids.get(cro) else {}),
            })

        if not rows_results:
            logger.warning(f"  No results for '{cro}'")
            continue

        # Get tableId from entry for notebook display
        table_id = None
        if cro in entry_ids:
            try:
                table_id = get_result_table_id_from_entry(entry_ids[cro])
            except Exception:
                pass

        payload: Dict[str, Any] = {"assayResults": rows_results}
        if table_id:
            payload["tableId"] = table_id

        try:
            resp = create_assay_results_bulk(payload)
            logger.info(f"  Results for '{cro}': {resp}")
        except Exception as e:
            logger.error(f"  Results error '{cro}': {e}")
            raise

    logger.info("Pipeline complete!")


if __name__ == "__main__":
    main()