"""
Importer.py  —  Benchling ingestion pipeline
=============================================
No CRO Mapping.xlsx. No hardcoded schema IDs.

ROOT CAUSES FIXED IN THIS VERSION:
  1. selected_schemas.json missing  → hardcoded fallback schema IDs from ERD
  2. mAb-Construct appearing as entity → DNA entity name = Sample_Name not Construct_Name
  3. DNA_Sequence_POC is Custom Entity → use create_custom_entity (not create_dna_sequence)
  4. schemaId required on assay results → always added from resolved results_schema_id
  5. Empty Excel rows creating nan/NaT entries → dropped before processing
  6. Duplicate folders on re-run → check existing before creating
  7. Type mismatch (text field sent as int) → _coerce respects benchling_type
  8. CRO integer field rejection → _SKIP_FIELDS list

SCHEMA IDs (from your Benchling ERD — fallback if selected_schemas.json missing):
  Sample entity  : ts_bi9do6KL1Z   (Custom Entity)
  DNA entity     : ts_JB4gsaH8D4   (Custom Entity — DNA_Sequence_POC)
  Results        : assaysch_cETPFdfLCJ  (Assay Result — Results-Demo)
  Container      : consch_Gt7eLA5MZd   (SV Test Tubes)
  Location       : locsch_285RvBkf5p
  Box            : boxsch_uZ1ZkIuFY3
"""

import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from benchling_client import (
    create_assay_results_bulk,
    create_custom_entity,
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
# FALLBACK SCHEMA IDs  (used if selected_schemas.json is missing)
# ══════════════════════════════════════════════════════════════════════════════
_FALLBACK_SCHEMAS = {
    "sample":         "ts_bi9do6KL1Z",
    "dna":            "ts_JB4gsaH8D4",
    "results":        "assaysch_cETPFdfLCJ",
    "inventory":      "consch_Gt7eLA5MZd",
    "location":       "locsch_285RvBkf5p",
    "box":            "boxsch_uZ1ZkIuFY3",
    "entry_template": "tmpl_4fdaFvrFMZ",
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
            logger.info(f"Destination folder: {nb.get('folder_name', fid)}")
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


def resolve_schema_id(key):
    """User selection (Step 3) → fallback hardcoded IDs."""
    sel_path = "ai/selected_schemas.json"
    if os.path.exists(sel_path):
        with open(sel_path) as f:
            sel = json.load(f)
        schema = sel.get(key, {})
        sid = schema.get("id") or schema.get("schema", {}).get("id")
        if sid:
            return sid
    sid = _FALLBACK_SCHEMAS.get(key)
    if sid:
        logger.info(f"  Fallback schema for '{key}': {sid}")
        return sid
    raise ValueError(f"No schema ID for '{key}'.")


def _get_api_creds():
    cfg = load_config()
    bc  = cfg.get("benchling", {})
    raw = bc.get("api_key_env_var", "BENCHLING_API_KEY")
    key = os.getenv(raw, raw) if not raw.startswith("sk_") else raw
    base = bc.get("base_url", "https://excelra.benchling.com/api/v2")
    return key, base


def find_existing_folder(name, parent_id):
    """Return existing folder ID to prevent duplicates on re-run."""
    try:
        key, base = _get_api_creds()
        resp = requests.get(f"{base}/folders", auth=(key, ""), timeout=15)
        if resp.status_code == 200:
            for f in resp.json().get("folders", []):
                if f.get("name") == name and f.get("parentFolderId") == parent_id:
                    return f.get("id")
    except Exception as e:
        logger.warning(f"  Folder check error: {e}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# FIELD BUILDER
# ══════════════════════════════════════════════════════════════════════════════

_SKIP_FIELDS = {
    "entity linked", "entity_linked", "linked_sample",  # runtime
    "cro", "folderid", "projectid",                      # structural
}


def _coerce(raw, benchling_type="text"):
    """Convert value to the type Benchling expects."""
    if isinstance(raw, pd.Timestamp):
        return raw.date().isoformat()
    try:
        if pd.isna(raw):
            return None
    except Exception:
        pass
    if hasattr(raw, "item"):
        raw = raw.item()

    if benchling_type == "text":
        return str(raw).strip()
    if benchling_type in ("integer", "int"):
        try:
            return int(float(str(raw)))
        except (ValueError, TypeError):
            return None
    if benchling_type == "float":
        try:
            return float(str(raw))
        except (ValueError, TypeError):
            return None
    if benchling_type == "date":
        return str(raw).strip() if raw else None
    if isinstance(raw, float) and raw == int(raw):
        return int(raw)
    return raw


def build_fields(schema_key, mapping, row):
    """Build Benchling fields dict: { FieldName: {value: typed_value} }"""
    out = {}
    for entry in mapping.get(schema_key, []):
        bf             = entry.get("benchling_field", "")
        col            = entry.get("suggested_column") or entry.get("mapped")
        status         = entry.get("status", "")
        benchling_type = entry.get("benchling_type", "text")

        if status == "ignored":
            continue
        if bf.lower().replace(" ", "_") in _SKIP_FIELDS or bf.lower() in _SKIP_FIELDS:
            continue
        if not col or col not in row.index:
            continue

        val = _coerce(row[col], benchling_type)
        if val is None:
            continue

        # Type safety — skip if coercion failed for numeric fields
        if benchling_type == "integer" and not isinstance(val, int):
            logger.warning(f"  Skipping '{bf}': '{val}' not valid integer")
            continue
        if benchling_type == "float" and not isinstance(val, (int, float)):
            logger.warning(f"  Skipping '{bf}': '{val}' not valid float")
            continue

        out[bf] = {"value": val}
    return out


def pos_to_well(p):
    """1→A1, 13→B1, 'A1'→'A1'"""
    try:
        i = int(str(p).strip())
        return f"{chr(64 + ((i - 1) // 12 + 1))}{(i - 1) % 12 + 1}"
    except Exception:
        return str(p).strip()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def main(file_path=None, mapping_file_path=None):

    config        = load_config()
    benchling_cfg = config.get("benchling", {})

    logger.info("Loading approved mapping...")
    mapping = load_approved_mapping()

    # Resolve all schema IDs (user selection → fallback)
    sample_schema_id    = resolve_schema_id("sample")
    dna_schema_id       = resolve_schema_id("dna")
    results_schema_id   = resolve_schema_id("results")
    container_schema_id = resolve_schema_id("inventory")
    location_schema_id  = resolve_schema_id("location")
    box_schema_id       = resolve_schema_id("box")
    entry_template_id   = _FALLBACK_SCHEMAS["entry_template"]

    logger.info(f"Schema IDs — Sample:{sample_schema_id} | DNA:{dna_schema_id} | "
                f"Results:{results_schema_id} | Container:{container_schema_id}")

    # Load + clean data
    data_file = get_data_file(mapping_file_path)
    if not data_file or not os.path.exists(data_file):
        raise FileNotFoundError(f"Data file not found: {data_file}")

    df     = pd.read_excel(data_file) if data_file.endswith(".xlsx") else pd.read_csv(data_file)
    before = len(df)
    df     = df.dropna(how="all")
    if "CRO-Name" in df.columns:
        df = df[df["CRO-Name"].notna()]
        df = df[df["CRO-Name"].astype(str).str.strip().str.len() > 0]
    if "Sample_ID" in df.columns:
        df = df[df["Sample_ID"].notna()]
    df = df.reset_index(drop=True)
    logger.info(f"Data: {before} rows → {len(df)} valid rows after cleaning")

    parent_folder_id = get_selected_folder(benchling_cfg)
    unique_cros      = df["CRO-Name"].dropna().unique() if "CRO-Name" in df.columns else ["Default"]

    # ── 1. CRO folders ────────────────────────────────────────────────────────
    logger.info("Creating CRO folders...")
    folder_ids:  Dict[str, str] = {}
    project_ids: Dict[str, str] = {}

    for cro in unique_cros:
        cro_str  = str(cro).strip()
        existing = find_existing_folder(cro_str, parent_folder_id)
        if existing:
            logger.info(f"  Reusing folder '{cro_str}': {existing}")
            folder_ids[cro]  = existing
            project_ids[cro] = None
        else:
            folder = create_folder({"name": cro_str, "parentFolderId": parent_folder_id})
            f_id   = getattr(folder, "id", None) or (folder.get("id") if isinstance(folder, dict) else None)
            p_id   = getattr(folder, "project_id", None) or (folder.get("projectId") if isinstance(folder, dict) else None)
            folder_ids[cro]  = f_id
            project_ids[cro] = p_id
            logger.info(f"  Created folder '{cro_str}': {f_id}")

    # ── 2. Notebook entries (one per CRO) ─────────────────────────────────────
    logger.info("Creating notebook entries...")
    entry_ids: Dict[str, str] = {}

    for cro in unique_cros:
        result = create_entry({
            "entryTemplateId": entry_template_id,
            "folderId":        folder_ids.get(cro, parent_folder_id),
            "name":            str(cro).strip(),
        })
        eid = getattr(result, "id", None) or (result.get("id") if isinstance(result, dict) else None)
        if not eid:
            raise RuntimeError(f"Entry creation failed for CRO: {cro}")
        entry_ids[cro] = eid
        logger.info(f"  Entry '{cro}': {eid}")

    # ── 3. DNA entities (one per sample row) ──────────────────────────────────
    # KEY: DNA_Sequence_POC is a Custom Entity in your tenant.
    # Name = Sample_Name (ADC-Sample-1) NOT Construct_Name (mAb-Construct).
    # Construct_Name is stored as a field value only.
    logger.info("Creating DNA entities...")
    created_dna: List[Tuple[str, int, str]] = []

    for idx, row in df.iterrows():
        cro  = str(row.get("CRO-Name", "Default")).strip() if "CRO-Name" in df.columns else "Default"
        name = str(row.get("Sample_Name") or row.get("Sample_ID") or f"DNA-{idx}").strip()

        fields  = build_fields("DNA Sequence", mapping, row)
        payload = {
            "name":     name,
            "schemaId": dna_schema_id,
            "folderId": folder_ids.get(cro, parent_folder_id),
            "fields":   fields,
        }
        if cro in entry_ids:
            payload["entryId"] = entry_ids[cro]

        try:
            logger.info(f"  Creating DNA entity: {name}")
            res    = create_custom_entity(payload)
            dna_id = getattr(res, "id", None) or (res.get("id") if isinstance(res, dict) else None)
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

        # Entity linked = DNA entity (links the sequence to this sample)
        if dna_id:
            fields["Entity linked"] = {"value": dna_id}
            logger.info(f"  Entity linked: {name} → {dna_id}")

        payload = {
            "name":     name,
            "schemaId": sample_schema_id,
            "folderId": folder_ids.get(cro, parent_folder_id),
            "fields":   fields,
        }
        if cro in entry_ids:
            payload["entryId"] = entry_ids[cro]

        try:
            logger.info(f"  Creating Sample: {name}")
            res       = create_custom_entity(payload)
            entity_id = getattr(res, "id", None) or (res.get("id") if isinstance(res, dict) else None)
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
        qty_val      = row.get("Quantity_mg",  0.0)
        conc_val     = row.get("Concentration", 0.0)
        storage_cond = str(row.get("Storage_Condition", "")).strip()

        if not loc_name or not box_name:
            logger.warning(f"  Row {idx}: missing Storage_Location/Box — skipping")
            continue

        # Location
        loc_key = f"loc_{loc_name}"
        if loc_key not in storage_cache:
            loc_id = find_storage_by_name(loc_name, location_schema_id)
            if not loc_id:
                r      = create_location({"name": loc_name, "schemaId": location_schema_id})
                loc_id = getattr(r, "id", None) or (r.get("id") if isinstance(r, dict) else None)
                logger.info(f"  Created location: {loc_name} ({loc_id})")
            else:
                logger.info(f"  Reusing location: {loc_name} ({loc_id})")
            storage_cache[loc_key] = loc_id
        location_id = storage_cache[loc_key]

        # Box
        box_key = f"box_{box_name}_{location_id}"
        if box_key not in storage_cache:
            box_id = find_storage_by_name(box_name, box_schema_id)
            if not box_id:
                r      = create_box({"name": box_name, "schemaId": box_schema_id, "parentStorageId": location_id})
                box_id = getattr(r, "id", None) or (r.get("id") if isinstance(r, dict) else None)
                logger.info(f"  Created box: {box_name} ({box_id})")
            else:
                logger.info(f"  Reusing box: {box_name} ({box_id})")
            storage_cache[box_key] = box_id
        box_id = storage_cache[box_key]

        # Container — barcode = Sample_ID, linked to sample entity
        well = pos_to_well(position)
        container_payload = {
            "name":            cont_name,
            "barcode":         barcode,
            "schemaId":        container_schema_id,
            "parentStorageId": f"{box_id}:{well}",
            "fields": {
                "Quantity_mg":       {"value": float(qty_val)  if qty_val  else 0.0},
                "Concentration":     {"value": float(conc_val) if conc_val else 0.0},
                "Storage_Condition": {"value": storage_cond},
            },
        }

        try:
            cont_result = create_container_direct(container_payload)
            cont_id     = cont_result.get("id") if isinstance(cont_result, dict) else None
            logger.info(f"  Container: {cont_id} | barcode:{barcode} | {box_name}:{well}")

            transfer_into_container_direct(cont_id, {
                "contents": [{
                    "entityId":      entity_id,
                    "concentration": {"value": float(conc_val) if conc_val else 0.0, "units": "mg/mL"},
                }]
            })
            logger.info(f"  Sample {entity_id} → container {cont_id}")

        except Exception as e:
            logger.warning(f"  Container/transfer error row {idx}: {e}")

    # ── 6. Assay results ──────────────────────────────────────────────────────
    logger.info("Uploading assay results...")

    cro_entities: Dict[str, List[Tuple[str, int]]] = {}
    for entity_id, idx, cro in created_entities:
        cro_entities.setdefault(cro, []).append((entity_id, idx))

    for cro, entity_list in cro_entities.items():
        assay_results = []

        for entity_id, idx in entity_list:
            row    = df.iloc[idx]
            fields = build_fields("Results", mapping, row)
            fields = {k.lower(): v for k, v in fields.items()}
            fields["linked_sample"] = {"value": entity_id}

            assay_results.append({
                "schemaId": results_schema_id,   # REQUIRED — always set
                "fields":   fields,
                **({"projectId": project_ids[cro]} if project_ids.get(cro) else {}),
            })

        if not assay_results:
            logger.warning(f"  No results for '{cro}' — skipping")
            continue

        # Optional tableId — links results into the notebook view
        entry_id        = entry_ids.get(cro)
        target_table_id = None
        if entry_id:
            try:
                target_table_id = get_result_table_id_from_entry(entry_id)
            except Exception:
                pass

        payload: Dict[str, Any] = {"assayResults": assay_results}
        if target_table_id:
            payload["tableId"] = target_table_id

        try:
            response = create_assay_results_bulk(payload)
            logger.info(f"  Results uploaded for '{cro}': {response}")
        except Exception as e:
            logger.error(f"  Result upload failed for '{cro}': {e}")
            raise

    logger.info("Pipeline complete!")


if __name__ == "__main__":
    main()