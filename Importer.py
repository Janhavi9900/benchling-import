"""
Importer.py  —  Benchling ingestion pipeline
=============================================
Fully dynamic — no CRO Mapping.xlsx, no hardcoded schema IDs.

Sources of truth:
  - ai/approved_mapping.json   : field → column mapping confirmed by user in UI
  - ai/selected_schemas.json   : schema IDs chosen by user in Step 3 of UI
  - ai/selected_notebook.json  : destination folder chosen by user in Step 1
  - uploads/harmonized_upload* : the data file uploaded by user

Flow:
  1. Load approved_mapping + selected_schemas + data file
  2. Create CRO folders + notebook entries
  3. Create DNA sequences   (fields from mapping, schemaId from selected_schemas)
  4. Create samples         (fields from mapping, Entity linked = DNA id)
  5. Create inventory       (Location → Box → Container, barcode = Sample_ID)
  6. Upload assay results   (schemaId from selected_schemas, linked to sample)
"""

import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

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
# HELPERS — file / config resolution
# ══════════════════════════════════════════════════════════════════════════════

def get_data_file(override: Optional[str] = None) -> str:
    """Return path to the user-uploaded data file."""
    if override and os.path.exists(override):
        return override
    env = os.getenv("HARMONIZED_FILE")
    if env and os.path.exists(env):
        return env
    for ext in [".xlsx", ".csv"]:
        p = os.path.join("uploads", f"harmonized_upload{ext}")
        if os.path.exists(p):
            return p
    cfg = load_config()
    return cfg.get("excel", {}).get("data_workbook", "Harmonized dataset_new.xlsx")


def get_selected_folder(benchling_cfg: dict) -> str:
    """Return the lib_ folder ID the user picked in the UI."""
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


def load_approved_mapping() -> Dict[str, List[Dict]]:
    """Load field→column mapping confirmed by user."""
    path = "ai/approved_mapping.json"
    if not os.path.exists(path):
        raise FileNotFoundError(
            "No approved mapping found. Complete the mapping step in the UI first."
        )
    with open(path) as f:
        return json.load(f)


def load_selected_schemas() -> Dict[str, Dict]:
    """
    Load schema selections from Step 3 of the UI.
    Keys are section IDs: sample, dna, results, inventory, location, box
    Values are: { id: <schemaId>, name: <schemaName>, type: <schemaType> }
    """
    path = "ai/selected_schemas.json"
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def lookup_schema_id_in_erd(schema_name: str) -> Optional[str]:
    """Fallback: find schema ID by name from cached ERD."""
    erd_path = "ai/benchling_erd.json"
    if not os.path.exists(erd_path):
        return None
    with open(erd_path) as f:
        erd = json.load(f)
    for s in erd.get("schemas", []):
        if s.get("name") == schema_name:
            return s.get("id")
    return None


def resolve_schema_id(
    section_key: str,
    selected_schemas: Dict,
    fallback_name: str = "",
) -> Optional[str]:
    """
    Return schema ID for a section.
    Tries selected_schemas[section_key] first, then ERD lookup by fallback_name.
    """
    schema = selected_schemas.get(section_key, {})
    # Handle both flat {id:...} and nested {schema: {id:...}} shapes
    sid = schema.get("id") or schema.get("schema", {}).get("id")
    if sid:
        return sid
    if fallback_name:
        return lookup_schema_id_in_erd(fallback_name)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# PAYLOAD BUILDER — approved_mapping.json driven
# ══════════════════════════════════════════════════════════════════════════════

# Fields resolved at runtime — never sent from the mapping
_RUNTIME_FIELDS = {"entity linked", "linked_sample", "entity_linked"}

# Fields that represent folder/org structure — Benchling handles these via
# folderId/entryId, not as schema field values. Sending them causes 400 errors
# when the Benchling field type (e.g. integer) doesn't match the text value.
_STRUCTURAL_FIELDS = {"cro", "folder", "folderid", "projectid"}


def _coerce(raw: Any, benchling_type: str = "text") -> Any:
    """
    Make a value JSON-safe and Benchling-friendly.
    Respects the Benchling field type so text fields are always strings,
    numeric fields are always numbers, date fields are ISO strings.
    """
    if isinstance(raw, pd.Timestamp):
        return raw.date().isoformat()
    try:
        if pd.isna(raw):
            return None
    except Exception:
        pass
    if hasattr(raw, "item"):        # numpy scalar → Python native
        raw = raw.item()

    if benchling_type == "text":
        # Always send as string — avoids "1260" being sent as integer 1260
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
        if isinstance(raw, str):
            return raw.strip()
        return str(raw)

    # fallback — preserve original but coerce whole-number floats to int
    if isinstance(raw, float) and raw == int(raw):
        return int(raw)
    return raw


def _type_matches(val: Any, benchling_type: str) -> bool:
    """
    Return False if the value will definitely be rejected by Benchling
    due to a type mismatch — e.g. sending "CRO-A" into an integer field.
    """
    if benchling_type == "integer":
        try:
            int(str(val).strip())
            return True
        except (ValueError, TypeError):
            return False   # non-numeric string into integer field → skip it
    if benchling_type == "float":
        try:
            float(str(val).strip())
            return True
        except (ValueError, TypeError):
            return False
    return True   # text / date / entity_link — always attempt


def build_fields(
    schema_key: str,
    mapping: Dict,
    row: pd.Series,
) -> Dict[str, Any]:
    """
    Build a Benchling fields dict from approved_mapping + one data row.
    Returns: { "FieldName": {"value": <value>}, ... }

    Skips:
      - ignored entries
      - runtime-resolved entity links (Entity linked, linked_sample)
      - structural fields handled via folderId/entryId (CRO, folder, etc.)
      - type-mismatched values (e.g. "CRO-A" into an integer field)
      - missing columns
    """
    out = {}
    for entry in mapping.get(schema_key, []):
        bf            = entry.get("benchling_field", "")
        col           = entry.get("suggested_column") or entry.get("mapped")
        status        = entry.get("status", "")
        benchling_type = entry.get("benchling_type", "text")

        if status == "ignored":
            continue
        # Skip runtime-resolved links
        if bf.lower().replace(" ", "_") in _RUNTIME_FIELDS or bf.lower() in _RUNTIME_FIELDS:
            continue
        # Skip structural/org fields that conflict with folder structure
        if bf.lower().replace(" ", "_") in _STRUCTURAL_FIELDS or bf.lower() in _STRUCTURAL_FIELDS:
            continue
        if not col or col not in row.index:
            continue

        val = _coerce(row[col], benchling_type)
        if val is None:
            continue

        # Skip if value type doesn't match Benchling field type
        if not _type_matches(val, benchling_type):
            logger.warning(
                f"  Skipping field '{bf}' — value '{val}' is not valid "
                f"for Benchling type '{benchling_type}'"
            )
            continue

        out[bf] = {"value": val}

    return out


# ══════════════════════════════════════════════════════════════════════════════
# POSITION HELPER
# ══════════════════════════════════════════════════════════════════════════════

def pos_to_well(p: Any) -> str:
    """
    Convert numeric position or existing well coordinate.
    1 → A1, 13 → B1, 'A1' → 'A1'
    """
    try:
        i = int(str(p).strip())
        return f"{chr(64 + ((i - 1) // 12 + 1))}{(i - 1) % 12 + 1}"
    except Exception:
        return str(p).strip()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def main(
    file_path: Optional[str] = None,
    mapping_file_path: Optional[str] = None,
) -> None:

    # ── 0. Load all config sources ────────────────────────────────────────────
    config        = load_config()
    benchling_cfg = config.get("benchling", {})

    logger.info("Loading approved mapping...")
    mapping = load_approved_mapping()
    logger.info(f"Schemas in mapping: {list(mapping.keys())}")

    logger.info("Loading schema selections...")
    selected_schemas = load_selected_schemas()
    logger.info(f"Selected schemas: { {k: v.get('name','?') for k, v in selected_schemas.items()} }")

    data_file = get_data_file(mapping_file_path)
    logger.info(f"Data file: {data_file}")
    if not os.path.exists(data_file):
        raise FileNotFoundError(f"Data file not found: {data_file}")

    df = (pd.read_excel(data_file) if data_file.endswith(".xlsx") else pd.read_csv(data_file))

    # ── Drop completely empty rows and rows missing Sample_ID or CRO-Name ─────
    # These are trailing blank rows from Excel that cause nan/NaT entries
    before = len(df)
    df = df.dropna(how="all")                          # drop rows where ALL cells are empty
    if "CRO-Name" in df.columns:
        df = df[df["CRO-Name"].notna()]                # drop rows with no CRO
        df = df[df["CRO-Name"].astype(str).str.strip() != ""]
    if "Sample_ID" in df.columns:
        df = df[df["Sample_ID"].notna()]               # drop rows with no Sample ID
    df = df.reset_index(drop=True)
    logger.info(f"Loaded {before} rows, {before - len(df)} empty rows dropped → {len(df)} valid rows x {len(df.columns)} columns")

    parent_folder_id = get_selected_folder(benchling_cfg)

    # ── Resolve schema IDs dynamically from user's Step 3 selection ───────────
    sample_schema_id    = (resolve_schema_id("sample",    selected_schemas)
                           or resolve_schema_id("Sample",       selected_schemas)
                           or lookup_schema_id_in_erd("Sample"))

    dna_schema_id       = (resolve_schema_id("dna",       selected_schemas)
                           or resolve_schema_id("DNA Sequence",  selected_schemas)
                           or lookup_schema_id_in_erd("DNA_Sequence_POC"))

    results_schema_id   = (resolve_schema_id("results",   selected_schemas)
                           or resolve_schema_id("Results",       selected_schemas))

    container_schema_id = (resolve_schema_id("inventory", selected_schemas)
                           or resolve_schema_id("Container",     selected_schemas))

    location_schema_id  = (resolve_schema_id("location",  selected_schemas)
                           or lookup_schema_id_in_erd("Location"))

    box_schema_id       = (resolve_schema_id("box",       selected_schemas)
                           or lookup_schema_id_in_erd("Box"))

    entry_template_id   = config.get("templates", {}).get(
        "entry_template_id", "tmpl_4fdaFvrFMZ"
    )

    logger.info(f"Schema IDs — Sample:{sample_schema_id} | DNA:{dna_schema_id} | "
                f"Results:{results_schema_id} | Container:{container_schema_id}")

    # ── 1. CRO folders ────────────────────────────────────────────────────────
    logger.info("Creating CRO folders...")
    folder_ids:  Dict[str, str] = {}
    project_ids: Dict[str, str] = {}
    unique_cros = (df["CRO-Name"].dropna().unique()
                   if "CRO-Name" in df.columns else ["Default"])

    def find_existing_folder(name: str, parent_id: str) -> Optional[str]:
        """Return existing folder ID if already exists under parent — prevents duplicates on re-run."""
        try:
            import requests as _req
            cfg      = load_config()
            api_key  = cfg["benchling"]["api_key_env_var"]
            key      = os.getenv(api_key, api_key) if not api_key.startswith("sk_") else api_key
            base     = cfg["benchling"]["base_url"]
            resp     = _req.get(f"{base}/folders", auth=(key, ""), timeout=15)
            if resp.status_code == 200:
                for f in resp.json().get("folders", []):
                    if f.get("name") == name and f.get("parentFolderId") == parent_id:
                        return f.get("id")
        except Exception:
            pass
        return None

    for cro in unique_cros:
        cro_str      = str(cro).strip()
        existing_fid = find_existing_folder(cro_str, parent_folder_id)
        if existing_fid:
            logger.info(f"  Reusing existing folder {cro_str}: {existing_fid}")
            folder_ids[cro]  = existing_fid
            project_ids[cro] = None
        else:
            folder  = create_folder({"name": cro_str, "parentFolderId": parent_folder_id})
            f_id    = getattr(folder, "id", None) or (folder.get("id") if isinstance(folder, dict) else None)
            p_id    = getattr(folder, "project_id", None) or (folder.get("projectId") if isinstance(folder, dict) else None)
            folder_ids[cro]  = f_id
            project_ids[cro] = p_id
            logger.info(f"  Created folder {cro_str}: {f_id}")

    # ── 2. Notebook entries (one per CRO) ─────────────────────────────────────
    logger.info("Creating notebook entries...")
    entry_ids: Dict[str, str] = {}

    for cro in unique_cros:
        result = create_entry({
            "entryTemplateId": entry_template_id,
            "folderId":        folder_ids.get(cro, parent_folder_id),
            "name":            str(cro),
        })
        eid = getattr(result, "id", None) or (result.get("id") if isinstance(result, dict) else None)
        if not eid:
            raise RuntimeError(f"Entry creation failed for CRO: {cro}")
        entry_ids[cro] = eid
        logger.info(f"  Entry {eid} for {cro}")

    # ── 3. DNA sequences (one per row) ────────────────────────────────────────
    logger.info("Creating DNA sequences...")
    created_dna: List[Tuple[str, int, str]] = []

    for idx, row in df.iterrows():
        cro   = str(row.get("CRO-Name", "Default")) if "CRO-Name" in df.columns else "Default"
        bases = str(row["Sequence"]) if "Sequence" in row.index and pd.notna(row.get("Sequence")) else ""
        name  = str(row.get("Construct_Name") or row.get("Sample_Name") or f"DNA-{idx}")

        payload = {
            "bases":      bases,
            "isCircular": False,
            "name":       name,
            "fields":     build_fields("DNA Sequence", mapping, row),
            "folderId":   folder_ids.get(cro, parent_folder_id),
        }
        if dna_schema_id:
            payload["schemaId"] = dna_schema_id
        if cro in entry_ids:
            payload["entryId"] = entry_ids[cro]

        try:
            logger.info(f"  Creating DNA: {name}")
            res    = create_dna_sequence(payload)
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
        name   = str(row.get("Sample_Name") or row.get("Sample_ID") or f"Sample-{idx}")
        fields = build_fields("Sample", mapping, row)

        # Wire Entity linked → DNA sequence created above
        if dna_id:
            fields["Entity linked"] = {"value": dna_id}
            logger.info(f"  Entity linked → {dna_id}")

        payload = {
            "name":     name,
            "fields":   fields,
            "folderId": folder_ids.get(cro, parent_folder_id),
        }
        if sample_schema_id:
            payload["schemaId"] = sample_schema_id
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

    # ── 5. Inventory: Location → Box → Container → Transfer ───────────────────
    logger.info("Creating inventory...")
    storage_cache: Dict[str, str] = {}

    for entity_id, idx, cro in created_entities:
        row          = df.iloc[idx]
        loc_name     = str(row.get("Storage_Location", "")).strip()
        box_name     = str(row.get("Box", "")).strip()
        position     = str(row.get("Position", "A1")).strip()
        barcode      = str(row.get("Sample_ID", "")).strip()      # shown in Benchling inventory
        cont_name    = str(row.get("Sample_Name", barcode)).strip()
        qty_val      = row.get("Quantity_mg", 0.0)
        conc_val     = row.get("Concentration", 0.0)
        storage_cond = str(row.get("Storage_Condition", "")).strip()

        if not loc_name or not box_name:
            logger.warning(f"  Row {idx}: missing Storage_Location or Box — skipping inventory")
            continue

        # Location
        loc_key = f"loc_{loc_name}"
        if loc_key not in storage_cache:
            loc_id = find_storage_by_name(loc_name, location_schema_id) if location_schema_id else None
            if not loc_id:
                loc_payload = {"name": loc_name}
                if location_schema_id:
                    loc_payload["schemaId"] = location_schema_id
                r      = create_location(loc_payload)
                loc_id = getattr(r, "id", None) or (r.get("id") if isinstance(r, dict) else None)
                logger.info(f"  Created location: {loc_name} ({loc_id})")
            else:
                logger.info(f"  Reusing location: {loc_name} ({loc_id})")
            storage_cache[loc_key] = loc_id
        location_id = storage_cache[loc_key]

        # Box
        box_key = f"box_{box_name}_{location_id}"
        if box_key not in storage_cache:
            box_id = find_storage_by_name(box_name, box_schema_id) if box_schema_id else None
            if not box_id:
                box_payload = {"name": box_name, "parentStorageId": location_id}
                if box_schema_id:
                    box_payload["schemaId"] = box_schema_id
                r      = create_box(box_payload)
                box_id = getattr(r, "id", None) or (r.get("id") if isinstance(r, dict) else None)
                logger.info(f"  Created box: {box_name} ({box_id})")
            else:
                logger.info(f"  Reusing box: {box_name} ({box_id})")
            storage_cache[box_key] = box_id
        box_id = storage_cache[box_key]

        # Container
        well         = pos_to_well(position)
        extra_fields = build_fields("Container", mapping, row)
        cont_fields  = {
            "Quantity_mg":       {"value": float(qty_val)  if qty_val  else 0.0},
            "Concentration":     {"value": float(conc_val) if conc_val else 0.0},
            "Storage_Condition": {"value": storage_cond},
            **extra_fields,
        }

        container_payload = {
            "name":            cont_name,
            "barcode":         barcode,
            "parentStorageId": f"{box_id}:{well}",
            "fields":          cont_fields,
        }
        if container_schema_id:
            container_payload["schemaId"] = container_schema_id

        try:
            cont_result = create_container_direct(container_payload)
            cont_id     = cont_result.get("id") if isinstance(cont_result, dict) else None
            logger.info(f"  Container: {cont_id} | barcode:{barcode} | pos:{box_name}:{well}")

            transfer_into_container_direct(cont_id, {
                "contents": [{
                    "entityId":      entity_id,
                    "concentration": {
                        "value": float(conc_val) if conc_val else 0.0,
                        "units": "mg/mL",
                    },
                }]
            })
            logger.info(f"  Sample {entity_id} → container {cont_id}")

        except Exception as e:
            logger.warning(f"  Container/transfer error row {idx}: {e}")

    # ── 6. Assay results ──────────────────────────────────────────────────────
    logger.info("Uploading assay results...")

    if not results_schema_id:
        logger.warning("No results schema ID — skipping results upload.")
        logger.warning("Tip: select an Assay Result schema in Step 3 of the UI.")
    else:
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
                    "schemaId": results_schema_id,   # from user's schema selection — not hardcoded
                    "fields":   fields,
                    **({"projectId": project_ids[cro]} if project_ids.get(cro) else {}),
                })

            if not assay_results:
                logger.warning(f"  No results for {cro} — skipping")
                continue

            # Optional: get tableId from notebook entry for display in notebook
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
                logger.info(f"  Results uploaded for {cro}: {response}")
            except Exception as e:
                logger.error(f"  Result upload failed for {cro}: {e}")
                raise

    logger.info("Pipeline complete!")


if __name__ == "__main__":
    main()