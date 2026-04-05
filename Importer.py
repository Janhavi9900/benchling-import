"""
Importer.py
Main pipeline orchestrator.
- Uses the file uploaded via the UI (no hardcoded file paths)
- Uses the notebook selected by the user in the UI
- Falls back to config.json values if nothing selected
"""

import json
import logging
import os
import sys
from typing import List

import pandas as pd

from benchling_client import (
    create_assay_results_bulk,
    create_custom_entity,
    create_dna_sequence,
    create_entry,
    create_folder,
    get_entry_details,
    get_result_table_id_from_entry,
    create_location,
    create_box,
    create_container_direct,
    transfer_into_container_direct,
    find_storage_by_name,
    find_any_entity_by_name,
    find_dna_sequence_by_name,
    find_custom_entity_by_name,
)
from config_loader import load_config
from payload_builder import (
    build_payload_from_mapping,
    build_payloads_from_mapping,
    infer_column_types,
    normalize_mapping_dataframe,
)

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


# ─── Helper: get active data file ─────────────────────────────────────────────

def get_data_file(override_path: str | None = None) -> str:
    """
    Returns the data file to use for ingestion.
    Priority:
      1. override_path passed directly (from backend websocket)
      2. HARMONIZED_FILE env var (set by backend on upload)
      3. uploads/ folder (check for uploaded file)
      4. config.json data_workbook (legacy fallback)
    """
    if override_path and os.path.exists(override_path):
        return override_path

    env_file = os.getenv("HARMONIZED_FILE")
    if env_file and os.path.exists(env_file):
        return env_file

    for ext in [".xlsx", ".csv"]:
        p = os.path.join("uploads", f"harmonized_upload{ext}")
        if os.path.exists(p):
            return p

    # Legacy fallback — config.json
    cfg = load_config()
    return cfg.get("excel", {}).get("data_workbook", "Harmonized dataset_new.xlsx")


# ─── Helper: get selected notebook folder ─────────────────────────────────────

def get_selected_folder(benchling_cfg: dict) -> str:
    """
    Returns the parent folder ID to ingest data into.
    Priority:
      1. ai/selected_notebook.json (set by user in UI)
      2. SELECTED_FOLDER_ID env var
      3. config.json parent_folder_id (fallback)
    """
    # Check UI selection first
    if os.path.exists("ai/selected_notebook.json"):
        with open("ai/selected_notebook.json") as f:
            selected = json.load(f)
        folder_id = selected.get("folder_id")
        if folder_id:
            logger.info(
                f"📁 Destination notebook: {selected.get('folder_name', folder_id)}"
            )
            return folder_id

    # Check env var
    env_folder = os.getenv("SELECTED_FOLDER_ID")
    if env_folder:
        return env_folder

    # Fall back to config.json
    folder_id = benchling_cfg.get("parent_folder_id")
    if folder_id:
        logger.info(f"📁 Using config folder: {folder_id}")
        return folder_id

    raise ValueError(
        "No destination folder found. "
        "Please select a notebook in the UI or set parent_folder_id in config.json."
    )


# ─── Entry import ─────────────────────────────────────────────────────────────

def entry_import(
    entry_df: pd.DataFrame,
    input_df: pd.DataFrame,
    template_path: str,
) -> dict:
    """
    Constructs a payload for a Notebook Entry.
    Uses the first row of data for entry-level attributes.
    """
    if input_df.empty:
        raise ValueError("Input data for entries is empty.")

    mapping      = normalize_mapping_dataframe(entry_df)
    column_types = infer_column_types(input_df)
    first_row    = input_df.iloc[0]

    payload = build_payload_from_mapping(
        mapping,
        template_path,
        data_row=first_row,
        column_types=column_types,
    )

    # Fallback: ensure entry has a name
    if not payload.get("name"):
        cro_name = first_row.get("CRO-Name", "Unknown Entry")
        payload["name"] = str(cro_name)

    return payload


# ─── DNA sequence import ───────────────────────────────────────────────────────

def dna_sequence_import(
    dna_sequence_df: pd.DataFrame,
    input_df: pd.DataFrame,
    template_path: str,
) -> List[dict]:
    """Builds a list of DNA sequence payloads from the input data."""
    return build_payloads_from_mapping(
        dna_sequence_df,
        template_path,
        input_df,
    )


# ─── Custom entity (sample) import ────────────────────────────────────────────

def custom_entity_import(
    custom_df: pd.DataFrame,
    input_df: pd.DataFrame,
    template_path: str,
) -> List[dict]:
    """Builds a list of Custom Entity (Sample) payloads from the input data."""
    return build_payloads_from_mapping(
        custom_df,
        template_path,
        input_df,
    )


# ─── Main pipeline ─────────────────────────────────────────────────────────────

def main(
    file_path: str | None = None,
    mapping_file_path: str | None = None,
) -> None:
    """
    Full ingestion pipeline:
      1. Read configuration
      2. Determine which data file to use (uploaded via UI or config fallback)
      3. Determine which notebook to ingest into (selected via UI or config fallback)
      4. Create CRO folders
      5. Create notebook entries
      6. Create DNA sequences
      7. Create samples (custom entities) — linked to DNA
      8. Manage inventory (locations, boxes, containers)
      9. Upload assay results
    """

    # ── Load config ──────────────────────────────────────────────────────────
    config        = load_config()
    excel_cfg     = config.get("excel", {})
    templates_cfg = config.get("templates", {})
    benchling_cfg = config.get("benchling", {})

    # ── Determine mapping workbook (CRO Mapping.xlsx) ────────────────────────
    # This is the schema mapping file — always from config
    file_path = file_path or excel_cfg.get("mapping_workbook")

    # ── Determine data file (uploaded by user) ────────────────────────────────
    mapping_file_path = get_data_file(mapping_file_path)
    logger.info(f"📂 Data file: {mapping_file_path}")

    if not file_path:
        raise ValueError("Mapping workbook path missing in config.json")
    if not os.path.exists(mapping_file_path):
        raise ValueError(f"Data file not found: {mapping_file_path}")

    # ── Sheet names ───────────────────────────────────────────────────────────
    entry_sheet     = excel_cfg.get("entry_sheet_name",     "Entry")
    dna_sheet       = excel_cfg.get("dna_sheet_name",       "DNA Sequence")
    custom_sheet    = excel_cfg.get("custom_sheet_name",    "Sample")
    results_sheet   = excel_cfg.get("results_sheet_name",   "Results")
    location_sheet  = excel_cfg.get("location_sheet_name",  "Location")
    box_sheet       = excel_cfg.get("box_sheet_name",       "Box")
    container_sheet = excel_cfg.get("container_sheet_name", "Container")

    # ── Template paths ────────────────────────────────────────────────────────
    entry_template     = templates_cfg.get("entry_template")
    dna_template       = templates_cfg.get("dna_template")
    custom_template    = templates_cfg.get("custom_template")
    result_template    = templates_cfg.get("result_template", "result_template.json")
    inventory_template = "inventory_template.json"

    # ── Get destination folder (selected in UI) ───────────────────────────────
    parent_folder_id = get_selected_folder(benchling_cfg)

    # ── 1. Read Excel mapping sheets ──────────────────────────────────────────
    logger.info("📖 Reading mapping workbook sheets...")
    sheets = pd.read_excel(
        file_path,
        sheet_name=[
            entry_sheet, dna_sheet, custom_sheet,
            results_sheet, location_sheet, box_sheet, container_sheet,
        ],
    )

    entry_df     = sheets[entry_sheet]
    dna_df       = sheets[dna_sheet]
    custom_df    = sheets[custom_sheet]
    results_df   = sheets[results_sheet]
    location_df  = sheets[location_sheet]
    box_df       = sheets[box_sheet]
    container_df = sheets[container_sheet]

    # Read the actual data rows from the uploaded file
    logger.info(f"📊 Reading data from: {os.path.basename(mapping_file_path)}")
    if mapping_file_path.endswith(".csv"):
        mapping_df = pd.read_csv(mapping_file_path)
    else:
        mapping_df = pd.read_excel(mapping_file_path)

    logger.info(f"✅ Loaded {len(mapping_df)} rows × {len(mapping_df.columns)} columns")

    # ── 2. Create folders per CRO ─────────────────────────────────────────────
    logger.info("📁 Creating CRO folders...")
    unique_cros = mapping_df["CRO-Name"].dropna().unique()
    folder_ids  = {}
    project_ids = {}

    for cro in unique_cros:
        folder_payload = {"name": str(cro), "parentFolderId": parent_folder_id}
        folder  = create_folder(folder_payload)
        f_id    = getattr(folder, "id", folder.get("id") if isinstance(folder, dict) else None)
        folder_ids[cro] = f_id

        p_id = getattr(folder, "project_id", None)
        if p_id is None and isinstance(folder, dict):
            p_id = folder.get("projectId")
        project_ids[cro] = p_id

        logger.info(f"✅ Created folder for CRO: {cro} (ID: {f_id}, Project: {p_id})")

    # ── 3. Create notebook entries (one per CRO) ──────────────────────────────
    logger.info("📓 Creating notebook entries...")
    entry_ids = {}

    for cro in unique_cros:
        cro_data = mapping_df[mapping_df["CRO-Name"] == cro]
        if cro_data.empty:
            continue

        entry_payload = entry_import(
            entry_df,
            input_df=cro_data,
            template_path=entry_template,
        )
        entry_payload["name"] = str(cro)
        if cro in folder_ids:
            entry_payload["folderId"] = folder_ids[cro]

        entry_result = create_entry(entry_payload)
        entry_id     = getattr(entry_result, "id", None)
        if entry_id is None and isinstance(entry_result, dict):
            entry_id = entry_result.get("id")

        if not entry_id:
            raise RuntimeError(f"Entry creation failed for CRO: {cro}")

        entry_ids[cro] = entry_id
        logger.info(f"✅ Created Entry ID: {entry_id} for CRO: {cro}")

    # ── 4. Build assay mapping ────────────────────────────────────────────────
    cro_assay_mapping = {}
    for cro in unique_cros:
        cro_data = mapping_df[mapping_df["CRO-Name"] == cro]
        assay_ids = (
            cro_data["Assay_ID"].dropna().unique().tolist()
            if "Assay_ID" in cro_data.columns else []
        )
        cro_assay_mapping[cro] = {
            "entry_id":      entry_ids.get(cro),
            "assay_ids":     assay_ids,
            "dna_sequences": [],
        }

    # ── 5. Create DNA sequences ───────────────────────────────────────────────
    logger.info("🧬 Creating DNA sequences...")
    dna_payloads = dna_sequence_import(dna_df, mapping_df, dna_template)
    created_dna_sequences = []

    for payload, (_, row) in zip(dna_payloads, mapping_df.iterrows()):
        cro = row.get("CRO-Name")
        if cro in entry_ids:
            payload["entryId"] = entry_ids[cro]
        if cro in folder_ids:
            payload["folderId"] = folder_ids[cro]

        dna_name = payload.get("name", "").strip()
        try:
            logger.info(f"  🧬 Creating DNA sequence: {dna_name}")
            dna_result = create_dna_sequence(payload)
            dna_id = getattr(
                dna_result, "id",
                dna_result.get("id") if isinstance(dna_result, dict) else None
            )
            if dna_id:
                created_dna_sequences.append(dna_id)
                if cro in cro_assay_mapping:
                    cro_assay_mapping[cro]["dna_sequences"].append(dna_id)
                logger.info(f"  ✅ Created DNA {dna_id} for CRO: {cro}")
        except Exception as e:
            logger.error(f"  ❌ Error creating DNA sequence '{dna_name}': {e}")
            raise e

    # ── 6. Create samples (custom entities) ───────────────────────────────────
    logger.info("🧪 Creating samples...")
    custom_payloads  = custom_entity_import(custom_df, mapping_df, custom_template)
    created_entities = []

    for idx, (payload, (_, row)) in enumerate(
        zip(custom_payloads, mapping_df.iterrows())
    ):
        cro = row.get("CRO-Name")
        if cro in entry_ids:
            payload["entryId"] = entry_ids[cro]
        if cro in folder_ids:
            payload["folderId"] = folder_ids[cro]

        # Link DNA sequence to sample (row-for-row)
        if idx < len(created_dna_sequences):
            dna_id = created_dna_sequences[idx]
            if "fields" not in payload:
                payload["fields"] = {}
            payload["fields"]["Entity linked"] = {"value": dna_id}
            logger.info(f"  🔗 Linking DNA {dna_id} to Sample row {idx + 1}")

        ce_name = payload.get("name", "").strip()
        try:
            logger.info(f"  🧪 Creating Sample: {ce_name}")
            result = create_custom_entity(payload)
            ce_id  = getattr(
                result, "id",
                result.get("id") if isinstance(result, dict) else None
            )
            created_entities.append((ce_id, cro))
            logger.info(f"  ✅ Created sample {ce_id} for CRO: {cro}")
        except Exception as e:
            logger.error(f"  ❌ Error creating Sample '{ce_name}': {e}")
            raise e

    # ── 7. Inventory management ────────────────────────────────────────────────
    logger.info("\n📦 Inventory Management: Locations, Boxes, Containers...")
    storage_cache = {}

    for idx, (custom_id, cro) in enumerate(created_entities):
        row = mapping_df.iloc[idx]

        # 7a. Resolve location
        location_payload = build_payload_from_mapping(
            normalize_mapping_dataframe(location_df),
            inventory_template,
            data_row=row,
            column_types=infer_column_types(mapping_df),
        )
        location_name   = location_payload.get("name")
        location_schema = location_payload.get("schemaId")

        if location_name:
            loc_key = f"loc_{location_name}_{location_schema}"
            if loc_key not in storage_cache:
                loc_id = find_storage_by_name(location_name, location_schema)
                if not loc_id:
                    loc_result = create_location(location_payload)
                    loc_id = getattr(
                        loc_result, "id",
                        loc_result.get("id") if isinstance(loc_result, dict) else None
                    )
                storage_cache[loc_key] = loc_id
            location_id = storage_cache[loc_key]

            # 7b. Resolve box
            box_payload = build_payload_from_mapping(
                normalize_mapping_dataframe(box_df),
                inventory_template,
                data_row=row,
                column_types=infer_column_types(mapping_df),
            )
            box_name   = box_payload.get("name")
            box_schema = box_payload.get("schemaId")

            if box_name:
                box_key = f"box_{box_name}_{box_schema}_{location_id}"
                if box_key not in storage_cache:
                    box_id = find_storage_by_name(box_name, box_schema)
                    if not box_id:
                        box_payload["parentStorageId"] = location_id
                        box_result = create_box(box_payload)
                        box_id = getattr(
                            box_result, "id",
                            box_result.get("id") if isinstance(box_result, dict) else None
                        )
                    storage_cache[box_key] = box_id
                box_id = storage_cache[box_key]

                # 7c. Create container
                container_payload = build_payload_from_mapping(
                    normalize_mapping_dataframe(container_df),
                    inventory_template,
                    data_row=row,
                    column_types=infer_column_types(mapping_df),
                )

                # Convert position to well coordinate
                raw_pos = str(row.get("Position", "1")).strip()

                def pos_to_coord(p):
                    try:
                        i       = int(p)
                        row_idx = (i - 1) // 10
                        col_idx = (i - 1) % 10
                        return f"{chr(65 + row_idx)}{col_idx + 1}"
                    except Exception:
                        return p

                pos = pos_to_coord(raw_pos)
                container_payload["parentStorageId"] = f"{box_id}:{pos}"

                try:
                    qty_val = (
                        container_payload.get("fields", {})
                        .get("Quantity_mg", {})
                        .get("value")
                    )
                    qty_obj = (
                        qty_val
                        if isinstance(qty_val, dict) and "value" in qty_val
                        else {"value": qty_val, "units": "mg"}
                    )

                    allowed_keys = {
                        "name", "barcode", "schemaId",
                        "parentStorageId", "quantity", "fields", "projectId",
                    }
                    create_payload = {
                        k: v for k, v in container_payload.items()
                        if k in allowed_keys
                    }
                    if not create_payload.get("name") and "Container" in container_payload:
                        create_payload["name"] = container_payload["Container"]

                    container_result = create_container_direct(create_payload)
                    cont_id = (
                        container_result.get("id")
                        if isinstance(container_result, dict)
                        else getattr(container_result, "id", None)
                    )

                    transfer_payload = {
                        "contents": [{"entityId": custom_id, "amount": qty_obj}]
                    }
                    transfer_into_container_direct(cont_id, transfer_payload)

                    logger.info(
                        f"  ✅ Container {cont_id} filled with "
                        f"{qty_obj['value']} {qty_obj['units']} of sample {custom_id}"
                    )

                except Exception as e:
                    logger.warning(f"  ⚠️ Container error for {custom_id}: {e}")

    # ── 8. Process assay results ───────────────────────────────────────────────
    logger.info("\n📊 Creating result payloads per CRO...")

    with open("result_template.json", encoding="utf-8") as f:
        base_assay_payload = json.load(f)
    tableId = base_assay_payload.get("tableId")

    # Group samples by CRO
    cro_custom_entities: dict[str, list] = {}
    for custom_id, cro in created_entities:
        cro_custom_entities.setdefault(cro, []).append(custom_id)

    cro_result_payloads = {}
    for cro, custom_ids in cro_custom_entities.items():
        cro_data       = mapping_df[mapping_df["CRO-Name"] == cro]
        result_payloads = build_payloads_from_mapping(
            results_df, result_template, cro_data
        )

        cro_results = []
        for idx, result_payload in enumerate(result_payloads):
            if idx < len(custom_ids):
                result_payload.setdefault("fields", {})["linked_sample"] = {
                    "value": custom_ids[idx]
                }

            if "fields" in result_payload:
                result_payload["fields"] = {
                    k.lower(): v for k, v in result_payload["fields"].items()
                }

            if not result_payload.get("projectId") and cro in project_ids:
                result_payload["projectId"] = project_ids[cro]

            cro_results.append(result_payload)

        cro_result_payloads[cro] = {
            "assayResults": cro_results,
            "tableId":      tableId,
        }

    # ── 9. Upload results to Benchling ────────────────────────────────────────
    logger.info("\n📋 Uploading results...")
    result_tables_cfg   = config.get("result_tables", {})
    default_table_id    = result_tables_cfg.get("default_table_id", tableId)

    for cro, entry_id in entry_ids.items():
        cro_payload = cro_result_payloads.get(cro)
        if not cro_payload:
            continue

        # Get the result table ID from the entry
        try:
            target_table_id = get_result_table_id_from_entry(entry_id) or default_table_id
        except Exception:
            target_table_id = default_table_id

        cro_payload["tableId"] = target_table_id
        logger.info(f"  📤 Uploading results for {cro} to Table {target_table_id}")

        try:
            response = create_assay_results_bulk(cro_payload)
            logger.info(f"     ✅ Success! Response: {response}")
        except Exception as e:
            logger.error(f"     ❌ Error uploading results for {cro}: {e}")
            raise


if __name__ == "__main__":
    main()