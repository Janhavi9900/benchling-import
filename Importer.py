import json
import logging
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

# ---------------------------------------------------
# LOGGING CONFIGURATION
# ---------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("output.log", mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
) 
logger = logging.getLogger(__name__)


# ---------------------------------------------------
# ENTRY IMPORT: Processes logic for Notebook Entries
# ---------------------------------------------------
def entry_import(
    entry_df: pd.DataFrame,
    input_df: pd.DataFrame,
    template_path: str,
) -> dict:
    """
    Constructs a payload for a Notebook Entry using a mapping sheet and template.
    Uses the first row of data to define entry-level attributes.
    """
    if input_df.empty:
        raise ValueError("Input data for entries is empty.")

    mapping = normalize_mapping_dataframe(entry_df)
    column_types = infer_column_types(input_df)

    # Get first row of input data for entry-level metadata
    first_row = input_df.iloc[0]

    # Build payload using the payload_builder utility
    payload = build_payload_from_mapping(
        mapping,
        template_path,
        data_row=first_row,
        column_types=column_types,
    )

    # Fallback: ensure the entry has a name (defaults to CRO Name)
    if not payload.get('name'):
        cro_name = first_row.get('CRO-Name', 'Unknown Entry')
        payload['name'] = str(cro_name)

    return payload


# ---------------------------------------------------
# DNA IMPORT: Processes logic for DNA Sequences
# ---------------------------------------------------
def dna_sequence_import(
    dna_sequence_df: pd.DataFrame,
    input_df: pd.DataFrame,
    template_path: str,
) -> List[dict]:
    """
    Builds a list of DNA sequence payloads from the input data.
    """
    payloads = build_payloads_from_mapping(
        dna_sequence_df,
        template_path,
        input_df,
    )
    return payloads


# ---------------------------------------------------
# CUSTOM ENTITY IMPORT: Processes logic for Samples
# ---------------------------------------------------
def custom_entity_import(
    custom_df: pd.DataFrame,
    input_df: pd.DataFrame,
    template_path: str,
) -> List[dict]:
    """
    Builds a list of Custom Entity (Sample) payloads from the input data.
    """
    payloads = build_payloads_from_mapping(
        custom_df,
        template_path,
        input_df,
    )
    return payloads


# ---------------------------------------------------
# MAIN PIPELINE: Orchestrates the entire import flow
# ---------------------------------------------------
def main(
    file_path: str | None = None,
    mapping_file_path: str | None = None,
) -> None:
    """
    1. Reads configuration and Excel data.
    2. Creates CRO-specific folders.
    3. Creates Notebook Entries (one per CRO).
    4. Creates DNA sequences and Samples, linking DNA to Samples.
    5. Manages Inventory (Locations -> Boxes -> Containers).
    6. Uploads assay results with sample linking.
    """
    # Load global configuration (API keys, project IDs, sheet names)
    config = load_config()

    excel_cfg = config.get("excel", {})
    templates_cfg = config.get("templates", {})
    benchling_cfg = config.get("benchling", {})

    # Defaults to paths in config if not provided as arguments
    file_path = file_path or excel_cfg.get("mapping_workbook")
    mapping_file_path = mapping_file_path or excel_cfg.get("data_workbook")

    if not file_path or not mapping_file_path:
        raise ValueError("Mapping and data workbook paths missing.")

    # Sheet names and template paths
    entry_sheet = excel_cfg.get("entry_sheet_name", "Entry")
    dna_sheet = excel_cfg.get("dna_sheet_name", "DNA Sequence")
    custom_sheet = excel_cfg.get("custom_sheet_name", "Sample")
    results_sheet = excel_cfg.get("results_sheet_name", "Results")
    location_sheet = excel_cfg.get("location_sheet_name", "Location")
    box_sheet = excel_cfg.get("box_sheet_name", "Box")
    container_sheet = excel_cfg.get("container_sheet_name", "Container")

    entry_template = templates_cfg.get("entry_template")
    dna_template = templates_cfg.get("dna_template")
    custom_template = templates_cfg.get("custom_template")
    result_template = templates_cfg.get("result_template", "result_template.json")
    inventory_template = "inventory_template.json"

    parent_folder_id = benchling_cfg.get("parent_folder_id")
    if not parent_folder_id:
        raise ValueError("parent_folder_id not configured.")

    # ---------------- 1. READ EXCEL DATA ----------------
    sheets = pd.read_excel(
        file_path,
        sheet_name=[entry_sheet, dna_sheet, custom_sheet, results_sheet, location_sheet, box_sheet, container_sheet],
    )

    entry_df = sheets[entry_sheet]
    dna_df = sheets[dna_sheet]
    custom_df = sheets[custom_sheet]
    results_df = sheets[results_sheet]
    location_df = sheets[location_sheet]
    box_df = sheets[box_sheet]
    container_df = sheets[container_sheet]

    # The actual data rows to be imported
    mapping_df = pd.read_excel(mapping_file_path)

    # ---------------- 2. CREATE FOLDERS PER CRO ----------------
    unique_cros = mapping_df['CRO-Name'].dropna().unique()
    folder_ids = {}
    project_ids = {} # Track project IDs for each CRO folder to aid result uploads
    for cro in unique_cros:
        folder_payload = {"name": str(cro), "parentFolderId": parent_folder_id}
        folder = create_folder(folder_payload)
        f_id = getattr(folder, "id", folder.get("id"))
        folder_ids[cro] = f_id
        
        # Extract projectId from the created folder for context
        p_id = getattr(folder, "project_id", None)
        if p_id is None and isinstance(folder, dict):
            p_id = folder.get("projectId")
        project_ids[cro] = p_id
            
        logger.info(f"✅ Created folder for CRO: {cro} (ID: {f_id}, Project: {p_id})")

    # ---------------- 3. CREATE NOTEBOOK ENTRIES (ONE PER CRO) ----------------
    entry_ids = {}
    for cro in unique_cros:
        # Each CRO gets exactly one entry containing all its data
        cro_data = mapping_df[mapping_df['CRO-Name'] == cro]
        if cro_data.empty:
            continue

        entry_payload = entry_import(
            entry_df,
            input_df=cro_data,
            template_path=entry_template,
        )

        # Force Entry Name to be identical to CRO Name
        entry_payload['name'] = str(cro)
        if cro in folder_ids:
            entry_payload['folderId'] = folder_ids[cro]

        # Use bulk or single creation via SDK
        entry_result = create_entry(entry_payload)
        entry_id = getattr(entry_result, "id", None)
        if entry_id is None and isinstance(entry_result, dict):
            entry_id = entry_result.get("id")

        if not entry_id:
            raise RuntimeError(f"Entry creation failed for CRO: {cro}")

        entry_ids[cro] = entry_id
        logger.info(f"✅ Created Entry ID: {entry_id} for CRO: {cro}")


    # ---------------- 4. BUILD ASSAY MAPPING FOR LINKING ----------------
    cro_assay_mapping = {}
    for cro in unique_cros:
        cro_data = mapping_df[mapping_df['CRO-Name'] == cro]
        if 'Assay_ID' in cro_data.columns:
            assay_ids = cro_data['Assay_ID'].dropna().unique().tolist()
            cro_assay_mapping[cro] = {
                'entry_id': entry_ids.get(cro),
                'assay_ids': assay_ids,
                'dna_sequences': cro_data.get('Sequence_ID', []).tolist() if 'Sequence_ID' in cro_data.columns else []
            }
        else:
            cro_assay_mapping[cro] = {'entry_id': entry_ids.get(cro), 'assay_ids': [], 'dna_sequences': []}

    # ---------------- 5. CREATE DNA SEQUENCES ----------------
    dna_payloads = dna_sequence_import(
        dna_df,
        mapping_df,
        dna_template,
    )

    created_dna_sequences = []  # Ordered list to match with samples
    for payload, (_, row) in zip(dna_payloads, mapping_df.iterrows()):
        cro = row.get('CRO-Name')
        if cro in entry_ids:
            payload['entryId'] = entry_ids[cro]
        if cro in folder_ids:
            payload['folderId'] = folder_ids[cro]

        dna_name = payload.get("name", "").strip()
        try:
            logger.info(f"  🧬 Creating new DNA sequence: {dna_name}")
            dna_result = create_dna_sequence(payload)
            dna_id = getattr(dna_result, "id", dna_result.get("id") if isinstance(dna_result, dict) else None)
            
            if dna_id:
                created_dna_sequences.append(dna_id)
                if cro in cro_assay_mapping:
                    cro_assay_mapping[cro]['dna_sequences'].append(dna_id)
                logger.info(f"  ✅ Created DNA sequence {dna_id} for CRO: {cro}")
        except Exception as e:
            logger.error(f"  ❌ Error creating DNA sequence '{dna_name}': {e}")
            raise e


    # ---------------- 6. CREATE SAMPLES (CUSTOM ENTITIES) ----------------
    custom_payloads = custom_entity_import(
        custom_df,
        mapping_df,
        custom_template,
    )

    created_custom_entities = []  # List of (id, cro) for result generation logic
    for idx, (payload, (_, row)) in enumerate(zip(custom_payloads, mapping_df.iterrows())):
        cro = row.get('CRO-Name')
        if cro in entry_ids:
            payload['entryId'] = entry_ids[cro]
        if cro in folder_ids:
            payload['folderId'] = folder_ids[cro]

        # LINK DNA SEQUENCE TO SAMPLE (Row-for-row match)
        if idx < len(created_dna_sequences):
            dna_id = created_dna_sequences[idx]
            if "fields" not in payload:
                payload["fields"] = {}
            payload["fields"]["Entity linked"] = {"value": dna_id}
            logger.info(f"  🔗 Linking DNA {dna_id} to Sample for row {idx+1}")

        ce_name = payload.get("name", "").strip()
        try:
            logger.info(f"  🧪 Creating new Sample: {ce_name}")
            custom_result = create_custom_entity(payload)
            custom_id = getattr(custom_result, "id", custom_result.get("id") if isinstance(custom_result, dict) else None)
            
            created_custom_entities.append((custom_id, cro))
            logger.info(f"  ✅ Created custom entity {custom_id} for CRO: {cro}")
        except Exception as e:
            logger.error(f"  ❌ Error creating Sample '{ce_name}': {e}")
            raise e

    # ---------------- 7. INVENTORY MANAGEMENT (LOCATIONS, BOXES, CONTAINERS) ----------------
    print("\n📦 Inventory Management: Locations, Boxes, and Containers...")
    storage_cache = {} # Cache to reuse existing storage items

    for idx, (custom_id, cro) in enumerate(created_custom_entities):
        row = mapping_df.iloc[idx]
        
        # 7.1 Resolve Location
        location_payload = build_payload_from_mapping(
            normalize_mapping_dataframe(location_df),
            inventory_template,
            data_row=row,
            column_types=infer_column_types(mapping_df)
        )
        location_name = location_payload.get("name")
        location_schema = location_payload.get("schemaId")
        
        if location_name:
            loc_key = f"loc_{location_name}_{location_schema}"
            if loc_key not in storage_cache:
                loc_id = find_storage_by_name(location_name, location_schema)
                if not loc_id:
                    loc_result = create_location(location_payload)
                    loc_id = getattr(loc_result, "id", loc_result.get("id"))
                storage_cache[loc_key] = loc_id
            location_id = storage_cache[loc_key]
            
            # 7.2 Resolve Box
            box_payload = build_payload_from_mapping(
                normalize_mapping_dataframe(box_df),
                inventory_template,
                data_row=row,
                column_types=infer_column_types(mapping_df)
            )
            box_name = box_payload.get("name")
            box_schema = box_payload.get("schemaId")
            
            if box_name:
                box_key = f"box_{box_name}_{box_schema}_{location_id}"
                if box_key not in storage_cache:
                    box_id = find_storage_by_name(box_name, box_schema)
                    if not box_id:
                        box_payload["parentStorageId"] = location_id
                        box_result = create_box(box_payload)
                        box_id = getattr(box_result, "id", box_result.get("id"))
                    storage_cache[box_key] = box_id
                box_id = storage_cache[box_key]
                
                # 7.3 Create Container and Populate
                container_payload = build_payload_from_mapping(
                    normalize_mapping_dataframe(container_df),
                    inventory_template,
                    data_row=row,
                    column_types=infer_column_types(mapping_df)
                )
                
                # Convert numeric index to 96-well coordinates
                raw_pos = str(row.get("Position", "1")).strip()
                def pos_to_coord(p):
                    try:
                        idx = int(p)
                        row_idx = (idx - 1) // 10
                        col_idx = (idx - 1) % 10
                        char = chr(65 + row_idx)
                        return f"{char}{col_idx + 1}"
                    except: return p
                
                pos = pos_to_coord(raw_pos)
                container_payload["parentStorageId"] = f"{box_id}:{pos}"
                
                try:
                    # Resolve quantity safely from 'Quantity_mg' field
                    qty_val = container_payload.get("fields", {}).get("Quantity_mg", {}).get("value")
                    qty_obj = qty_val if (isinstance(qty_val, dict) and "value" in qty_val) else {"value": qty_val, "units": "mg"}
                    
                    # Clean payload for creation (Benchling rejects unknown top-level keys)
                    allowed_keys = {"name", "barcode", "schemaId", "parentStorageId", "quantity", "fields", "projectId"}
                    create_payload = {k: v for k, v in container_payload.items() if k in allowed_keys}
                    if not create_payload.get("name") and "Container" in container_payload:
                        create_payload["name"] = container_payload["Container"]

                    # Execute creation then transfer
                    container_result = create_container_direct(create_payload)
                    cont_id = container_result.get("id") if isinstance(container_result, dict) else getattr(container_result, "id", None)
                    
                    transfer_payload = {"contents": [{"entityId": custom_id, "amount": qty_obj}]}
                    transfer_into_container_direct(cont_id, transfer_payload)
                    
                    logger.info(f"  ✅ Created Container {cont_id} filled with {qty_obj['value']} {qty_obj['units']} of sample {custom_id}")
                except Exception as e:
                    logger.warning(f"  ⚠️ Error creating/filling container for {custom_id}: {e}")

    # ---------------- 8. PROCESS ASSAY RESULTS ----------------
    with open("result_template.json", encoding="utf-8") as f:
        base_assay_payload = json.load(f)
    tableId = base_assay_payload.get("tableId")

    logger.info("\n📊 Creating result payloads per CRO (linked to samples):")

    # Group created samples by CRO for result association
    cro_custom_entities = {}
    for custom_id, cro in created_custom_entities:
        if cro not in cro_custom_entities:
            cro_custom_entities[cro] = []
        cro_custom_entities[cro].append(custom_id)

    # Build and group results logic
    cro_result_payloads = {}
    for cro, custom_ids in cro_custom_entities.items():
        cro_data = mapping_df[mapping_df['CRO-Name'] == cro]
        result_payloads = build_payloads_from_mapping(results_df, result_template, cro_data)

        cro_results = []
        for idx, result_payload in enumerate(result_payloads):
            # Link Result to matching Sample
            if idx < len(custom_ids):
                custom_id = custom_ids[idx]
                result_payload.setdefault("fields", {})["linked_sample"] = {"value": custom_id}

            # Field names must be lowercase for Results API
            if "fields" in result_payload:
                result_payload["fields"] = {k.lower(): v for k, v in result_payload["fields"].items()}

            if not result_payload.get('projectId') and cro in project_ids:
                result_payload['projectId'] = project_ids[cro]

            cro_results.append(result_payload)

        cro_result_payloads[cro] = {"assayResults": cro_results, "tableId": tableId}

    # ---------------- 9. UPLOAD RESULTS TO DYNAMIC TABLES ----------------
    print("\n📋 Finalizing entry result table IDs and Uploading...")
    result_tables_cfg = config.get("result_tables", {})
    cro_table_mapping = result_tables_cfg.get("cro_table_mapping", {})
    default_table_id = result_tables_cfg.get("default_table_id", tableId)

    for cro, entry_id in entry_ids.items():
        # Resolve the specific Table ID within the Notebook Entry
        target_table_id = get_result_table_id_from_entry(entry_id) or cro_table_mapping.get(cro, default_table_id)
        
        cro_payload = cro_result_payloads.get(cro)
        if not cro_payload: continue
        
        cro_payload['tableId'] = target_table_id
        logger.info(f"  📤 Uploading results for {cro} to Table {target_table_id}")

        try:
            assay_response = create_assay_results_bulk(cro_payload)
            logger.info(f"     ✅ Success! Response: {assay_response}")
        except Exception as e:
            logger.error(f"     ⚠️  Error uploading results for {cro}: {str(e)}")
            raise

if __name__ == "__main__":
    main()
