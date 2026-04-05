"""
ai/mapping_assistant.py
Dynamically maps ANY uploaded file's columns to live Benchling ERD fields.
No hardcoded file paths — always uses the file uploaded via the UI.
"""

import pandas as pd
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# SET THIS TO False WHEN YOU HAVE API KEY
USE_MOCK = True
# ─────────────────────────────────────────

# Always reads from env var set by backend when user uploads a file
MAPPING_FILE     = "CRO Mapping.xlsx"
APPROVED_MAPPING = "ai/approved_mapping.json"
ERD_FILE         = "ai/benchling_erd.json"

SCHEMAS = ["Entry", "Sample", "DNA Sequence", "Results", "Location", "Box", "Container"]

# Maps pipeline schema names → real Benchling schema names in ERD
SCHEMA_TO_BENCHLING = {
    "Sample":       "Sample",
    "DNA Sequence": "DNA_Sequence_POC",
    "Results":      "Results-Demo",
    "Container":    "SV Test Tubes",
    "Entry":        None,
    "Location":     None,
    "Box":          None,
}


def get_data_file() -> str:
    """
    Always returns the file uploaded by the user via the UI.
    Falls back to env var, then upload folder, then legacy file.
    """
    # Priority 1: set by backend when user uploads
    env_file = os.getenv("HARMONIZED_FILE")
    if env_file and os.path.exists(env_file):
        return env_file

    # Priority 2: check uploads folder
    for ext in [".xlsx", ".csv"]:
        p = os.path.join("uploads", f"harmonized_upload{ext}")
        if os.path.exists(p):
            return p

    # Priority 3: legacy fallback (only for CLI usage)
    return "Harmonized dataset_new.xlsx"


# ─── Load ERD ─────────────────────────────────────────────────────────────────

def load_erd() -> dict:
    if not os.path.exists(ERD_FILE):
        print("  ⚠️  ERD not found. Run 'python run_erd_fetch.py' first.")
        return {}
    with open(ERD_FILE) as f:
        return json.load(f)


def get_benchling_fields(schema_name: str, erd: dict) -> dict:
    benchling_name = SCHEMA_TO_BENCHLING.get(schema_name)
    if not benchling_name:
        return {}
    for s in erd.get("schemas", []):
        if s["name"] == benchling_name:
            return {
                f["name"]: {
                    "type":     f.get("type", "unknown"),
                    "required": f.get("required", False),
                    "field_id": f.get("field_id", ""),
                }
                for f in s.get("fields", [])
                if not f.get("archived", False)
            }
    return {}


# ─── Read uploaded file columns ───────────────────────────────────────────────

def get_uploaded_columns() -> list:
    """Read column names from whatever file the user uploaded."""
    data_file = get_data_file()
    try:
        if data_file.endswith(".xlsx"):
            df = pd.read_excel(data_file, nrows=1)
        else:
            df = pd.read_csv(data_file, nrows=1)
        return df.columns.tolist()
    except Exception as e:
        print(f"  ⚠️  Could not read uploaded file: {e}")
        return []


# ─── Read CRO Mapping sheet (kept for CLI fallback only) ──────────────────────

def read_mapping_sheet(sheet_name):
    try:
        df = pd.read_excel(MAPPING_FILE, sheet_name=sheet_name, header=None)
        if df.empty or df.shape[1] < 3:
            return []
        mappings = []
        for _, row in df.iterrows():
            row_vals = [str(v).strip() if pd.notna(v) else "" for v in row]
            if row_vals[0] in ["Entity Attributes", "nan", ""]:
                continue
            mappings.append({
                "benchling_field": row_vals[0],
                "is_input_column": row_vals[1].lower() == "yes" if len(row_vals) > 1 else False,
                "api_value":       row_vals[2] if len(row_vals) > 2 else "",
                "input_column":    row_vals[3] if len(row_vals) > 3 else "",
            })
        return mappings
    except Exception as e:
        print(f"  ⚠️  Could not read sheet '{sheet_name}': {e}")
        return []


# ─── Real Claude AI suggestion ────────────────────────────────────────────────

def ask_claude(benchling_fields, uploaded_columns, schema_name):
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        prompt = f"""
You are a data mapping expert for a Benchling bioinformatics pipeline.
Schema: {schema_name}
Benchling fields that need values from the uploaded data file:
{json.dumps(benchling_fields, indent=2)}
Available columns in the uploaded file:
{json.dumps(uploaded_columns, indent=2)}
For each Benchling field suggest the best matching uploaded column.
Return ONLY a JSON array (no explanation, no markdown):
[
  {{
    "benchling_field": "name",
    "suggested_column": "Sample_Name",
    "confidence": 85,
    "reason": "Both refer to sample identifier"
  }}
]
Rules:
- confidence is 0-100
- If no good match exists set suggested_column to null and confidence to 0
"""
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()
        return json.loads(raw)
    except Exception as e:
        print(f"  ⚠️  Claude API error: {e}")
        return []


# ─── Mock Claude suggestion ───────────────────────────────────────────────────

def mock_claude_suggest(benchling_fields: list, uploaded_columns: list, schema_name: str) -> list:
    """
    Dynamically maps Benchling fields to any uploaded file's columns.
    Works with any column names — not hardcoded to specific file structure.
    """

    # Schema-specific name overrides — confirmed by user
    schema_name_mapping = {
        "Sample":       ("Sample_Name",      99, "Confirmed: Sample name → Sample_Name"),
        "DNA Sequence": ("Construct_Name",   99, "Confirmed: DNA Sequence name → Construct_Name"),
        "Location":     ("Storage_Location", 99, "Confirmed: Location name → Storage_Location"),
        "Box":          ("Box",              99, "Confirmed: Box name → Box column"),
        "Container":    ("Storage_Location", 75, "Best guess: Container → Storage_Location"),
        "Entry":        ("CRO-Name",         90, "Entry name → CRO-Name"),
        "Results":      ("Assay_ID",         90, "Results name → Assay_ID"),
    }

    # Keyword matching rules
    keyword_map = {
        "name":               ("Sample_Name",       88, "Name fields map to sample name"),
        "bases":              ("Sequence",           97, "'bases' is DNA sequence data"),
        "sequence":           ("Sequence",           97, "Direct name match"),
        "sampleid":           ("Sample_ID",          99, "Exact match on ID field"),
        "batch":              ("Batch_ID",           95, "Batch identifier match"),
        "folder":             (None,                  0, "Hardcoded — no uploaded column needed"),
        "template":           (None,                  0, "Hardcoded — no uploaded column needed"),
        "program":            ("Program",            99, "Exact column name match"),
        "target":             ("Target",             99, "Exact column name match"),
        "linker":             ("Linker_Type",        95, "Linker field match"),
        "dar":                ("DAR",                99, "Exact match"),
        "conjugation":        ("Conjugation_Method", 95, "Conjugation method match"),
        "qc":                 ("QC_Status",          92, "QC status match"),
        "compound":           ("Compound_Name",      95, "Compound name match"),
        "smiles":             ("SMILES",             99, "Exact match"),
        "molecular_weight":   ("Molecular_Weight",   99, "Exact match"),
        "supplier":           ("Supplier",           99, "Exact match"),
        "construct":          ("Construct_Name",     95, "Construct name match"),
        "vector":             ("Vector",             99, "Exact match"),
        "sequence_length":    ("Sequence_Length",    99, "Exact match"),
        "gc_content":         ("GC_Content",         99, "Exact match"),
        "host":               ("Host_System",        92, "Host system match"),
        "manufacturing_date": ("Manufacturing_Date", 99, "Exact match"),
        "expiry":             ("Expiry_Date",        95, "Expiry date match"),
        "manufacturer":       ("Manufacturer",       99, "Exact match"),
        "purity":             ("Purity_Percent",     92, "Purity percentage match"),
        "storage_condition":  ("Storage_Condition",  99, "Exact match"),
        "storage_location":   ("Storage_Location",   99, "Exact match"),
        "box":                ("Box",                99, "Exact match"),
        "position":           ("Position",           99, "Exact match"),
        "quantity":           ("Quantity_mg",        90, "Quantity field match"),
        "concentration":      ("Concentration",      99, "Exact match"),
        "assay_id":           ("Assay_ID",           99, "Exact match"),
        "assay_type":         ("Assay_Type",         99, "Exact match"),
        "method":             ("Method",             99, "Exact match"),
        "result_value":       ("Result_Value",       99, "Exact match"),
        "result_unit":        ("Result_Unit",        99, "Exact match"),
        "replicate":          ("Replicate",          99, "Exact match"),
        "analyst":            ("Analyst",            99, "Exact match"),
        "internal_sample":    ("Sample_ID",          95, "Internal sample ID → Sample_ID"),
        "external_specimen":  ("Sample_Name",        85, "External specimen → Sample_Name"),
        "cro":                ("CRO-Name",           95, "CRO field → CRO-Name"),
    }

    # Build lowercase lookup from uploaded columns
    uploaded_lower = {c.lower(): c for c in uploaded_columns}
    suggestions = []

    for field in benchling_fields:
        field_lower = field.lower().replace(" ", "_")
        matched = False

        # Priority 1: Schema-specific override for 'name' field
        if field_lower == "name" and schema_name in schema_name_mapping:
            col, conf, reason = schema_name_mapping[schema_name]
            # Only use if column actually exists in uploaded file
            actual_col = col if col in uploaded_columns else None
            actual_conf = conf if actual_col else 0
            actual_reason = reason if actual_col else f"Column '{col}' not found in uploaded file"
            suggestions.append({
                "benchling_field":  field,
                "suggested_column": actual_col,
                "confidence":       actual_conf,
                "reason":           actual_reason,
                "status":           "auto" if actual_col and actual_conf >= 90 else "missing"
            })
            continue

        # Priority 2: Exact match against uploaded columns
        if field_lower in uploaded_lower:
            suggestions.append({
                "benchling_field":   field,
                "suggested_column":  uploaded_lower[field_lower],
                "confidence":        99,
                "reason":            "Exact column name match",
                "status":            "auto"
            })
            matched = True

        # Priority 3: Keyword map — only if column exists in uploaded file
        if not matched:
            for keyword, (col, conf, reason) in keyword_map.items():
                if keyword in field_lower:
                    # Check if the suggested column actually exists
                    actual_col = col if col in uploaded_columns else None
                    if actual_col:
                        suggestions.append({
                            "benchling_field":  field,
                            "suggested_column": actual_col,
                            "confidence":       conf,
                            "reason":           reason,
                            "status":           "auto" if conf >= 90 else "review"
                        })
                    else:
                        # Try fuzzy match against uploaded columns
                        fuzzy = next(
                            (c for c in uploaded_columns
                             if keyword in c.lower() or c.lower() in keyword),
                            None
                        )
                        if fuzzy:
                            suggestions.append({
                                "benchling_field":  field,
                                "suggested_column": fuzzy,
                                "confidence":       70,
                                "reason":           f"Fuzzy match: '{fuzzy}' resembles '{field}'",
                                "status":           "review"
                            })
                        else:
                            suggestions.append({
                                "benchling_field":  field,
                                "suggested_column": None,
                                "confidence":       0,
                                "reason":           f"No matching column in uploaded file",
                                "status":           "missing"
                            })
                    matched = True
                    break

        # Priority 4: No match found
        if not matched:
            suggestions.append({
                "benchling_field":  field,
                "suggested_column": None,
                "confidence":       0,
                "reason":           "No matching column found in uploaded file",
                "status":           "missing"
            })

    return suggestions


# ─── Detect column changes ────────────────────────────────────────────────────

def detect_column_changes():
    if not os.path.exists(APPROVED_MAPPING):
        return []
    with open(APPROVED_MAPPING) as f:
        approved = json.load(f)
    current_cols = set(get_uploaded_columns())
    previously_used = set()
    for schema_data in approved.values():
        for item in schema_data:
            col = item.get("suggested_column") or item.get("approved_column")
            if col:
                previously_used.add(col)
    return list(previously_used - current_cols)


# ─── Save approved mapping ────────────────────────────────────────────────────

def save_approved_mapping(all_suggestions):
    os.makedirs("ai", exist_ok=True)
    with open(APPROVED_MAPPING, "w") as f:
        json.dump(all_suggestions, f, indent=2)
    print(f"\n  💾 Approved mapping saved to: {APPROVED_MAPPING}")


# ─── Main analysis ────────────────────────────────────────────────────────────

def analyze_mapping(schema_name, erd: dict = None):
    print(f"\n{'='*55}")
    print(f"  📋 Analyzing Schema: {schema_name}")
    print(f"{'='*55}")

    uploaded_cols = get_uploaded_columns()
    data_file     = get_data_file()
    print(f"  📂 Data file: {os.path.basename(data_file)}")
    print(f"  📊 Columns in uploaded file: {len(uploaded_cols)}")

    benchling_fields_def = get_benchling_fields(schema_name, erd or {})
    if not benchling_fields_def:
        print(f"  ℹ️  No ERD data for '{schema_name}'")
        return []

    print(f"  🧬 Benchling fields ({len(benchling_fields_def)}): {list(benchling_fields_def.keys())}")
    required = [f for f, d in benchling_fields_def.items() if d["required"]]
    if required:
        print(f"  ⚠️  Required: {required}")

    input_fields = list(benchling_fields_def.keys())

    print(f"\n  🤖 {'[MOCK MODE]' if USE_MOCK else '[CLAUDE AI]'} Generating suggestions...")
    suggestions = mock_claude_suggest(input_fields, uploaded_cols, schema_name) \
                  if USE_MOCK else ask_claude(input_fields, uploaded_cols, schema_name)

    # Enrich with ERD type info
    for s in suggestions:
        fd = benchling_fields_def.get(s["benchling_field"], {})
        s["benchling_type"]     = fd.get("type", "unknown")
        s["benchling_required"] = fd.get("required", False)
        s["benchling_field_id"] = fd.get("field_id", "")

    # Print results
    print(f"\n  {'─'*50}")
    auto = [s for s in suggestions if s.get("status") == "auto"]
    review = [s for s in suggestions if s.get("status") in ["review", "missing"]]

    for s in suggestions:
        conf   = s["confidence"]
        col    = s["suggested_column"]
        field  = s["benchling_field"]
        status = s.get("status", "missing")
        icon   = "✅ AUTO" if status == "auto" else "⚠️  REVIEW" if status == "review" else "❌ MISSING"
        print(f"  {icon}  {field:<25} → {str(col):<25} [{conf}%]")
        print(f"           {s['reason']}")

    print(f"\n  ✅ Auto: {len(auto)} | ⚠️  Review: {len(review)}")
    return suggestions


def run_full_analysis():
    print("\n" + "🚀 " * 20)
    print("  BENCHLING MAPPING ASSISTANT — DYNAMIC FILE")
    print(f"  Mode: {'🟡 MOCK' if USE_MOCK else '🟢 CLAUDE AI'}")
    print(f"  File: {os.path.basename(get_data_file())}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🚀 " * 20)

    erd = load_erd()
    if erd:
        print(f"\n  🧬 ERD: {erd.get('schema_count', 0)} schemas")

    changes = detect_column_changes()
    if changes:
        print(f"\n⚠️  Columns used before but now missing: {changes}")

    uploaded_cols = get_uploaded_columns()
    print(f"\n  📊 Uploaded file has {len(uploaded_cols)} columns")

    all_results = {}
    for schema in SCHEMAS:
        suggestions = analyze_mapping(schema, erd)
        if suggestions:
            all_results[schema] = suggestions

    save_approved_mapping(all_results)

    total  = sum(len(v) for v in all_results.values())
    auto   = sum(1 for v in all_results.values()
                 for s in v if s.get("status") == "auto")
    review = total - auto

    print(f"\n{'='*55}")
    print(f"  📊 SUMMARY")
    print(f"{'='*55}")
    print(f"  File     : {os.path.basename(get_data_file())}")
    print(f"  Schemas  : {len(all_results)}")
    print(f"  Fields   : {total}")
    print(f"  Auto     : {auto} ✅")
    print(f"  Review   : {review} ⚠️")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    run_full_analysis()