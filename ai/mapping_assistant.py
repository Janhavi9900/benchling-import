"""
ai/mapping_assistant.py
Analyzes CRO Mapping.xlsx and Harmonized dataset and suggests
intelligent column mappings using Claude AI (or mock mode).
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

MAPPING_FILE      = "CRO Mapping.xlsx"
HARMONIZED_FILE   = "Harmonized dataset_new.xlsx"
APPROVED_MAPPING  = "ai/approved_mapping.json"

SCHEMAS = ["Entry", "Sample", "DNA Sequence", "Results", "Location", "Box", "Container"]


# ─── STEP 1: Read CRO Mapping sheet ───────────────────────────────────────────

def read_mapping_sheet(sheet_name):
    """Read a sheet from CRO Mapping.xlsx and return structured mapping rows."""
    try:
        df = pd.read_excel(MAPPING_FILE, sheet_name=sheet_name, header=None)
        if df.empty or df.shape[1] < 3:
            return []

        mappings = []
        for _, row in df.iterrows():
            row_vals = [str(v).strip() if pd.notna(v) else "" for v in row]
            # Skip header rows
            if row_vals[0] in ["Entity Attributes", "nan", ""]:
                continue
            entry = {
                "benchling_field": row_vals[0],
                "is_input_column": row_vals[1].lower() == "yes" if len(row_vals) > 1 else False,
                "api_value":       row_vals[2] if len(row_vals) > 2 else "",
                "input_column":    row_vals[3] if len(row_vals) > 3 else "",
            }
            mappings.append(entry)
        return mappings
    except Exception as e:
        print(f"  ⚠️  Could not read sheet '{sheet_name}': {e}")
        return []


# ─── STEP 2: Read Harmonized columns ──────────────────────────────────────────

def get_harmonized_columns():
    """Return list of column names from the harmonized file."""
    df = pd.read_excel(HARMONIZED_FILE, nrows=1)
    return df.columns.tolist()


# ─── STEP 3: Claude AI suggestion (real) ──────────────────────────────────────

def ask_claude(benchling_fields, harmonized_columns, schema_name):
    """Call real Claude API to suggest mappings."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        prompt = f"""
You are a data mapping expert for a Benchling bioinformatics pipeline.

Schema: {schema_name}

Benchling fields that need values from input data:
{json.dumps(benchling_fields, indent=2)}

Available columns in the harmonized input file:
{json.dumps(harmonized_columns, indent=2)}

For each Benchling field, suggest the best matching harmonized column.
Return ONLY a JSON array like this (no explanation, no markdown):
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
- If no good match exists, set suggested_column to null and confidence to 0
- Be specific about reasons
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


# ─── STEP 4: Mock Claude suggestion (no API key needed) ───────────────────────

def mock_claude_suggest(benchling_fields, harmonized_columns, schema_name):
    """
    Simulates Claude AI suggestions using smart keyword matching.
    Replace with ask_claude() when API key is ready.
    """
    # Schema-specific overrides — confirmed by user
    schema_name_mapping = {
        "Sample":       ("Sample_Name",      99, "Confirmed: Sample name → Sample_Name"),
        "DNA Sequence": ("Construct_Name",   99, "Confirmed: DNA Sequence name → Construct_Name"),
        "Location":     ("Storage_Location", 99, "Confirmed: Location name → Storage_Location"),
        "Box":          ("Box",              99, "Confirmed: Box name → Box column"),
        "Container":    ("Storage_Location", 75, "Best guess: Container → Storage_Location, verify"),
        "Entry":        ("CRO-Name",         90, "Entry name → CRO-Name"),
        "Results":      ("Assay_ID",         90, "Results name → Assay_ID"),
    }

    # Smart keyword matching rules
    keyword_map = {
        "name":               ("Sample_Name",       88, "Name fields typically map to sample name"),
        "bases":              ("Sequence",           97, "'bases' is DNA sequence data — exact match"),
        "sequence":           ("Sequence",           97, "Direct name match"),
        "sampleid":           ("Sample_ID",          99, "Exact match on ID field"),
        "batch":              ("Batch_ID",           95, "Batch identifier match"),
        "folder":             (None,                  0, "Hardcoded value — no harmonized column needed"),
        "template":           (None,                  0, "Hardcoded value — no harmonized column needed"),
        "program":            ("Program",            99, "Exact column name match"),
        "target":             ("Target",             99, "Exact column name match"),
        "linker":             ("Linker_Type",        95, "Linker field match"),
        "dar":                ("DAR",                99, "Exact match"),
        "conjugation":        ("Conjugation_Method", 95, "Conjugation method match"),
        "qc":                 ("QC_Status",          92, "QC status match"),
        "compound":           ("Compound_Name",      95, "Compound name match"),
        "smiles":             ("SMILES",             99, "Exact match — SMILES notation"),
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
    }

    suggestions = []
    harmonized_lower = {c.lower(): c for c in harmonized_columns}

    for field in benchling_fields:
        field_lower = field.lower().replace(" ", "_")
        matched = False

        # ── Priority 1: Schema-specific override for 'name' field ──
        if field_lower == "name" and schema_name in schema_name_mapping:
            col, conf, reason = schema_name_mapping[schema_name]
            suggestions.append({
                "benchling_field":  field,
                "suggested_column": col,
                "confidence":       conf,
                "reason":           reason
            })
            continue

        # ── Priority 2: Exact match ──
        if field_lower in harmonized_lower:
            suggestions.append({
                "benchling_field":   field,
                "suggested_column":  harmonized_lower[field_lower],
                "confidence":        99,
                "reason":            "Exact column name match"
            })
            matched = True

        # ── Priority 3: Keyword map ──
        if not matched:
            for keyword, (col, conf, reason) in keyword_map.items():
                if keyword in field_lower:
                    suggestions.append({
                        "benchling_field":  field,
                        "suggested_column": col,
                        "confidence":       conf,
                        "reason":           reason
                    })
                    matched = True
                    break

        # ── Priority 4: No match found ──
        if not matched:
            suggestions.append({
                "benchling_field":  field,
                "suggested_column": None,
                "confidence":       0,
                "reason":           "No matching column found — manual review needed"
            })

    return suggestions


# ─── STEP 5: Detect column changes ────────────────────────────────────────────

def detect_column_changes():
    """Compare current harmonized columns with previously approved mapping."""
    if not os.path.exists(APPROVED_MAPPING):
        return []

    with open(APPROVED_MAPPING) as f:
        approved = json.load(f)

    current_cols = set(get_harmonized_columns())
    previously_used = set()
    for schema_data in approved.values():
        for item in schema_data:
            if item.get("approved_column"):
                previously_used.add(item["approved_column"])

    missing = previously_used - current_cols
    return list(missing)


# ─── STEP 6: Main analysis function ───────────────────────────────────────────

def analyze_mapping(schema_name):
    """Full analysis for one schema sheet."""
    print(f"\n{'='*55}")
    print(f"  📋 Analyzing Schema: {schema_name}")
    print(f"{'='*55}")

    mapping_rows = read_mapping_sheet(schema_name)
    if not mapping_rows:
        print(f"  ⚠️  No mapping data found for '{schema_name}' — skipping.")
        return []

    harmonized_cols = get_harmonized_columns()

    # Only analyze fields that come from harmonized file
    input_fields = [r["benchling_field"] for r in mapping_rows if r["is_input_column"]]
    hardcoded    = [r for r in mapping_rows if not r["is_input_column"]]

    print(f"  🔒 Hardcoded fields ({len(hardcoded)}): {[r['benchling_field'] for r in hardcoded]}")
    print(f"  🔍 Fields to map from harmonized file ({len(input_fields)}): {input_fields}")

    if not input_fields:
        print("  ℹ️  No fields need harmonized column mapping.")
        return []

    # Get suggestions
    print(f"\n  🤖 {'[MOCK MODE] ' if USE_MOCK else '[CLAUDE AI] '}Generating suggestions...")
    if USE_MOCK:
        suggestions = mock_claude_suggest(input_fields, harmonized_cols, schema_name)
    else:
        suggestions = ask_claude(input_fields, harmonized_cols, schema_name)

    # Display results
    print(f"\n  {'─'*50}")
    auto_approved = []
    needs_review  = []

    for s in suggestions:
        conf = s["confidence"]
        col  = s["suggested_column"]
        field = s["benchling_field"]

        if conf >= 90 and col:
            status = "✅ AUTO"
            auto_approved.append(s)
        elif conf > 0 and col:
            status = "⚠️  REVIEW"
            needs_review.append(s)
        else:
            status = "❌ MISSING"
            needs_review.append(s)

        print(f"  {status}  {field:<25} → {str(col):<25} [{conf}%]")
        print(f"           Reason: {s['reason']}")

    print(f"\n  ✅ Auto-approved: {len(auto_approved)} | ⚠️  Needs review: {len(needs_review)}")
    return suggestions


# ─── STEP 7: Save approved mapping ────────────────────────────────────────────

def save_approved_mapping(all_suggestions):
    """Save all suggestions as the approved mapping file."""
    os.makedirs("ai", exist_ok=True)
    with open(APPROVED_MAPPING, "w") as f:
        json.dump(all_suggestions, f, indent=2)
    print(f"\n  💾 Approved mapping saved to: {APPROVED_MAPPING}")


# ─── STEP 8: Run full analysis ─────────────────────────────────────────────────

def run_full_analysis():
    print("\n" + "🚀 " * 20)
    print("  BENCHLING MAPPING ASSISTANT")
    print(f"  Mode: {'🟡 MOCK (no API key)' if USE_MOCK else '🟢 CLAUDE AI (live)'}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🚀 " * 20)

    # Check for column changes
    changes = detect_column_changes()
    if changes:
        print(f"\n⚠️  WARNING: These columns were used before but are now MISSING:")
        for c in changes:
            print(f"   ❌ {c}")

    # Get harmonized columns
    harmonized_cols = get_harmonized_columns()
    print(f"\n📊 Harmonized file has {len(harmonized_cols)} columns")

    # Analyze each schema
    all_results = {}
    for schema in SCHEMAS:
        suggestions = analyze_mapping(schema)
        if suggestions:
            all_results[schema] = suggestions

    # Save results
    save_approved_mapping(all_results)

    # Final summary
    total     = sum(len(v) for v in all_results.values())
    auto      = sum(1 for v in all_results.values() for s in v if s["confidence"] >= 90 and s["suggested_column"])
    review    = total - auto

    print(f"\n{'='*55}")
    print(f"  📊 FINAL SUMMARY")
    print(f"{'='*55}")
    print(f"  Schemas analyzed : {len(all_results)}")
    print(f"  Total fields     : {total}")
    print(f"  Auto-approved    : {auto} ✅")
    print(f"  Needs review     : {review} ⚠️")
    print(f"\n  Next step: Run 'python run_mapping_check.py' to review")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    run_full_analysis()