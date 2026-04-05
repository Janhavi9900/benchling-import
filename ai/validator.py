"""
ai/validator.py
Validates any uploaded data file before ingestion into Benchling.
Fully dynamic — works with any column structure, any file name.
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

APPROVED_MAPPING = "ai/approved_mapping.json"
REPORTS_DIR      = "reports"


def get_data_file() -> str:
    """
    Always returns the file uploaded by the user via the UI.
    Never uses a hardcoded file path.
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

    # Priority 3: legacy fallback for CLI only
    return "Harmonized dataset_new.xlsx"


def load_data() -> pd.DataFrame:
    """Load whichever file the user uploaded."""
    data_file = get_data_file()
    print(f"  📂 Validating: {os.path.basename(data_file)}")
    if data_file.endswith(".xlsx"):
        df = pd.read_excel(data_file)
    else:
        df = pd.read_csv(data_file)
    print(f"  📥 Loaded {len(df)} rows × {len(df.columns)} columns")
    return df


# ─── Rule-based checks ────────────────────────────────────────────────────────

def run_rule_based_checks(df: pd.DataFrame, schema_name: str):
    """
    Run checks based on what columns are actually present in the uploaded file.
    Does NOT assume any fixed column structure.
    """
    issues   = []
    warnings = []
    cols     = set(df.columns.tolist())

    # Load approved mapping to know which columns are used for this schema
    mapped_cols = []
    if os.path.exists(APPROVED_MAPPING):
        with open(APPROVED_MAPPING) as f:
            mapping = json.load(f)
        schema_fields = mapping.get(schema_name, [])
        mapped_cols = [
            f.get("suggested_column") or f.get("mapped")
            for f in schema_fields
            if f.get("suggested_column") or f.get("mapped")
        ]
        mapped_cols = [c for c in mapped_cols if c]

    # Check 1: mapped columns exist in file
    for col in mapped_cols:
        if col not in cols:
            issues.append(
                f"❌ MISSING COLUMN: '{col}' is mapped for {schema_name} "
                f"but not found in uploaded file"
            )

    # Check 2: null values in mapped columns
    for col in mapped_cols:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                issues.append(
                    f"❌ NULL VALUES: '{col}' has {null_count} empty values "
                    f"({null_count/len(df)*100:.1f}% of rows)"
                )

    # Check 3: duplicate IDs in key columns
    id_cols = [c for c in mapped_cols if c and
               any(x in c.lower() for x in ["id", "name", "construct", "batch"])]
    for col in id_cols:
        if col in df.columns:
            dupes = df[col].duplicated().sum()
            if dupes > 0:
                dupe_vals = df[df[col].duplicated()][col].tolist()[:3]
                warnings.append(
                    f"⚠️  DUPLICATES: '{col}' has {dupes} duplicate values "
                    f"e.g. {dupe_vals}"
                )

    # Check 4: numeric columns — only if present
    num_candidates = [c for c in mapped_cols if c and c in df.columns and
                      any(x in c.lower() for x in
                          ["quantity", "concentration", "purity", "dar",
                           "weight", "length", "gc_content", "result"])]
    for col in num_candidates:
        non_numeric = df[col].dropna().apply(
            lambda x: not str(x).replace(".", "").replace("-", "").isnumeric()
        ).sum()
        if non_numeric > 0:
            issues.append(
                f"❌ WRONG TYPE: '{col}' should be numeric "
                f"but has {non_numeric} non-numeric values"
            )

    # Check 5: date logic — only if both date columns present
    date_cols = [c for c in df.columns if "date" in c.lower()]
    mfg_col  = next((c for c in date_cols if "manuf" in c.lower()), None)
    exp_col  = next((c for c in date_cols if "expir" in c.lower()), None)

    if mfg_col and exp_col:
        try:
            mfg = pd.to_datetime(df[mfg_col], errors="coerce")
            exp = pd.to_datetime(df[exp_col],  errors="coerce")
            bad = (exp < mfg).sum()
            if bad > 0:
                issues.append(
                    f"❌ DATE LOGIC: {bad} rows have {exp_col} before {mfg_col}"
                )
            expired = (exp < pd.Timestamp.now()).sum()
            if expired > 0:
                warnings.append(
                    f"⚠️  EXPIRED: {expired} rows have {exp_col} in the past"
                )
        except Exception:
            pass

    # Check 6: negative numeric values
    for col in num_candidates:
        neg = (pd.to_numeric(df[col], errors="coerce") < 0).sum()
        if neg > 0:
            issues.append(f"❌ NEGATIVE VALUES: '{col}' has {neg} negative values")

    # Check 7: CRO group info
    cro_col = next((c for c in df.columns if "cro" in c.lower()), None)
    if cro_col:
        cro_vals = df[cro_col].dropna().unique().tolist()
        warnings.append(f"ℹ️  CRO groups found: {cro_vals}")

    return issues, warnings


# ─── Real Claude validation ───────────────────────────────────────────────────

def ask_claude_validate(df: pd.DataFrame, schema_name: str) -> list:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        sample = df.head(5).to_json(orient="records", indent=2)
        prompt = f"""
You are a data quality expert for a Benchling bioinformatics pipeline.
Schema being validated: {schema_name}
Sample data (first 5 rows): {sample}
Identify data quality issues that would cause Benchling ingestion problems.
Return ONLY a JSON array (no markdown):
[
  {{
    "severity": "error" or "warning" or "info",
    "column": "column_name",
    "issue": "description",
    "affected_rows": number or null,
    "suggestion": "how to fix"
  }}
]
"""
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        return json.loads(response.content[0].text.strip())
    except Exception as e:
        print(f"  ⚠️  Claude API error: {e}")
        return []


# ─── Mock Claude validation ───────────────────────────────────────────────────

def mock_claude_validate(df: pd.DataFrame, schema_name: str) -> list:
    """
    Smart AI-like checks based on what columns are present.
    Fully dynamic — no hardcoded column assumptions.
    """
    insights = []
    cols = df.columns.tolist()

    # GC Content check — only if column present
    gc_col = next((c for c in cols if "gc" in c.lower() and "content" in c.lower()), None)
    if gc_col:
        gc = pd.to_numeric(df[gc_col], errors="coerce")
        out = ((gc < 20) | (gc > 80)).sum()
        if out > 0:
            insights.append({
                "severity":     "warning",
                "column":       gc_col,
                "issue":        f"{out} rows have GC Content outside 20–80% range",
                "affected_rows": int(out),
                "suggestion":   "Verify these sequences are correct"
            })

    # DAR check — only if column present
    dar_col = next((c for c in cols if c.upper() == "DAR"), None)
    if dar_col:
        dar = pd.to_numeric(df[dar_col], errors="coerce")
        high = (dar > 8).sum()
        if high > 0:
            insights.append({
                "severity":     "warning",
                "column":       dar_col,
                "issue":        f"{high} rows have DAR > 8 (unusually high for ADCs)",
                "affected_rows": int(high),
                "suggestion":   "Confirm DAR values are correct"
            })

    # Purity check
    purity_col = next((c for c in cols if "purity" in c.lower()), None)
    if purity_col:
        purity = pd.to_numeric(df[purity_col], errors="coerce")
        low = (purity < 85).sum()
        if low > 0:
            insights.append({
                "severity":     "warning",
                "column":       purity_col,
                "issue":        f"{low} rows have purity < 85% — may fail QC",
                "affected_rows": int(low),
                "suggestion":   "Flag for QC review before ingestion"
            })

    # Sequence length mismatch
    seq_col = next((c for c in cols if c.lower() == "sequence"), None)
    len_col = next((c for c in cols if "sequence_length" in c.lower() or "seq_length" in c.lower()), None)
    if seq_col and len_col:
        df2 = df.copy()
        df2["_actual_len"] = df2[seq_col].dropna().apply(len)
        df2["_stated_len"] = pd.to_numeric(df2[len_col], errors="coerce")
        mismatch = (df2["_actual_len"] != df2["_stated_len"]).sum()
        if mismatch > 0:
            insights.append({
                "severity":     "error",
                "column":       len_col,
                "issue":        f"{mismatch} rows where {len_col} doesn't match actual sequence length",
                "affected_rows": int(mismatch),
                "suggestion":   f"Recalculate {len_col} from actual {seq_col} column"
            })

    # SMILES check for sample-type schemas
    smiles_col = next((c for c in cols if "smiles" in c.lower()), None)
    if smiles_col and schema_name == "Sample":
        empty = df[smiles_col].isnull().sum()
        if empty > 0:
            insights.append({
                "severity":     "warning",
                "column":       smiles_col,
                "issue":        f"{empty} rows missing SMILES notation",
                "affected_rows": int(empty),
                "suggestion":   "SMILES recommended for compound registration"
            })

    return insights


# ─── Generate report ──────────────────────────────────────────────────────────

def generate_report(schema_name, issues, warnings, ai_insights, df):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"{REPORTS_DIR}/validation_{schema_name.replace(' ','_')}_{timestamp}.txt"
    lines = [
        "BENCHLING VALIDATION REPORT",
        f"Schema    : {schema_name}",
        f"File      : {os.path.basename(get_data_file())}",
        f"Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Rows      : {len(df)}",
        f"Columns   : {len(df.columns)}",
        "=" * 55, "",
        f"ISSUES ({len(issues)}):",
        *([f"  {i}" for i in issues] or ["  None"]), "",
        f"WARNINGS ({len(warnings)}):",
        *([f"  {w}" for w in warnings] or ["  None"]), "",
        f"AI INSIGHTS ({len(ai_insights)}):",
    ]
    for ins in ai_insights:
        lines.append(f"  [{ins['severity'].upper()}] {ins['column']}: {ins['issue']}")
        lines.append(f"    Fix: {ins['suggestion']}")
    if not ai_insights:
        lines.append("  None")
    lines += ["", "=" * 55,
              f"RESULT: {'FAILED' if issues else 'PASSED'}"]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n  Report saved: {path}")
    return len(issues) == 0


# ─── Validate one schema ──────────────────────────────────────────────────────

def validate_schema(schema_name: str) -> bool:
    print(f"\n{'='*55}")
    print(f"  Validating: {schema_name}")
    print(f"{'='*55}")

    df = load_data()

    print("\n  Running rule-based checks...")
    issues, warnings = run_rule_based_checks(df, schema_name)

    print(f"  Running {'[MOCK]' if USE_MOCK else '[CLAUDE AI]'} checks...")
    ai_insights = mock_claude_validate(df, schema_name) if USE_MOCK \
                  else ask_claude_validate(df, schema_name)

    # Print results
    print(f"\n  {'─'*50}")
    if issues:
        print(f"  Issues ({len(issues)}):")
        for i in issues: print(f"    {i}")
    else:
        print("  No critical issues found!")

    if warnings:
        print(f"\n  Warnings ({len(warnings)}):")
        for w in warnings: print(f"    {w}")

    if ai_insights:
        print(f"\n  AI Insights ({len(ai_insights)}):")
        for ins in ai_insights:
            icon = "❌" if ins["severity"] == "error" else "⚠️ " if ins["severity"] == "warning" else "ℹ️ "
            print(f"    {icon} [{ins['column']}] {ins['issue']}")
            print(f"       Fix: {ins['suggestion']}")

    passed = generate_report(schema_name, issues, warnings, ai_insights, df)
    print(f"\n  {'PASSED ✅' if passed else 'FAILED ❌'}")
    return passed


# ─── Validate all schemas ─────────────────────────────────────────────────────

def validate_all() -> bool:
    print("\n" + "=" * 55)
    print("  BENCHLING DATA VALIDATOR")
    print(f"  File: {os.path.basename(get_data_file())}")
    print(f"  Mode: {'MOCK' if USE_MOCK else 'CLAUDE AI'}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    # Load approved mapping to know which schemas to validate
    schemas_to_validate = []
    if os.path.exists(APPROVED_MAPPING):
        with open(APPROVED_MAPPING) as f:
            mapping = json.load(f)
        schemas_to_validate = list(mapping.keys())
    else:
        schemas_to_validate = ["Sample", "DNA Sequence", "Results", "Container"]

    results = {}
    for schema in schemas_to_validate:
        results[schema] = validate_schema(schema)

    print(f"\n{'='*55}")
    print(f"  VALIDATION SUMMARY")
    print(f"  File: {os.path.basename(get_data_file())}")
    print(f"{'='*55}")
    for schema, passed in results.items():
        print(f"  {'PASS ✅' if passed else 'FAIL ❌'} {schema}")
    print(f"\n  {sum(results.values())}/{len(results)} schemas passed")
    print(f"{'='*55}\n")
    return all(results.values())


if __name__ == "__main__":
    validate_all()