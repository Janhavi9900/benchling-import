"""
ai/validator.py
Validates harmonized data before ingestion into Benchling.
Catches bad data early so nothing broken reaches Benchling.
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

HARMONIZED_FILE  = "Harmonized dataset_new.xlsx"
APPROVED_MAPPING = "ai/approved_mapping.json"
REPORTS_DIR      = "reports"

# ── Schema rules: what each schema REQUIRES ───────────────────────────────────
SCHEMA_RULES = {
    "Sample": {
        "required_columns": ["Sample_Name", "Sample_ID", "Program", "Target"],
        "date_columns":     [],
        "numeric_columns":  [],
        "unique_columns":   ["Sample_ID"],
    },
    "DNA Sequence": {
        "required_columns": ["Construct_Name", "Sequence", "Vector"],
        "date_columns":     [],
        "numeric_columns":  ["Sequence_Length", "GC_Content"],
        "unique_columns":   ["Construct_Name"],
    },
    "Results": {
        "required_columns": ["Assay_ID", "Assay_Type", "Method",
                             "Result_Value", "Result_Unit", "Analyst"],
        "date_columns":     [],
        "numeric_columns":  ["Result_Value"],
        "unique_columns":   ["Assay_ID"],
    },
    "Location": {
        "required_columns": ["Storage_Location", "Storage_Condition"],
        "date_columns":     [],
        "numeric_columns":  [],
        "unique_columns":   [],
    },
    "Box": {
        "required_columns": ["Box", "Position"],
        "date_columns":     [],
        "numeric_columns":  [],
        "unique_columns":   [],
    },
    "Container": {
        "required_columns": ["Batch_ID", "Quantity_mg", "Concentration"],
        "date_columns":     ["Manufacturing_Date", "Expiry_Date"],
        "numeric_columns":  ["Quantity_mg", "Concentration", "Purity_Percent"],
        "unique_columns":   ["Batch_ID"],
    },
}

# ── Global checks applied to ALL schemas ──────────────────────────────────────
GLOBAL_RULES = {
    "required_columns": ["CRO-Name", "Sample_ID"],
    "date_columns":     ["Manufacturing_Date", "Expiry_Date", "Calibration_Date"],
    "numeric_columns":  ["Molecular_Weight", "DAR", "Purity_Percent",
                         "Quantity_mg", "Concentration", "Sequence_Length"],
}


# ─── STEP 1: Load data ────────────────────────────────────────────────────────

def load_harmonized_data():
    df = pd.read_excel(HARMONIZED_FILE)
    print(f"  📥 Loaded {len(df)} rows × {len(df.columns)} columns")
    return df


# ─── STEP 2: Rule-based validation ────────────────────────────────────────────

def run_rule_based_checks(df, schema_name):
    issues   = []
    warnings = []
    rules    = SCHEMA_RULES.get(schema_name, {})

    # Check 1: Required columns exist + no nulls
    required = list(set(
        GLOBAL_RULES["required_columns"] +
        rules.get("required_columns", [])
    ))
    for col in required:
        if col not in df.columns:
            issues.append(f"❌ MISSING COLUMN: '{col}' required for {schema_name}")
        else:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                issues.append(
                    f"❌ NULL VALUES: '{col}' has {null_count} empty values "
                    f"({null_count/len(df)*100:.1f}% of rows)"
                )

    # Check 2: Numeric columns
    numeric_cols = list(set(
        GLOBAL_RULES["numeric_columns"] +
        rules.get("numeric_columns", [])
    ))
    for col in numeric_cols:
        if col in df.columns:
            non_numeric = df[col].dropna().apply(
                lambda x: not str(x).replace(".", "").replace("-", "").isnumeric()
            ).sum()
            if non_numeric > 0:
                issues.append(
                    f"❌ WRONG TYPE: '{col}' should be numeric "
                    f"but has {non_numeric} non-numeric values"
                )

    # Check 3: Date columns
    date_cols = list(set(
        GLOBAL_RULES["date_columns"] +
        rules.get("date_columns", [])
    ))
    for col in date_cols:
        if col in df.columns:
            try:
                pd.to_datetime(df[col].dropna())
            except Exception:
                issues.append(f"❌ INVALID DATE: '{col}' has unparseable date values")

    # Check 4: Unique columns
    for col in rules.get("unique_columns", []):
        if col in df.columns:
            dupes = df[col].duplicated().sum()
            if dupes > 0:
                dupe_vals = df[df[col].duplicated()][col].tolist()[:3]
                warnings.append(
                    f"⚠️  DUPLICATES: '{col}' has {dupes} duplicate values "
                    f"e.g. {dupe_vals}"
                )

    # Check 5: Expiry date logic
    if "Manufacturing_Date" in df.columns and "Expiry_Date" in df.columns:
        try:
            mfg = pd.to_datetime(df["Manufacturing_Date"], errors="coerce")
            exp = pd.to_datetime(df["Expiry_Date"],        errors="coerce")
            bad = (exp < mfg).sum()
            if bad > 0:
                issues.append(
                    f"❌ DATE LOGIC: {bad} rows have Expiry_Date before Manufacturing_Date"
                )
            expired = (exp < pd.Timestamp.now()).sum()
            if expired > 0:
                warnings.append(f"⚠️  EXPIRED: {expired} rows have Expiry_Date in the past")
        except Exception:
            pass

    # Check 6: CRO-Name info
    if "CRO-Name" in df.columns:
        cro_vals = df["CRO-Name"].dropna().unique().tolist()
        warnings.append(f"ℹ️  CRO-Names found: {cro_vals}")

    # Check 7: Negative numeric values
    for col in ["Quantity_mg", "Concentration", "Purity_Percent", "Molecular_Weight"]:
        if col in df.columns:
            neg = (pd.to_numeric(df[col], errors="coerce") < 0).sum()
            if neg > 0:
                issues.append(f"❌ NEGATIVE VALUES: '{col}' has {neg} negative values")

    return issues, warnings


# ─── STEP 3: Real Claude validation ───────────────────────────────────────────

def ask_claude_validate(df, schema_name):
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        sample = df.head(5).to_json(orient="records", indent=2)
        prompt = f"""
You are a data quality expert for a Benchling bioinformatics pipeline.
Schema: {schema_name}
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


# ─── STEP 4: Mock Claude validation ───────────────────────────────────────────

def mock_claude_validate(df, schema_name):
    insights = []

    # GC Content range check
    if "GC_Content" in df.columns:
        gc = pd.to_numeric(df["GC_Content"], errors="coerce")
        out_of_range = ((gc < 20) | (gc > 80)).sum()
        if out_of_range > 0:
            insights.append({
                "severity":     "warning",
                "column":       "GC_Content",
                "issue":        f"{out_of_range} rows have GC Content outside 20-80% range",
                "affected_rows": int(out_of_range),
                "suggestion":   "Verify these sequences are correct"
            })

    # DAR value check
    if "DAR" in df.columns:
        dar     = pd.to_numeric(df["DAR"], errors="coerce")
        high_dar = (dar > 8).sum()
        if high_dar > 0:
            insights.append({
                "severity":     "warning",
                "column":       "DAR",
                "issue":        f"{high_dar} rows have DAR > 8 (unusually high for ADCs)",
                "affected_rows": int(high_dar),
                "suggestion":   "Confirm DAR values are correct"
            })

    # Purity check
    if "Purity_Percent" in df.columns:
        purity     = pd.to_numeric(df["Purity_Percent"], errors="coerce")
        low_purity = (purity < 85).sum()
        if low_purity > 0:
            insights.append({
                "severity":     "warning",
                "column":       "Purity_Percent",
                "issue":        f"{low_purity} rows have Purity < 85% — may fail QC",
                "affected_rows": int(low_purity),
                "suggestion":   "Flag these for QC review before ingestion"
            })

    # SMILES check for Sample schema
    if "SMILES" in df.columns and schema_name == "Sample":
        empty_smiles = df["SMILES"].isnull().sum()
        if empty_smiles > 0:
            insights.append({
                "severity":     "error",
                "column":       "SMILES",
                "issue":        f"{empty_smiles} rows missing SMILES notation",
                "affected_rows": int(empty_smiles),
                "suggestion":   "SMILES is required for compound registration"
            })

    # Sequence length mismatch check
    if "Sequence" in df.columns and "Sequence_Length" in df.columns:
        df2               = df.copy()
        df2["actual_len"] = df2["Sequence"].dropna().apply(len)
        df2["stated_len"] = pd.to_numeric(df2["Sequence_Length"], errors="coerce")
        mismatch          = (df2["actual_len"] != df2["stated_len"]).sum()
        if mismatch > 0:
            insights.append({
                "severity":     "error",
                "column":       "Sequence_Length",
                "issue":        f"{mismatch} rows where Sequence_Length doesn't match actual sequence",
                "affected_rows": int(mismatch),
                "suggestion":   "Recalculate Sequence_Length from actual Sequence column"
            })

    return insights


# ─── STEP 5: Generate report ───────────────────────────────────────────────────

def generate_report(schema_name, issues, warnings, ai_insights, df):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path      = f"{REPORTS_DIR}/validation_{schema_name.replace(' ','_')}_{timestamp}.txt"

    lines = [
        "BENCHLING VALIDATION REPORT",
        f"Schema     : {schema_name}",
        f"Timestamp  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Rows       : {len(df)}",
        f"Mode       : {'MOCK' if USE_MOCK else 'CLAUDE AI'}",
        "=" * 55, "",
        f"RULE-BASED ISSUES ({len(issues)}):",
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

    lines += [
        "", "=" * 55,
        f"RESULT: {'FAILED' if issues else 'PASSED'}",
    ]

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\n  Report saved: {path}")
    return len(issues) == 0


# ─── STEP 6: Validate one schema ─────────────────────────────────────────────

def validate_schema(schema_name):
    print(f"\n{'='*55}")
    print(f"  Validating Schema: {schema_name}")
    print(f"{'='*55}")

    df = load_harmonized_data()

    print("\n  Running rule-based checks...")
    issues, warnings = run_rule_based_checks(df, schema_name)

    print(f"  Running {'[MOCK]' if USE_MOCK else '[CLAUDE AI]'} checks...")
    ai_insights = mock_claude_validate(df, schema_name) if USE_MOCK \
                  else ask_claude_validate(df, schema_name)

    # Print results
    print(f"\n  {'─'*50}")
    if issues:
        print(f"  Issues ({len(issues)}):")
        for i in issues:
            print(f"    {i}")
    else:
        print("  No critical issues found!")

    if warnings:
        print(f"\n  Warnings ({len(warnings)}):")
        for w in warnings:
            print(f"    {w}")

    if ai_insights:
        print(f"\n  AI Insights ({len(ai_insights)}):")
        for ins in ai_insights:
            icon = "❌" if ins["severity"] == "error" \
                   else "⚠️ " if ins["severity"] == "warning" else "ℹ️ "
            print(f"    {icon} [{ins['column']}] {ins['issue']}")
            print(f"       Fix: {ins['suggestion']}")

    passed = generate_report(schema_name, issues, warnings, ai_insights, df)
    print(f"\n  {'VALIDATION PASSED' if passed else 'VALIDATION FAILED'}")
    return passed


# ─── STEP 7: Validate all schemas ────────────────────────────────────────────

def validate_all():
    print("\n" + "=" * 55)
    print("  BENCHLING DATA VALIDATOR")
    print(f"  Mode: {'MOCK' if USE_MOCK else 'CLAUDE AI'}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    results = {}
    for schema in SCHEMA_RULES.keys():
        results[schema] = validate_schema(schema)

    print(f"\n{'='*55}")
    print(f"  VALIDATION SUMMARY")
    print(f"{'='*55}")
    for schema, passed in results.items():
        print(f"  {'PASS' if passed else 'FAIL'} {schema}")
    passed_count = sum(results.values())
    print(f"\n  {passed_count}/{len(results)} schemas passed")
    print(f"  Reports saved in: {REPORTS_DIR}/")
    print(f"{'='*55}\n")
    return all(results.values())


if __name__ == "__main__":
    validate_all()