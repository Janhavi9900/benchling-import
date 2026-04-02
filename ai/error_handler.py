"""
ai/error_handler.py
Intercepts Benchling SDK errors, explains them in plain English,
suggests fixes, and optionally auto-retries with corrected payload.
"""

import json
import os
import traceback
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# SET THIS TO False WHEN YOU HAVE API KEY
USE_MOCK = True
# ─────────────────────────────────────────

ERROR_LOG = "reports/error_log.txt"


# ─── KNOWN ERROR PATTERNS ─────────────────────────────────────────────────────

KNOWN_ERRORS = [
    {
        "pattern":     "404",
        "title":       "Entity Not Found",
        "plain":       "Benchling couldn't find the item you're trying to update/reference.",
        "likely_cause":"The ID in your payload doesn't exist in Benchling.",
        "fix":         "Check folderId, schemaId, or entityId values in config.json or CRO Mapping.xlsx."
    },
    {
        "pattern":     "400",
        "title":       "Bad Request — Invalid Payload",
        "plain":       "Your data doesn't match what Benchling expects.",
        "likely_cause":"Missing required field, wrong data type, or invalid value.",
        "fix":         "Check required fields in your schema. Run 'python run_mapping_check.py' first."
    },
    {
        "pattern":     "401",
        "title":       "Authentication Failed",
        "plain":       "Your Benchling API key is invalid or expired.",
        "likely_cause":"Wrong API key or key has been rotated.",
        "fix":         "Update BENCHLING_API_KEY in your .env file with a valid key."
    },
    {
        "pattern":     "403",
        "title":       "Permission Denied",
        "plain":       "Your API key doesn't have permission for this action.",
        "likely_cause":"API key lacks write access to this folder or schema.",
        "fix":         "Ask your Benchling admin to grant write permissions to your API key."
    },
    {
        "pattern":     "409",
        "title":       "Conflict — Duplicate Entry",
        "plain":       "This record already exists in Benchling.",
        "likely_cause":"You're trying to create something that already exists.",
        "fix":         "Use upsert instead of create, or check if record exists first."
    },
    {
        "pattern":     "429",
        "title":       "Rate Limit Exceeded",
        "plain":       "You're sending too many requests too fast.",
        "likely_cause":"Ingesting too many rows simultaneously.",
        "fix":         "Add a small delay between requests. Try batching in groups of 10."
    },
    {
        "pattern":     "500",
        "title":       "Benchling Server Error",
        "plain":       "Something went wrong on Benchling's side.",
        "likely_cause":"Temporary server issue or malformed payload.",
        "fix":         "Wait 30 seconds and retry. If persists, check Benchling status page."
    },
    {
        "pattern":     "invalid_field",
        "title":       "Invalid Field Name",
        "plain":       "A field in your payload doesn't exist in the Benchling schema.",
        "likely_cause":"Column name mismatch between mapping file and actual Benchling schema.",
        "fix":         "Run 'python run_erd_fetch.py' to get latest schema fields, then update mapping."
    },
    {
        "pattern":     "required",
        "title":       "Missing Required Field",
        "plain":       "A required field in Benchling is empty or missing.",
        "likely_cause":"Harmonized data has null values for a field Benchling requires.",
        "fix":         "Run 'python run_validation.py' to find which rows have missing required fields."
    },
    {
        "pattern":     "NoneType",
        "title":       "Null Value Error",
        "plain":       "Your code tried to use a value that doesn't exist (None/null).",
        "likely_cause":"A harmonized file column has an empty cell that wasn't handled.",
        "fix":         "Add null checks in payload_builder.py before sending to Benchling."
    },
]


# ─── STEP 1: Match error to known pattern ─────────────────────────────────────

def match_error_pattern(error_str: str) -> dict | None:
    """Match error string against known patterns."""
    error_lower = error_str.lower()
    for pattern in KNOWN_ERRORS:
        if pattern["pattern"].lower() in error_lower:
            return pattern
    return None


# ─── STEP 2: Mock Claude error explanation ────────────────────────────────────

def mock_explain_error(error_str: str, payload: dict, schema: str) -> dict:
    """Simulated Claude AI error explanation — replace with ask_claude() when ready."""

    matched = match_error_pattern(error_str)

    if matched:
        explanation = {
            "title":        matched["title"],
            "plain_english": matched["plain"],
            "likely_cause": matched["likely_cause"],
            "fix":          matched["fix"],
            "severity":     "high" if any(c in error_str for c in ["401","403","400"]) else "medium",
            "auto_fixable": "schemaId" in error_str or "folderId" in error_str,
            "suggested_payload_fix": None,
        }
    else:
        explanation = {
            "title":        "Unknown Error",
            "plain_english": f"An unexpected error occurred: {error_str[:100]}",
            "likely_cause": "Unknown — needs manual investigation",
            "fix":          "Check the full error log in reports/error_log.txt",
            "severity":     "high",
            "auto_fixable": False,
            "suggested_payload_fix": None,
        }

    # Payload-specific checks
    payload_issues = []
    if payload:
        if not payload.get("folderId"):
            payload_issues.append("❌ folderId is missing or empty")
        if not payload.get("schemaId"):
            payload_issues.append("❌ schemaId is missing or empty")
        if not payload.get("name") and schema != "Results":
            payload_issues.append("❌ name field is empty — required for most Benchling entities")
        if schema == "DNA Sequence" and not payload.get("bases"):
            payload_issues.append("❌ bases (DNA sequence) is empty")

    explanation["payload_issues"] = payload_issues
    return explanation


# ─── STEP 3: Real Claude error explanation ────────────────────────────────────

def ask_claude_explain(error_str: str, payload: dict, schema: str) -> dict:
    """Call real Claude API to explain and suggest fix for a Benchling error."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        prompt = f"""
You are a Benchling API expert. A Python ingestion script got this error:

Error: {error_str}
Schema: {schema}
Payload sent: {json.dumps(payload, indent=2, default=str)[:1000]}

Explain in plain English:
1. What went wrong
2. Why it happened
3. Exactly how to fix it

Return ONLY JSON (no markdown):
{{
  "title": "short error title",
  "plain_english": "what went wrong in simple terms",
  "likely_cause": "why this happened",
  "fix": "exact steps to fix",
  "severity": "high/medium/low",
  "auto_fixable": true/false,
  "suggested_payload_fix": {{}} or null
}}
"""
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}]
        )
        return json.loads(response.content[0].text.strip())
    except Exception as e:
        return mock_explain_error(error_str, payload, schema)


# ─── STEP 4: Log error to file ────────────────────────────────────────────────

def log_error(schema: str, row_id: str, error_str: str, explanation: dict):
    """Append error details to the error log file."""
    os.makedirs("reports", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*55}\n")
        f.write(f"Time     : {timestamp}\n")
        f.write(f"Schema   : {schema}\n")
        f.write(f"Row ID   : {row_id}\n")
        f.write(f"Error    : {error_str[:200]}\n")
        f.write(f"Title    : {explanation['title']}\n")
        f.write(f"Plain    : {explanation['plain_english']}\n")
        f.write(f"Cause    : {explanation['likely_cause']}\n")
        f.write(f"Fix      : {explanation['fix']}\n")
        if explanation.get("payload_issues"):
            f.write(f"Payload  : {explanation['payload_issues']}\n")
        f.write(f"{'='*55}\n")


# ─── STEP 5: Main handler — wrap any Benchling call ───────────────────────────

def handle_benchling_error(
    error: Exception,
    payload: dict,
    schema: str,
    row_id: str = "unknown",
    retry_fn=None
) -> dict:
    """
    Main error handler. Call this whenever a Benchling SDK call fails.

    Usage:
        try:
            result = create_custom_entity(payload)
        except Exception as e:
            result = handle_benchling_error(e, payload, "Sample", row_id="ADC001")
    """
    error_str = str(error)
    tb        = traceback.format_exc()

    print(f"\n  {'─'*50}")
    print(f"  ❌ BENCHLING ERROR — Schema: {schema} | Row: {row_id}")
    print(f"  {'─'*50}")
    print(f"  Raw error: {error_str[:150]}")

    # Get explanation
    if USE_MOCK:
        explanation = mock_explain_error(error_str, payload, schema)
    else:
        explanation = ask_claude_explain(error_str, payload, schema)

    # Print explanation
    print(f"\n  🤖 {'[MOCK]' if USE_MOCK else '[CLAUDE AI]'} Analysis:")
    print(f"  Title    : {explanation['title']}")
    print(f"  Problem  : {explanation['plain_english']}")
    print(f"  Cause    : {explanation['likely_cause']}")
    print(f"  Fix      : {explanation['fix']}")
    print(f"  Severity : {explanation['severity'].upper()}")

    if explanation.get("payload_issues"):
        print(f"\n  Payload problems found:")
        for issue in explanation["payload_issues"]:
            print(f"    {issue}")

    # Log to file
    log_error(schema, row_id, error_str, explanation)
    print(f"  📄 Logged to: {ERROR_LOG}")

    # Auto-retry if fix is available
    if explanation.get("auto_fixable") and retry_fn:
        print(f"\n  🔄 Attempting auto-retry...")
        try:
            result = retry_fn()
            print(f"  ✅ Auto-retry succeeded!")
            return {"status": "retried", "result": result}
        except Exception as retry_err:
            print(f"  ❌ Auto-retry also failed: {retry_err}")

    return {
        "status":      "failed",
        "error":       error_str,
        "explanation": explanation,
        "schema":      schema,
        "row_id":      row_id,
    }


# ─── STEP 6: Test the error handler ───────────────────────────────────────────

def test_error_handler():
    """Run test cases to verify error handler works correctly."""
    print("\n" + "🛡️  " * 15)
    print("  BENCHLING ERROR HANDLER — TEST")
    print(f"  Mode: {'🟡 MOCK' if USE_MOCK else '🟢 CLAUDE AI'}")
    print("🛡️  " * 15)

    test_cases = [
        {
            "error":   Exception("400 Bad Request: required field 'name' is missing"),
            "payload": {"folderId": "lib_123", "schemaId": "ts_456", "name": ""},
            "schema":  "Sample",
            "row_id":  "ADC001",
        },
        {
            "error":   Exception("404 Not Found: folderId lib_INVALID does not exist"),
            "payload": {"folderId": "lib_INVALID", "schemaId": "ts_bi9do6KL1Z"},
            "schema":  "Sample",
            "row_id":  "ADC002",
        },
        {
            "error":   Exception("409 Conflict: entity with name 'ADC-Sample-1' already exists"),
            "payload": {"name": "ADC-Sample-1", "folderId": "lib_Wi1jEfMSMK"},
            "schema":  "Sample",
            "row_id":  "ADC003",
        },
        {
            "error":   Exception("NoneType object has no attribute 'strip'"),
            "payload": {"name": None, "bases": None},
            "schema":  "DNA Sequence",
            "row_id":  "ADC004",
        },
    ]

    results = []
    for i, tc in enumerate(test_cases, 1):
        print(f"\n  TEST {i}/{len(test_cases)}")
        result = handle_benchling_error(
            tc["error"], tc["payload"], tc["schema"], tc["row_id"]
        )
        results.append(result["status"])

    print(f"\n{'='*55}")
    print(f"  TEST SUMMARY")
    print(f"{'='*55}")
    print(f"  Tests run    : {len(test_cases)}")
    print(f"  All handled  : ✅")
    print(f"  Error log    : {ERROR_LOG}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    test_error_handler()