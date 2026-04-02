"""
ai/schema_fetcher.py
Fetches live schema definitions from Benchling and builds an ERD.
This gives Claude real field types, required flags and relationships
so mapping + validation are much smarter.
"""

from __future__ import annotations
import json
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

ERD_OUTPUT      = "ai/benchling_erd.json"
ERD_REPORT      = "reports/erd_report.txt"
BASE_URL        = "https://excelra.benchling.com/api/v2"

# The 4 core schema types we care about
CORE_SCHEMAS = {
    "entity_schemas":  "Custom Entity Schemas (Sample)",
    "dna_schemas":     "DNA Sequence Schemas",
    "result_schemas":  "Assay Result Schemas",
    "container_schemas": "Container Schemas",
}


# ─── STEP 1: Benchling API helper ─────────────────────────────────────────────

def _api_get(endpoint: str) -> dict:
    """Make authenticated GET request to Benchling API."""
    api_key = os.getenv("BENCHLING_API_KEY")
    if not api_key:
        raise RuntimeError("BENCHLING_API_KEY not found in .env file")

    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, auth=(api_key, ""), timeout=30)

    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 401:
        raise RuntimeError("❌ Benchling API key is invalid or expired")
    elif resp.status_code == 403:
        raise RuntimeError("❌ No permission to access this endpoint")
    else:
        print(f"  ⚠️  {endpoint} returned {resp.status_code} — skipping")
        return {}


# ─── STEP 2: Fetch each schema type ───────────────────────────────────────────

def fetch_entity_schemas() -> list:
    """Fetch Custom Entity schemas (Sample, Molecule etc.)"""
    data = _api_get("entity-schemas")
    return data.get("entitySchemas", [])


def fetch_dna_schemas() -> list:
    """Fetch DNA Sequence schemas."""
    data = _api_get("dna-sequence-schemas")
    return data.get("dnaSequenceSchemas", [])


def fetch_result_schemas() -> list:
    """Fetch Assay Result schemas."""
    data = _api_get("assay-result-schemas")
    return data.get("assayResultSchemas", [])


def fetch_container_schemas() -> list:
    """Fetch Container schemas."""
    data = _api_get("container-schemas")
    return data.get("containerSchemas", [])


# ─── STEP 3: Parse schema into clean ERD node ─────────────────────────────────

def parse_schema(schema: dict, schema_type: str) -> dict:
    """Extract fields, types, required flags from a raw Benchling schema."""
    fields = []
    for f in schema.get("fieldDefinitions", []):
        fields.append({
            "name":        f.get("name", ""),
            "type":        f.get("type", "unknown"),
            "required":    f.get("isRequired", False),
            "archived":    f.get("isArchived", False),
            "is_multi":    f.get("isMulti", False),
            "field_id":    f.get("id", ""),
        })

    return {
        "id":          schema.get("id", ""),
        "name":        schema.get("name", ""),
        "type":        schema_type,
        "prefix":      schema.get("prefix", ""),
        "field_count": len(fields),
        "fields":      fields,
        "required_fields": [f["name"] for f in fields if f["required"] and not f["archived"]],
        "all_fields":      [f["name"] for f in fields if not f["archived"]],
    }


# ─── STEP 4: Build relationships between schemas ──────────────────────────────

def build_relationships(erd_nodes: list) -> list:
    """
    Infer relationships between schemas based on field names and types.
    e.g. Sample → Container via Batch_ID
         DNA Sequence → Sample via entity link fields
    """
    relationships = []
    node_names = {n["name"]: n["id"] for n in erd_nodes}

    known_links = [
        ("Sample",       "DNA Sequence", "Construct_Name", "one-to-many"),
        ("Sample",       "Container",    "Batch_ID",       "one-to-many"),
        ("Sample",       "Results",      "Sample_ID",      "one-to-many"),
        ("Container",    "Box",          "Box",            "many-to-one"),
        ("Box",          "Location",     "Storage_Location","many-to-one"),
        ("Results",      "Entry",        "Assay_ID",       "many-to-one"),
    ]

    for src, tgt, via, rel_type in known_links:
        if src in node_names or tgt in node_names:
            relationships.append({
                "from":          src,
                "to":            tgt,
                "via_field":     via,
                "relationship":  rel_type,
            })

    return relationships


# ─── STEP 5: Compare with existing ERD to detect changes ──────────────────────

def detect_schema_changes(new_erd: dict) -> list:
    """Compare new ERD with previously saved ERD to find changes."""
    if not os.path.exists(ERD_OUTPUT):
        return []

    with open(ERD_OUTPUT) as f:
        old_erd = json.load(f)

    changes = []
    old_schemas = {s["name"]: s for s in old_erd.get("schemas", [])}
    new_schemas = {s["name"]: s for s in new_erd.get("schemas", [])}

    for name, new_s in new_schemas.items():
        if name not in old_schemas:
            changes.append(f"🆕 NEW SCHEMA: '{name}' was added to Benchling")
            continue

        old_s = old_schemas[name]
        old_fields = set(old_s.get("all_fields", []))
        new_fields = set(new_s.get("all_fields", []))

        added   = new_fields - old_fields
        removed = old_fields - new_fields

        if added:
            changes.append(f"➕ FIELDS ADDED in '{name}': {list(added)}")
        if removed:
            changes.append(f"➖ FIELDS REMOVED in '{name}': {list(removed)}")

    for name in old_schemas:
        if name not in new_schemas:
            changes.append(f"🗑️  SCHEMA REMOVED: '{name}' no longer exists in Benchling")

    return changes


# ─── STEP 6: Save ERD to file ─────────────────────────────────────────────────

def save_erd(erd: dict):
    os.makedirs("ai", exist_ok=True)
    with open(ERD_OUTPUT, "w") as f:
        json.dump(erd, f, indent=2)
    print(f"\n  💾 ERD saved to: {ERD_OUTPUT}")


# ─── STEP 7: Print ERD report ─────────────────────────────────────────────────

def print_erd_report(erd: dict, changes: list):
    os.makedirs("reports", exist_ok=True)
    lines = []

    lines.append("=" * 55)
    lines.append("  BENCHLING ERD REPORT")
    lines.append(f"  Generated : {erd['generated_at']}")
    lines.append(f"  Tenant    : {erd['tenant']}")
    lines.append(f"  Schemas   : {len(erd['schemas'])}")
    lines.append("=" * 55)

    for schema in erd["schemas"]:
        lines.append(f"\n📋 {schema['name']} [{schema['type']}]")
        lines.append(f"   ID     : {schema['id']}")
        lines.append(f"   Fields : {schema['field_count']} total")
        if schema["required_fields"]:
            lines.append(f"   ⚠️  Required : {schema['required_fields']}")
        if schema["all_fields"]:
            lines.append(f"   📝 All fields: {schema['all_fields']}")

    lines.append(f"\n{'─'*55}")
    lines.append("  RELATIONSHIPS")
    lines.append(f"{'─'*55}")
    for r in erd.get("relationships", []):
        lines.append(f"  {r['from']} ──[{r['via_field']}]──► {r['to']} ({r['relationship']})")

    if changes:
        lines.append(f"\n{'─'*55}")
        lines.append("  ⚠️  SCHEMA CHANGES DETECTED")
        lines.append(f"{'─'*55}")
        for c in changes:
            lines.append(f"  {c}")
    else:
        lines.append("\n  ✅ No schema changes since last run")

    report = "\n".join(lines)
    print(report)

    with open(ERD_REPORT, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\n  📄 Report saved: {ERD_REPORT}")


# ─── STEP 8: Main function ────────────────────────────────────────────────────

def fetch_and_build_erd() -> dict:
    print("\n" + "🧬 " * 20)
    print("  BENCHLING ERD SCHEMA FETCHER")
    print(f"  Tenant : {BASE_URL}")
    print(f"  Time   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🧬 " * 20)

    print("\n  🔌 Connecting to Benchling API...")

    all_schemas = []

    # Fetch all 4 core schema types
    fetchers = [
        ("Custom Entity", fetch_entity_schemas),
        ("DNA Sequence",  fetch_dna_schemas),
        ("Assay Result",  fetch_result_schemas),
        ("Container",     fetch_container_schemas),
    ]

    for label, fetcher in fetchers:
        print(f"  📥 Fetching {label} schemas...")
        try:
            raw = fetcher()
            for s in raw:
                node = parse_schema(s, label)
                all_schemas.append(node)
                print(f"     ✅ {node['name']} — {node['field_count']} fields "
                      f"({len(node['required_fields'])} required)")
        except Exception as e:
            print(f"     ⚠️  Could not fetch {label}: {e}")

    if not all_schemas:
        print("\n  ❌ No schemas fetched — check API key and permissions")
        return {}

    # Build ERD
    erd = {
        "tenant":       "excelra.benchling.com",
        "generated_at": datetime.now().isoformat(),
        "schema_count": len(all_schemas),
        "schemas":      all_schemas,
        "relationships": build_relationships(all_schemas),
    }

    # Detect changes
    changes = detect_schema_changes(erd)

    # Save + report
    save_erd(erd)
    print_erd_report(erd, changes)

    print(f"\n{'='*55}")
    print(f"  ✅ ERD built with {len(all_schemas)} schemas")
    print(f"  ✅ {len(erd['relationships'])} relationships mapped")
    if changes:
        print(f"  ⚠️  {len(changes)} schema changes detected!")
    print(f"{'='*55}\n")

    return erd


if __name__ == "__main__":
    fetch_and_build_erd()