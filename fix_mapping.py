"""
fix_mapping.py
Run once: python fix_mapping.py
1. Ignores the CRO field in DNA Sequence (it's an integer field, CRO-Name is text — mismatch)
2. Ensures linked_sample in Results is ignored (runtime-resolved)
"""
import json

path = "ai/approved_mapping.json"
with open(path) as f:
    mapping = json.load(f)

# Fix 1: ignore CRO field in DNA Sequence — Benchling expects integer, we have "CRO-A" text
for entry in mapping.get("DNA Sequence", []):
    if entry["benchling_field"] == "CRO":
        entry["status"] = "ignored"
        entry["suggested_column"] = None
        print("  Ignored: DNA Sequence -> CRO")

# Fix 2: ignore linked_sample in Results — wired at runtime in Importer.py
for entry in mapping.get("Results", []):
    if entry["benchling_field"] == "linked_sample":
        entry["status"] = "ignored"
        entry["suggested_column"] = None
        print("  Ignored: Results -> linked_sample")

with open(path, "w") as f:
    json.dump(mapping, f, indent=2)

print("Done — approved_mapping.json updated.")