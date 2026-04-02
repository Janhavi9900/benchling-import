"""
run_pipeline.py
Master pipeline runner — orchestrates all AI modules + Benchling ingestion.

Full flow:
  Step 1: Fetch live ERD from Benchling
  Step 2: Validate column mapping
  Step 3: Validate data quality
  Step 4: Run Benchling ingestion (Importer.py)
  Step 5: Generate final report

Usage:
  python run_pipeline.py              → full run
  python run_pipeline.py --dry-run    → validate only, skip ingestion
  python run_pipeline.py --skip-erd   → skip ERD fetch (use cached)
"""

import sys
import os
import json
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DRY_RUN   = "--dry-run"  in sys.argv   # validate only, no ingestion
SKIP_ERD  = "--skip-erd" in sys.argv   # use cached ERD, skip live fetch
REPORT_DIR = "reports"
LOG_FILE   = f"{REPORT_DIR}/pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"


# ─── HELPERS ──────────────────────────────────────────────────────────────────

class PipelineLogger:
    """Logs to both terminal and file simultaneously."""
    def __init__(self, path: str):
        os.makedirs(REPORT_DIR, exist_ok=True)
        self.file = open(path, "w", encoding="utf-8")
        self.path = path

    def log(self, msg: str = ""):
        print(msg)
        self.file.write(msg + "\n")
        self.file.flush()

    def close(self):
        self.file.close()


def print_banner(logger, title: str, emoji: str = "🚀"):
    logger.log(f"\n{emoji * 20}")
    logger.log(f"  {title}")
    logger.log(f"{emoji * 20}")


def print_step(logger, step: int, title: str, total: int = 5):
    logger.log(f"\n{'─'*55}")
    logger.log(f"  STEP {step}/{total}: {title}")
    logger.log(f"{'─'*55}")


def print_result(logger, passed: bool, msg: str):
    icon = "✅" if passed else "❌"
    logger.log(f"  {icon} {msg}")


# ─── STEP 1: ERD FETCH ────────────────────────────────────────────────────────

def step_erd_fetch(logger) -> bool:
    print_step(logger, 1, "Fetch Live Benchling Schemas (ERD)")
    if SKIP_ERD:
        if os.path.exists("ai/benchling_erd.json"):
            logger.log("  ⏭️  Skipping ERD fetch — using cached ai/benchling_erd.json")
            with open("ai/benchling_erd.json") as f:
                erd = json.load(f)
            logger.log(f"  📋 Cached ERD: {erd.get('schema_count', 0)} schemas "
                      f"from {erd.get('generated_at', 'unknown')[:10]}")
            return True
        else:
            logger.log("  ⚠️  No cached ERD found — fetching live anyway")

    try:
        from ai.schema_fetcher import fetch_and_build_erd
        erd = fetch_and_build_erd()
        if erd:
            print_result(logger, True,
                f"ERD fetched: {erd.get('schema_count', 0)} schemas from Benchling")
            return True
        else:
            print_result(logger, False, "ERD fetch returned empty result")
            return False
    except Exception as e:
        print_result(logger, False, f"ERD fetch failed: {e}")
        logger.log("  ℹ️  Continuing with cached ERD if available...")
        return os.path.exists("ai/benchling_erd.json")


# ─── STEP 2: MAPPING CHECK ────────────────────────────────────────────────────

def step_mapping_check(logger) -> bool:
    print_step(logger, 2, "Validate Column Mapping")
    try:
        from ai.mapping_assistant import run_full_analysis
        run_full_analysis()
        print_result(logger, True, "Mapping check complete — see output above")
        return True
    except Exception as e:
        print_result(logger, False, f"Mapping check failed: {e}")
        return False


# ─── STEP 3: DATA VALIDATION ──────────────────────────────────────────────────

def step_validation(logger) -> bool:
    print_step(logger, 3, "Validate Data Quality")
    try:
        from ai.validator import validate_all
        passed = validate_all()
        print_result(logger, passed,
            "All schemas passed validation" if passed
            else "Some schemas have issues — review before ingesting")
        return passed
    except Exception as e:
        print_result(logger, False, f"Validation failed: {e}")
        return False


# ─── STEP 4: BENCHLING INGESTION ──────────────────────────────────────────────

def step_ingestion(logger) -> dict:
    print_step(logger, 4, "Benchling Ingestion")

    if DRY_RUN:
        logger.log("  ⏭️  DRY RUN mode — skipping actual ingestion")
        logger.log("  ℹ️  Remove --dry-run flag to ingest for real")
        return {"skipped": True, "reason": "dry_run"}

    results = {
        "success": 0,
        "failed":  0,
        "errors":  [],
        "skipped": False,
    }

    try:
        from Importer import main
        from ai.error_handler import handle_benchling_error

        logger.log("  🚀 Starting Benchling ingestion...")
        logger.log("  ⚠️  This will create real records in Benchling!")
        logger.log("")

        # Wrap Importer.main() with error handler
        try:
            main()
            results["success"] += 1
            print_result(logger, True, "Ingestion completed successfully")
        except Exception as e:
            # Route error through AI error handler
            explanation = handle_benchling_error(
                error=e,
                payload={},
                schema="Pipeline",
                row_id="main()"
            )
            results["failed"] += 1
            results["errors"].append({
                "schema":  "Pipeline",
                "error":   str(e)[:200],
                "title":   explanation.get("explanation", {}).get("title", "Unknown"),
                "fix":     explanation.get("explanation", {}).get("fix", ""),
            })
            print_result(logger, False, f"Ingestion failed: {explanation.get('explanation', {}).get('title', str(e))}")

    except ImportError as ie:
        print_result(logger, False, f"Could not import Importer.py: {ie}")
        results["failed"] += 1

    return results


# ─── STEP 5: GENERATE REPORT ──────────────────────────────────────────────────

def step_report(logger, results: dict, timings: dict, validation_passed: bool) -> None:
    print_step(logger, 5, "Generate Pipeline Report")

    duration = (timings["end"] - timings["start"]).seconds

    logger.log(f"\n{'='*55}")
    logger.log(f"  PIPELINE RUN REPORT")
    logger.log(f"{'='*55}")
    logger.log(f"  Run date    : {timings['start'].strftime('%Y-%m-%d %H:%M:%S')}")
    logger.log(f"  Duration    : {duration}s")
    logger.log(f"  Mode        : {'DRY RUN 🔍' if DRY_RUN else 'LIVE 🚀'}")
    logger.log(f"  ERD source  : {'Cached ⏭️' if SKIP_ERD else 'Live 🧬'}")
    logger.log(f"")
    logger.log(f"  STEPS COMPLETED:")
    logger.log(f"  {'✅' if timings.get('erd_ok')        else '❌'} Step 1: ERD Fetch")
    logger.log(f"  {'✅' if timings.get('mapping_ok')    else '❌'} Step 2: Mapping Check")
    logger.log(f"  {'✅' if validation_passed            else '❌'} Step 3: Data Validation")
    logger.log(f"  {'⏭️ ' if DRY_RUN else '✅' if not results.get('failed') else '❌'} Step 4: Ingestion")
    logger.log(f"  ✅ Step 5: Report Generated")
    logger.log(f"")

    if not DRY_RUN and not results.get("skipped"):
        logger.log(f"  INGESTION RESULTS:")
        logger.log(f"  Successful : {results['success']}")
        logger.log(f"  Failed     : {results['failed']}")
        if results["errors"]:
            logger.log(f"\n  ERRORS:")
            for err in results["errors"]:
                logger.log(f"    ❌ [{err['schema']}] {err['title']}")
                logger.log(f"       Fix: {err['fix']}")

    logger.log(f"")
    logger.log(f"  REPORTS SAVED:")
    logger.log(f"  📄 Pipeline log   : {LOG_FILE}")
    logger.log(f"  📄 Error log      : reports/error_log.txt")
    logger.log(f"  📄 ERD report     : reports/erd_report.txt")
    logger.log(f"  📄 Mapping        : ai/approved_mapping.json")
    logger.log(f"  📄 ERD            : ai/benchling_erd.json")
    logger.log(f"{'='*55}")

    overall = (
        timings.get("erd_ok", False) and
        timings.get("mapping_ok", False) and
        validation_passed and
        (DRY_RUN or not results.get("failed"))
    )
    logger.log(f"\n  OVERALL: {'✅ PIPELINE SUCCEEDED' if overall else '⚠️  PIPELINE COMPLETED WITH ISSUES'}")
    logger.log(f"{'='*55}\n")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    logger = PipelineLogger(LOG_FILE)
    timings = {"start": datetime.now()}

    print_banner(logger, "BENCHLING AI PIPELINE", "🧬")
    logger.log(f"  Mode     : {'🔍 DRY RUN (no ingestion)' if DRY_RUN else '🚀 LIVE RUN'}")
    logger.log(f"  ERD      : {'⏭️  Using cached' if SKIP_ERD else '🌐 Fetching live'}")
    logger.log(f"  Started  : {timings['start'].strftime('%Y-%m-%d %H:%M:%S')}")
    logger.log(f"  Log file : {LOG_FILE}")

    # Step 1: ERD
    erd_ok = step_erd_fetch(logger)
    timings["erd_ok"] = erd_ok

    # Step 2: Mapping
    mapping_ok = step_mapping_check(logger)
    timings["mapping_ok"] = mapping_ok

    # Step 3: Validation
    validation_passed = step_validation(logger)

    # Gate: stop if validation fails and not dry run
    if not validation_passed and not DRY_RUN:
        logger.log("\n  🛑 STOPPING: Validation failed.")
        logger.log("  Fix data issues before running live ingestion.")
        logger.log("  Tip: Use --dry-run to test without ingesting.\n")
        timings["end"] = datetime.now()
        step_report(logger, {}, timings, validation_passed)
        logger.close()
        sys.exit(1)

    # Step 4: Ingestion
    results = step_ingestion(logger)

    # Step 5: Report
    timings["end"] = datetime.now()
    step_report(logger, results, timings, validation_passed)

    logger.close()
    print(f"\n  📄 Full log saved to: {LOG_FILE}")


if __name__ == "__main__":
    main()