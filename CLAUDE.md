# Benchling Import Project

Python pipeline that ingests CRO data into Benchling using the Benchling SDK.

## Stack
- Python 3.x
- Benchling SDK
- Pandas for Excel/data processing
- Anthropic Claude API for AI enhancements
- openpyxl for xlsx reading

## Key Files
- `Importer.py` - Main entry point, orchestrates ingestion per schema
- `payload_builder.py` - Builds Benchling API payload from mapping + data row
- `benchling_client.py` - Benchling SDK wrapper and connection
- `config_loader.py` - Loads configuration from config.json
- `CRO Mapping.xlsx` - Maps harmonized columns → Benchling schema fields
- `Harmonized dataset_new.xlsx` - Source data to be ingested

## AI Modules (in ai/ folder)
- `ai/validator.py` - Pre-ingestion data validation via Claude
- `ai/mapping_assistant.py` - Auto column mapping via Claude
- `ai/error_handler.py` - Benchling error explanation via Claude
- `ai/reporter.py` - Post-ingestion summary report via Claude

## Rules
- Never hardcode API keys — always use .env file
- Always validate data before ingestion starts
- Log all ingestion attempts to output.log
- Handle missing/null values gracefully in payload builder
- Test with small datasets (5 rows) before full runs

## Commands
- `python Importer.py` - Run full ingestion
- `python -m pytest tests/` - Run tests
- `venv\Scripts\activate` - Activate virtual environment (Windows)