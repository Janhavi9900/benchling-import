# benchling-import
Benchling data ingestion pipeline with Claude AI
# Benchling Data Importer

This project provides a Python-based importer for uploading data to Benchling, including entries, DNA sequences, custom entities (samples), and assay results. It automatically creates folders based on the "CRO-Name" field from the input data and creates one entry per CRO in the respective folder.

## Features

- **Automatic Folder Creation**: Creates folders in Benchling based on unique "CRO-Name" values from the input Excel file.
- **Multiple Entries**: Creates one entry per CRO and stores it in the corresponding folder.
- **Data Import**: Imports DNA sequences, custom entities, and assay results from Excel mappings and JSON templates.
- **Smart Linking**: Links DNA sequences, custom entities, and assay results to the correct entry based on CRO information.
- **Flexible Mapping**: Uses Excel sheets to map input columns to Benchling fields.
- **Configuration-Driven**: All settings are managed via `config.json`.

## Project Structure

- `Importer.py`: Main script containing import functions and pipeline.
- `benchling_client.py`: Client functions for interacting with Benchling API.
- `payload_builder.py`: Utilities for building payloads from mappings.
- `config_loader.py`: Loads configuration from `config.json`.
- `config.json`: Configuration file with API settings, file paths, and templates.
- `*.json` templates: JSON templates for entries, DNA sequences, and custom entities.
- Excel files: Mapping workbook and data workbook.

## Configuration

Edit `config.json` to set:

- Benchling API key and base URL.
- Parent folder ID for creating CRO folders.
- Paths to Excel workbooks and JSON templates.
- Sheet names for different data types.

Example `config.json`:
```json
{
  "benchling": {
    "base_url": "https://excelra.benchling.com/api/v2",
    "parent_folder_id": "lib_xxxxxx",
    "api_key_env_var": "sk_xxxxxxx",
    "timeout_seconds": 30
  },
  "excel": {
    "mapping_workbook": "CRO Mapping.xlsx",
    "data_workbook": "Harmonized dataset_new.xlsx",
    "entry_sheet_name": "Entry",
    "dna_sheet_name": "DNA Sequence",
    "custom_sheet_name": "Sample",
    "results_sheet_name": "Results"
  },
  "templates": {
    "entry_template": "entry_template.json",
    "dna_template": "entity_template.json",
    "custom_template": "sample_template.json"
  }
}
```

## Mapping Sheet Format

The mapping workbook (CRO Mapping.xlsx) should have sheets with the following columns:

- **Entity Attributes**: Top-level JSON field names (e.g., "name", "entryTemplateId", "folderId")
- **Is Input file column**: "Yes" or "No" - whether to fetch from input data
- **API Values**: Static value or column name from input file (if "Is Input file column" = "Yes")
- **Input columns**: Additional field names to map to payload["fields"]

### Example Entry Sheet:
| Entity Attributes | Is Input file column | API Values | Input columns |
|---|---|---|---|
| name | Yes | CRO-Name | |
| entryTemplateId | No | tplt_xxxxxx | |

### Example DNA Sequence Sheet:
| Entity Attributes | Is Input file column | API Values | Input columns |
|---|---|---|---|
| name | Yes | Name | |
| dnaSequenceId | No | seq_auto_generate | |
| | | | Sequence |

## Usage

1. Prepare your Excel mapping workbook with sheets for Entry, DNA Sequence, Sample, and Results.
   - Each sheet should have a header row with the mapping columns.
   - Data rows define how to map input columns to Benchling fields.

2. Prepare your data workbook with the actual data, including a "CRO-Name" column.

3. Prepare JSON templates for entries, DNA sequences, and custom entities.

4. Update `config.json` with correct paths and settings (including `parent_folder_id`).

5. Run the importer:

```bash
python Importer.py
```

## Process Flow

```
1. Load configuration
   ↓
2. Read Excel workbooks (mapping and data)
   ↓
3. Extract unique CRO names from data
   ↓
4. Create folders for each unique CRO
   ↓
5. For each CRO:
   a. Filter data for that CRO
   b. Create entry with folder assignment
   c. Store entry ID mapped to CRO
   ↓
6. Create DNA sequences
   a. Build payloads from mapping
   b. Assign to correct folder (by CRO)
   c. Link to correct entry (by CRO)
   ↓
7. Create custom entities (samples)
   a. Build payloads from mapping
   b. Assign to correct folder (by CRO)
   c. Link to correct entry (by CRO)
   ↓
8. Load assay results from JSON
   ↓
9. Link results to appropriate entries
   ↓
10. Upload assay results
```

## Data Flow Diagram

```
Input Data (Excel)
    ↓
    ├─→ Extract CRO-Name values
    ↓
    Create Folders (one per CRO)
    ↓
    ├─→ Filter data by CRO
    ↓
    Create Entries (one per CRO, in folder)
    ↓ (Entry IDs stored with CRO mapping)
    ├─→ DNA Sequences
    │   ├─→ Assign to CRO folder
    │   └─→ Link to CRO entry
    ├─→ Custom Entities
    │   ├─→ Assign to CRO folder
    │   └─→ Link to CRO entry
    ├─→ Assay Results
        └─→ Link to entry
    ↓
    Upload to Benchling
```

## Dependencies

- pandas
- requests
- benchling-sdk

Install via `pip install -r requirements.txt`.

## Key Classes and Functions

### Importer.py

- `entry_import()`: Creates a single entry payload from mapping and data.
- `dna_sequence_import()`: Creates DNA sequence payloads and assigns to folders.
- `custom_entity_import()`: Creates custom entity payloads and assigns to folders.
- `main()`: Main pipeline orchestrating the entire import process.

### benchling_client.py

- `create_entry()`: Creates an entry in Benchling.
- `create_dna_sequence()`: Creates a DNA sequence in Benchling.
- `create_custom_entity()`: Creates a custom entity in Benchling.
- `create_folder()`: Creates a folder in Benchling.
- `create_assay_results_bulk()`: Uploads assay results in bulk.

### payload_builder.py

- `build_payload_from_mapping()`: Builds a single payload from mapping and data row.
- `build_payloads_from_mapping()`: Builds multiple payloads for all data rows.
- `normalize_mapping_dataframe()`: Normalizes mapping sheet headers.
- `infer_column_types()`: Infers data types for columns.

## Benchling SDK Methods Used

This project utilizes the official `benchling-sdk` for several core operations:

- **Authentication & Initialization**:
  - `Benchling(url, auth_method)`: Initializing the client.
  - `ApiKeyAuth(api_key)`: API Key authentication.

- **Notebook Entries**:
  - `EntryCreate.from_dict()`: Creating an entry model from a payload.
  - `client.entries.create_entry(entry)`: Creating a new notebook entry.
  - `client.entries.get_entry_by_id(id)`: Retrieving entry details and notes.

- **DNA Sequences**:
  - `DnaSequenceCreate.from_dict()`: Creating a DNA sequence model.
  - `client.dna_sequences.create(dna_sequence)`: Creating a new DNA sequence.
  - `client.dna_sequences.list(name, archive_reason, ...)`: Searching and listing DNA sequences.

- **Custom Entities (Samples)**:
  - `CustomEntityCreate.from_dict()`: Creating a custom entity model.
  - `client.custom_entities.create(entity)`: Creating a new custom entity.
  - `client.custom_entities.list(name, archive_reason, ...)`: Searching and listing custom entities.

- **Folders & Projects**:
  - `FolderCreate.from_dict()`: Creating a folder model.
  - `client.folders.create(folder)`: Creating a new folder in a project.

- **Inventory Management**:
  - `LocationCreate.from_dict()`: Creating a location model.
  - `client.locations.create(location)`: Creating a new storage location.
  - `BoxCreate.from_dict()`: Creating a box model.
  - `client.boxes.create(box)`: Creating a new storage box.
  - `client.locations.list(schema_id)`: Finding existing locations.
  - `client.boxes.list(schema_id)`: Finding existing boxes.
  - `client.containers.move(container_id, ...)`: Moving containers to new storage locations.

- **Direct REST API Calls**:
  - used for `POST /transfers` and `POST /assay-results:bulk-create` where SDK methods were supplemented with direct `requests` calls for specific functionality.


Please refer benchling api documentation page for further clarifications.
