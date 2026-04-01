import copy
import json
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd


def _normalize_column_name(name: Any) -> str:
    """
    Normalize a column name for robust, case-insensitive matching.
    """
    if name is None:
        return ""
    return str(name).strip().lower()


def normalize_mapping_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a mapping sheet where the first row contains column headers.

    Returns a copy with the first row promoted to headers and the index reset.
    """
    df = df.copy()
    df.columns = df.iloc[0]
    return df[1:].reset_index(drop=True)


def _to_iso_if_timestamp(value: Any) -> Any:
    """
    Convert pandas / Python datetime values to ISO 8601 strings.
    Leave non-datetime values unchanged.
    """
    # Treat explicit "no value" markers as missing.
    if value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _is_number_like(value: Any) -> bool:
    """
    Return True if the value is inherently numeric or a string that can be
    safely parsed as a float.
    """
    if isinstance(value, bool):
        # Treat booleans separately; they are not "number-like" for our purposes.
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        try:
            float(value)
            return True
        except ValueError:
            return False
    return False


def _infer_column_type(series: pd.Series) -> str:
    """
    Infer a stable logical type for a column based on all of its non-null values.

    Returns one of: "datetime", "number", "string", "unknown".
    """
    s = series.dropna()
    if s.empty:
        return "unknown"

    # First, rely on the pandas dtype, which is derived from the entire column.
    # This ties our logical type directly to how Excel data was parsed.
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"

    if pd.api.types.is_numeric_dtype(series):
        return "number"

    # For object / string columns, fall back to content-based inspection using
    # *all* non-null values in the column.
    if all(isinstance(v, (pd.Timestamp, datetime)) for v in s):
        return "datetime"

    if all(_is_number_like(v) for v in s):
        return "number"

    # Fallback: treat as string.
    return "string"


def _infer_column_types(df: pd.DataFrame) -> Dict[str, str]:
    """
    Infer logical types for all columns in a DataFrame.
    """
    return {col: _infer_column_type(df[col]) for col in df.columns}


def infer_column_types(df: pd.DataFrame) -> Dict[str, str]:
    """
    Public wrapper around the internal column type inference helper.

    This allows callers that only have access to the full input DataFrame (and
    not to individual Series objects) to compute a stable, per-column logical
    type map based on all non-null values in each column.
    """
    raw_types = _infer_column_types(df)
    # Key the mapping by normalized column name so that lookups are robust to
    # minor differences in case/whitespace between the mapping sheet and data.
    return {_normalize_column_name(col): t for col, t in raw_types.items()}


def _coerce_value_by_type(value: Any, logical_type: str | None) -> Any:
    """
    Coerce a scalar value to a consistent JSON-friendly representation based on
    the inferred logical column type.
    """
    # Always normalize timestamps first.
    value = _to_iso_if_timestamp(value)

    if logical_type == "number":
        # Always coerce numeric types to standard Python int or float to avoid
        # JSON serialization errors with pandas int64/float64.
        if value is not None and not isinstance(value, (bool, int, float)) and hasattr(value, "item"):
            # This handles numpy/pandas scalars
            value = value.item()

        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, bool):
            # Avoid serializing booleans as numbers.
            return value
        if isinstance(value, (int, float)):
            # If it's a whole-number float (e.g. 1401.0), cast to int so Benchling
            # integer fields don't reject it as 1401.0.
            if isinstance(value, float) and value.is_integer():
                return int(value)
            return value

        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                # If parsing fails, leave as string; this should be rare because
                # we only mark the column as "number" when all values are number-like.
                return value
    elif logical_type == "string":
        # For string-typed columns, force everything to a string representation
        # (except for missing values), so that the JSON type is stable even if
        # pandas parsed some cells as numbers.
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return str(value)

    # For "datetime" and "unknown", we keep the value as-is after timestamp
    # normalization.
    return value


def build_payload_from_mapping(
    mapping_df: pd.DataFrame,
    template_path: str,
    data_row: pd.Series,
    column_types: Dict[str, str] | None = None,
) -> Dict[str, Any]:
    """
    Build a single Benchling payload for one input row.

    Supports both old and new column names:
    - "Entity Attributes" or "Mandatory Fields": top-level JSON keys
    - "Is Input file column" or "Is Field": whether to fetch from input
    - "API Values" or "Mandatory Values": static/column values
    - "Input columns" or "Other Fields": field-level mappings
    """
    with open(template_path, encoding="utf-8") as f:
        template = json.load(f)

    payload: Dict[str, Any] = copy.deepcopy(template)

    # Build a lookup from normalized column name to the actual column label
    index_lookup = {_normalize_column_name(col): col for col in data_row.index}

    for _, row in mapping_df.iterrows():
        # Support both old and new column names
        mandatory_field = row.get("Entity Attributes") or row.get("Mandatory Fields")
        mandatory_value = row.get("API Values") or row.get("Mandatory Values")
        other_field = row.get("Input columns") or row.get("Other Fields")

        if pd.notna(mandatory_field):
            is_field = str(row.get("Is Input file column") or row.get("Is Field", "")).strip().lower() in {"yes", "y", "true", "1"}
            source_column: str | None = None
            logical_type: str | None = None

            if is_field:
                # Prefer the column named in the value field if present
                if isinstance(mandatory_value, str):
                    key = _normalize_column_name(mandatory_value)
                    if key in index_lookup:
                        source_column = index_lookup[key]
                        logical_type = (
                            column_types.get(key) if column_types is not None else None
                        )
                        value = data_row[source_column]
                    else:
                        value = mandatory_value
                # Otherwise fall back to a column whose name matches the mandatory field
                elif isinstance(mandatory_field, str):
                    key = _normalize_column_name(mandatory_field)
                    if key in index_lookup:
                        source_column = index_lookup[key]
                        logical_type = (
                            column_types.get(key) if column_types is not None else None
                        )
                        value = data_row[source_column]
                    else:
                        value = mandatory_value
                else:
                    value = mandatory_value
            else:
                # Not a dynamic field: use the literal value from the mapping row
                value = mandatory_value

            value = _coerce_value_by_type(value, logical_type)
            if pd.notna(value):
                payload[mandatory_field] = value

        if pd.notna(other_field):
            key = _normalize_column_name(other_field)
            if key in index_lookup:
                source_column = index_lookup[key]
                logical_type = column_types.get(key) if column_types is not None else None
                other_value = _coerce_value_by_type(data_row[source_column], logical_type)
                if pd.notna(other_value):
                    payload.setdefault("fields", {})
                    payload["fields"][other_field] = {"value": other_value}

    return payload


def build_payloads_from_mapping(
    config_df: pd.DataFrame,
    template_path: str,
    input_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Build one payload per row in the input_df using a mapping sheet and JSON template.

    - config_df: Excel sheet defining how to map columns into Benchling fields.
    - template_path: path to a JSON template file on disk.
    - input_df: Excel sheet containing one row per logical entity to create.
    """
    normalized_config = normalize_mapping_dataframe(config_df)
    column_types = infer_column_types(input_df)
    payloads: List[Dict[str, Any]] = []

    for _, data_row in input_df.iterrows():
        payloads.append(
            build_payload_from_mapping(
                normalized_config,
                template_path,
                data_row=data_row,
                column_types=column_types,
            )
        )

    return payloads

