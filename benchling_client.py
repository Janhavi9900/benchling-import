"""
Benchling API client using the official benchling-sdk plus REST helpers.

KEY FIX in this version:
  - find_location_by_name / find_box_by_name iterate PageIterator CORRECTLY
    (PageIterator yields pages = List[Model], not individual items)
  - transfer_into_container_direct uses the correct SDK method:
    client.containers.transfer_into_container(ContainerTransfer)
  - Separate location / box lookup functions to avoid schema ID mix-up
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional
from benchling_api_client.v2.stable.models.container_quantity import ContainerQuantity
from benchling_api_client.v2.stable.models.container_quantity_units import ContainerQuantityUnits


import requests
from benchling_sdk.auth.api_key_auth import ApiKeyAuth
from benchling_sdk.benchling import Benchling
from benchling_sdk.models import (
    CustomEntityCreate,
    DnaSequenceCreate,
    EntryCreate,
    FolderCreate,
    ContainerCreate,
    LocationCreate,
    BoxCreate,
    DnaSequenceUpsertRequest,
    CustomEntityUpsertRequest,
)

from config_loader import load_config


# ══════════════════════════════════════════════════════════════════════════════
# Internal helpers
# ══════════════════════════════════════════════════════════════════════════════

def _load_config() -> Dict[str, Any]:
    return load_config()


def _tenant_url_from_config(cfg: Dict[str, Any]) -> str:
    base_url = str(cfg.get("benchling", {}).get(
        "base_url", "https://excelra.benchling.com/api/v2"
    )).rstrip("/")
    if base_url.endswith("/api/v2"):
        return base_url[: -len("/api/v2")]
    return base_url


def _rest_base_url(cfg: Dict[str, Any]) -> str:
    return str(cfg.get("benchling", {}).get(
        "base_url", "https://excelra.benchling.com/api/v2"
    )).rstrip("/")


def _resolve_api_key(cfg: Dict[str, Any]) -> str:
    bc = cfg.get("benchling", {})

    literal = bc.get("api_key")
    if isinstance(literal, str) and literal.strip():
        return literal.strip()

    env_or_key = bc.get("api_key_env_var")
    if isinstance(env_or_key, str) and env_or_key.strip():
        s = env_or_key.strip()
        if s.startswith("sk_"):
            return s
        val = os.getenv(s)
        if val and val.strip():
            return val.strip()
        raise RuntimeError(f"Benchling API key env var '{s}' is not set.")

    val = os.getenv("BENCHLING_API_KEY")
    if val and val.strip():
        return val.strip()

    raise RuntimeError(
        "Benchling API key not configured. "
        "Set benchling.api_key_env_var or benchling.api_key in config.json."
    )


def _get_client() -> Benchling:
    cfg = _load_config()
    return Benchling(
        url=_tenant_url_from_config(cfg),
        auth_method=ApiKeyAuth(_resolve_api_key(cfg)),
    )


# ══════════════════════════════════════════════════════════════════════════════
# Entity creation
# ══════════════════════════════════════════════════════════════════════════════

def create_entry(payload: Dict[str, Any]) -> Any:
    client = _get_client()
    return client.entries.create_entry(entry=EntryCreate.from_dict(payload))


def create_dna_sequence(payload: Dict[str, Any]) -> Any:
    client = _get_client()
    return client.dna_sequences.create(dna_sequence=DnaSequenceCreate.from_dict(payload))


def create_custom_entity(payload: Dict[str, Any]) -> Any:
    client = _get_client()
    return client.custom_entities.create(entity=CustomEntityCreate.from_dict(payload))


def create_folder(payload: Dict[str, Any]) -> Any:
    client = _get_client()
    return client.folders.create(folder=FolderCreate.from_dict(payload))


def create_location(payload: Dict[str, Any]) -> Any:
    client = _get_client()
    return client.locations.create(location=LocationCreate.from_dict(payload))


def create_box(payload: Dict[str, Any]) -> Any:
    client = _get_client()
    return client.boxes.create(box=BoxCreate.from_dict(payload))


def create_container_direct(payload: Dict[str, Any]) -> Any:
    """
    Create a container via REST (camelCase keys).
    Accepted keys: name, barcode, schemaId, parentStorageId, fields, projectId
    """
    cfg     = _load_config()
    api_key = _resolve_api_key(cfg)
    base    = _rest_base_url(cfg)
    resp    = requests.post(f"{base}/containers", json=payload, auth=(api_key, ""))
    if resp.status_code == 201:
        return resp.json()
    raise Exception(f"Failed to create container: {resp.status_code} — {resp.text}")


# ══════════════════════════════════════════════════════════════════════════════
# Storage lookup — FIXED PageIterator iteration
#
# CRITICAL: PageIterator.__next__ returns List[Model] (a PAGE of items).
# The old code did:
#   for loc in client.locations.list(...):   <- loc was a List, not a Location
#       if loc.name == name:                  <- always failed silently
# This caused every run to create NEW duplicate locations and boxes.
#
# Correct pattern:
#   for page in client.locations.list(...):  <- page is List[Location]
#       for loc in page:                     <- loc is now a Location object
#           if loc.name == name: return loc.id
# ══════════════════════════════════════════════════════════════════════════════

def find_location_by_name(name: str, schema_id: str) -> Optional[str]:
    """
    Find an existing location by name and schema ID.
    Uses name= filter so only matching records are fetched.
    Iterates PageIterator correctly (each page is a List[Location]).
    """
    if not name:
        return None
    client = _get_client()
    try:
        for page in client.locations.list(name=name, schema_id=schema_id):
            for loc in page:          # page = List[Location]
                if getattr(loc, "name", None) == name:
                    return loc.id
    except Exception as e:
        print(f"  Location lookup error: {e}")
    return None


def find_box_by_name(name: str, schema_id: str) -> Optional[str]:
    """
    Find an existing box by name and schema ID.
    Iterates PageIterator correctly (each page is a List[Box]).
    """
    if not name:
        return None
    client = _get_client()
    try:
        for page in client.boxes.list(name=name, schema_id=schema_id):
            for box in page:          # page = List[Box]
                if getattr(box, "name", None) == name:
                    return box.id
    except Exception as e:
        print(f"  Box lookup error: {e}")
    return None


def find_storage_by_name(name: str, schema_id: str) -> Optional[str]:
    """Legacy compatibility wrapper."""
    return find_location_by_name(name, schema_id) or find_box_by_name(name, schema_id)


# ══════════════════════════════════════════════════════════════════════════════
# Container transfer — FIXED: correct SDK method
#
# The correct SDK method is:
#   client.containers.transfer_into_container(
#       destination_container_id = container_id,
#       transfer_request = ContainerTransfer(
#           destination_contents = [ContainerTransferDestinationContentsItem(entity_id=...)],
#           source_entity_id     = entity_id,
#       )
#   )
#
# This makes the sample entity appear in:
#   - The container's "Contents" section (shows sample name)
#   - The sample's "Inventory" tab in Benchling (shows the container)
# ══════════════════════════════════════════════════════════════════════════════

def transfer_into_container_direct(container_id: str, snippet: Dict[str, Any]) -> Any:
    from benchling_api_client.v2.stable.models.container_transfer import ContainerTransfer
    from benchling_api_client.v2.stable.models.container_transfer_destination_contents_item import (
        ContainerTransferDestinationContentsItem,
    )
    from benchling_api_client.v2.stable.models.measurement import Measurement
    from benchling_api_client.v2.types import UNSET

    contents = snippet.get("contents", [])
    if not contents:
        return {}

    item      = contents[0]
    entity_id = item.get("entityId")
    if not entity_id:
        raise ValueError("entityId is required for container transfer")

    # Build optional concentration measurement
    conc_data = item.get("concentration")
    conc_obj  = UNSET
    if conc_data and isinstance(conc_data, dict):
        try:
            conc_obj = Measurement(
                value=float(conc_data.get("value", 0.0)),
                units=conc_data.get("units", "mg/mL"),
            )
        except Exception:
            pass

    dest_item = ContainerTransferDestinationContentsItem(
        entity_id=entity_id,
        **({"concentration": conc_obj} if conc_obj is not UNSET else {}),
    )


    # transferQuantity is REQUIRED by Benchling API

    transfer_qty = ContainerQuantity(
        value=1.0,
        units=ContainerQuantityUnits("mg"),
    )

    transfer = ContainerTransfer(
        destination_contents=[dest_item],
        source_entity_id=entity_id,
        transfer_quantity=transfer_qty,
    )

    client = _get_client()
    client.containers.transfer_into_container(
        destination_container_id=container_id,
        transfer_request=transfer,
    )
    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════════════════
# Entry / results helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_entry_details(entry_id: str) -> Dict[str, Any]:
    client = _get_client()
    return client.entries.get_entry_by_id(entry_id).to_dict()


def get_result_table_id_from_entry(entry_id: str) -> Optional[str]:
    try:
        details = get_entry_details(entry_id)
        for day in details.get("days", []):
            for note in day.get("notes", []):
                if note.get("type") == "results_table":
                    return note.get("apiId")
    except Exception as e:
        print(f"  Table ID lookup error for {entry_id}: {e}")
    return None


def create_assay_results_bulk(payload: Dict[str, Any]) -> Dict[str, Any]:
    cfg     = _load_config()
    api_key = _resolve_api_key(cfg)
    base    = _rest_base_url(cfg)
    timeout = cfg.get("benchling", {}).get("timeout_seconds", 30)
    resp = requests.post(
        f"{base}/assay-results:bulk-create",
        auth=(api_key, ""),
        json=payload,
        timeout=timeout,
    )
    if resp.status_code not in (200, 201, 202):
        print(f"\n❌ API Error: {resp.status_code}")
        print(f"Response: {resp.text}")
        resp.raise_for_status()
    return resp.json()


# ══════════════════════════════════════════════════════════════════════════════
# Upsert / search helpers
# ══════════════════════════════════════════════════════════════════════════════

def upsert_dna_sequence(entity_registry_id: str, payload: Dict[str, Any]) -> Any:
    client = _get_client()
    return client.dna_sequences.upsert(
        entity_registry_id=entity_registry_id,
        dna_sequence=DnaSequenceUpsertRequest.from_dict(payload),
    )


def upsert_custom_entity(entity_registry_id: str, payload: Dict[str, Any]) -> Any:
    client = _get_client()
    return client.custom_entities.upsert(
        entity_registry_id=entity_registry_id,
        custom_entity=CustomEntityUpsertRequest.from_dict(payload),
    )


def find_dna_sequence_by_name(name: str) -> Optional[str]:
    if not name:
        return None
    client = _get_client()
    try:
        for page in client.dna_sequences.list(name=name):
            for ent in page:
                return ent.id
    except Exception:
        pass
    return None


def find_custom_entity_by_name(name: str) -> Optional[str]:
    if not name:
        return None
    client = _get_client()
    try:
        for page in client.custom_entities.list(name=name):
            for ent in page:
                return ent.id
    except Exception:
        pass
    return None


def find_any_entity_by_name(name: str) -> Optional[str]:
    if not name:
        return None
    client = _get_client()
    for service in [client.dna_sequences, client.aa_sequences, client.custom_entities]:
        try:
            for page in service.list(name=name):
                for ent in page:
                    return ent.id
        except Exception:
            continue
    return None