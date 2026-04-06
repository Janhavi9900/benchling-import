"""
Benchling API client using the official benchling-sdk plus a small helper
for calling the assay-results bulk API.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

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
    CustomEntityUpsertRequest
)

from config_loader import load_config


def _load_config() -> Dict[str, Any]:
    return load_config()


def _tenant_url_from_config(cfg: Dict[str, Any]) -> str:
    benchling_cfg = cfg.get("benchling", {})
    base_url = str(benchling_cfg.get("base_url", "https://excelra.benchling.com/api/v2")).rstrip("/")
    if base_url.endswith("/api/v2"):
        return base_url[: -len("/api/v2")]
    return base_url


def _rest_base_url(cfg: Dict[str, Any]) -> str:
    benchling_cfg = cfg.get("benchling", {})
    return str(benchling_cfg.get("base_url", "https://excelra.benchling.com/api/v2")).rstrip("/")


def _resolve_api_key(cfg: Dict[str, Any]) -> str:
    benchling_cfg = cfg.get("benchling", {})

    literal_key = benchling_cfg.get("api_key")
    if isinstance(literal_key, str) and literal_key.strip():
        return literal_key.strip()

    maybe_env_or_key = benchling_cfg.get("api_key_env_var")
    if isinstance(maybe_env_or_key, str) and maybe_env_or_key.strip():
        s = maybe_env_or_key.strip()
        # Backwards-compat: if this looks like a key, use it directly.
        if s.startswith("sk_"):
            return s
        env_val = os.getenv(s)
        if env_val and env_val.strip():
            return env_val.strip()
        raise RuntimeError(
            f"Benchling API key env var '{s}' is not set. Export it (e.g. `export {s}=...`) or set benchling.api_key."
        )

    env_val = os.getenv("BENCHLING_API_KEY")
    if env_val and env_val.strip():
        return env_val.strip()

    raise RuntimeError(
        "Benchling API key is not configured. Set benchling.api_key_env_var (preferred) or benchling.api_key in config.json."
    )


def _get_client() -> Benchling:
    """Return a configured Benchling SDK client."""
    cfg = _load_config()
    return Benchling(
        url=_tenant_url_from_config(cfg),
        auth_method=ApiKeyAuth(_resolve_api_key(cfg)),
    )


def create_entry(payload: Dict[str, Any]) -> Any:
    """
    Create an entry in Benchling using the SDK.
    """
    client = _get_client()
    
    model = EntryCreate.from_dict(payload)
    return client.entries.create_entry(entry=model)


def create_dna_sequence(payload: Dict[str, Any]) -> Any:
    """
    Create a DNA sequence in Benchling using the SDK.
    """
    client = _get_client()
    model = DnaSequenceCreate.from_dict(payload)
    return client.dna_sequences.create(dna_sequence=model)


def create_custom_entity(payload: Dict[str, Any]) -> Any:
    """
    Create a custom entity in Benchling using the SDK.
    """
    client = _get_client()
    model = CustomEntityCreate.from_dict(payload)
    return client.custom_entities.create(entity=model)


def create_folder(payload: Dict[str, Any]) -> Any:
    """
    Create a folder in Benchling using the SDK.
    """
    client = _get_client()
    model = FolderCreate.from_dict(payload)
    return client.folders.create(folder=model)


def create_container(payload: Dict[str, Any]) -> Any:
    """
    Create a container in Benchling using the SDK.
    Note: The SDK model may skip 'contents'.
    """
    client = _get_client()
    model = ContainerCreate.from_dict(payload)
    return client.containers.create(container=model)


def create_container_direct(payload: Dict[str, Any]) -> Any:
    """
    Create a container in Benchling using direct API call (to support 'contents').
    """
    config = _load_config()
    api_key = _resolve_api_key(config)
    base_url = _rest_base_url(config)
    url = f"{base_url}/containers"
    
    response = requests.post(url, json=payload, auth=(api_key, ""))
    if response.status_code == 201:
        return response.json()
    else:
        raise Exception(f"Failed to create container: {response.status_code} - {response.text}")


def move_container(container_id: str, parent_storage_id: str, position: Any) -> Any:
    """
    Move a container to a new position in Benchling using the SDK.
    """
    client = _get_client()
    return client.containers.move(
        container_id=container_id,
        parent_storage_id=parent_storage_id,
        position=position
    )


def create_location(payload: Dict[str, Any]) -> Any:
    """
    Create a storage location in Benchling.
    """
    client = _get_client()
    model = LocationCreate.from_dict(payload)
    return client.locations.create(location=model)


def create_box(payload: Dict[str, Any]) -> Any:
    """
    Create a storage box in Benchling.
    """
    client = _get_client()
    model = BoxCreate.from_dict(payload)
    return client.boxes.create(box=model)


def find_storage_by_name(name: str, schema_id: str) -> str | None:
    """
    Find a storage item (location or box) by name and schema ID.
    Returns the ID if found, else None.
    """
    client = _get_client()
    # Try locations
    try:
        locations = client.locations.list(schema_id=schema_id)
        for loc in locations:
            if getattr(loc, "name", None) == name:
                return loc.id
    except Exception as e:
        print(f"  Lookup error (location): {e}")
    
    # Try boxes
    try:
        boxes = client.boxes.list(schema_id=schema_id)
        for box in boxes:
            if getattr(box, "name", None) == name:
                return box.id
    except Exception as e:
        print(f"  Lookup error (box): {e}")
            
    return None


def get_entry_details(entry_id: str) -> Dict[str, Any]:
    """
    Get entry details including result tables from Benchling API.
    """
    client = _get_client()
    return client.entries.get_entry_by_id(entry_id).to_dict()

def get_result_table_id_from_entry(entry_id: str) -> str | None:
    """
    Extracts the apiId of the first results table found in an entry's notes.
    """
    try:
        entry_details = get_entry_details(entry_id)
        for day in entry_details.get('days', []):
            for note in day.get('notes', []):
                if note.get('type') == 'results_table':
                    return note.get('apiId')
    except Exception as e:
        print(f"Error fetching table API ID for entry {entry_id}: {e}")
    return None


def create_assay_results_bulk(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call POST /assay-results:bulk-create with a prepared payload.
    Returns taskId for async operation (202 Accepted).
    """
    cfg = _load_config()
    api_key = _resolve_api_key(cfg)
    base_url = _rest_base_url(cfg)
    timeout = cfg.get("benchling", {}).get("timeout_seconds", 30)

    resp = requests.post(
        f"{base_url}/assay-results:bulk-create",
        auth=(api_key, ""),
        json=payload,
        timeout=timeout,
    )

    if resp.status_code not in (200, 202, 201):
        print(f"\n❌ API Error: {resp.status_code}")
        print(f"Response: {resp.text}")
        resp.raise_for_status()

    return resp.json()


def upsert_dna_sequence(entity_registry_id: str, payload: Dict[str, Any]) -> Any:
    """
    Upsert a DNA sequence in Benchling using the SDK.
    """
    client = _get_client()
    model = DnaSequenceUpsertRequest.from_dict(payload)
    return client.dna_sequences.upsert(entity_registry_id=entity_registry_id, dna_sequence=model)


def upsert_custom_entity(entity_registry_id: str, payload: Dict[str, Any]) -> Any:
    """
    Upsert a custom entity in Benchling using the SDK.
    """
    client = _get_client()
    model = CustomEntityUpsertRequest.from_dict(payload)
    return client.custom_entities.upsert(entity_registry_id=entity_registry_id, custom_entity=model)


def find_dna_sequence_by_name(name: str) -> str | None:
    """
    Find a DNA sequence by its name, registry ID, or alias.
    """
    if not name:
        return None
    client = _get_client()
    try:
        # Search Active first
        for page in client.dna_sequences.list(name=name):
            for ent in page:
                return ent.id
        for page in client.dna_sequences.list(names_any_of=[name]):
            for ent in page:
                return ent.id
        for page in client.dna_sequences.list(entity_registry_ids_any_of=[name]):
            for ent in page:
                return ent.id
        # Search Archived
        for page in client.dna_sequences.list(name=name, archive_reason="ANY"):
            for ent in page:
                return ent.id
    except Exception:
        pass
    return None


def find_custom_entity_by_name(name: str) -> str | None:
    """
    Find a custom entity by its name, registry ID, or alias.
    """
    if not name:
        return None
    client = _get_client()
    try:
        # Search Active first
        for page in client.custom_entities.list(name=name):
            for ent in page:
                return ent.id
        for page in client.custom_entities.list(names_any_of=[name]):
            for ent in page:
                return ent.id
        for page in client.custom_entities.list(entity_registry_ids_any_of=[name]):
            for ent in page:
                return ent.id
        # Search Archived
        for page in client.custom_entities.list(name=name, archive_reason="ANY"):
            for ent in page:
                return ent.id
    except Exception:
        pass
    return None


def find_any_entity_by_name(name: str) -> str | None:
    """
    Find ANY entity type in Benchling by name, alias, or registry identifier.
    """
    if not name:
        return None
    
    client = _get_client()
    services = [client.dna_sequences, client.aa_sequences, client.custom_entities]
    
    for service in services:
        try:
            # Search Active first
            for page in service.list(name=name):
                for ent in page:
                    return ent.id
            for page in service.list(names_any_of=[name]):
                for ent in page:
                    return ent.id
            for page in service.list(entity_registry_ids_any_of=[name]):
                for ent in page:
                    return ent.id
            # Search Archived
            for page in service.list(name=name, archive_reason="ANY"):
                for ent in page:
                    return ent.id
        except Exception:
            continue
    return None


def transfer_into_container_direct(container_id: str, snippet: Dict[str, Any]) -> Any:
    """
    Fill a container with an entity using POST /containers/{id}/content.
    """
    cfg = _load_config()
    api_key = _resolve_api_key(cfg)
    base_url = _rest_base_url(cfg)
    url = f"{base_url}/containers/{container_id}/content"

    contents = snippet.get("contents", [])
    items = []
    for item in contents:
        entry = {"entityId": item.get("entityId")}
        conc = item.get("concentration") or item.get("amount")
        if conc:
            entry["concentration"] = conc
        items.append(entry)

    payload = {"contents": items}
    resp = requests.post(url, json=payload, auth=(api_key, ""))

    if resp.status_code in (200, 201, 202):
        return resp.json() if resp.text else {}
    else:
        raise Exception(f"Container content transfer failed: {resp.status_code} - {resp.text}")

