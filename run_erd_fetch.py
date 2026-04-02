"""
run_erd_fetch.py
Connects to Benchling and builds a live ERD of all schemas.
Run this whenever Benchling schemas might have changed.
Usage: python run_erd_fetch.py
"""
from ai.schema_fetcher import fetch_and_build_erd

if __name__ == "__main__":
    fetch_and_build_erd()