"""
run_mapping_check.py
Run this before every ingestion to validate your column mapping.
Usage: python run_mapping_check.py
"""
from ai.mapping_assistant import run_full_analysis

if __name__ == "__main__":
    run_full_analysis()