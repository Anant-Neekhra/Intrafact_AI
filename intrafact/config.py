import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve()

while not (PROJECT_ROOT/"Readme.md").exists():
    if PROJECT_ROOT == PROJECT_ROOT.parent:
        raise FileNotFoundError("Could not find root directory")
    PROJECT_ROOT = PROJECT_ROOT.parent

DATA_DIR = PROJECT_ROOT/"data"
RAW_DATA_DIR = DATA_DIR/"raw"
PROCESSED_DATA_DIR = DATA_DIR/"processed"
CHROMA_DB_DIR = DATA_DIR/"chroma_db"
SQLITE_DB_PATH = DATA_DIR/"metadata.db"

os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(CHROMA_DB_DIR, exist_ok=True)
# Removed the SQL part because it was giving errors
