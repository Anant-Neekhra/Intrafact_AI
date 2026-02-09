import sqlite3
import json
from datetime import datetime
from intrafact.config import SQLITE_DB_PATH

class MetadataStore:
    def __init__(self):
        self.conn = sqlite3.connect(str(SQLITE_DB_PATH))
        self.create_tables()

    def create_tables(self):
        """
        Creates the schema if it doesn't exist.
        """
        cursor = self.conn.cursor()
        
        # Table 1: Documents (The files we ingested)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                file_name TEXT,
                file_hash TEXT,
                ingested_at TIMESTAMP,
                metadata_json TEXT
            )
        """)
        
        # Table 2: Chunks (The pieces we split them into)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                id TEXT PRIMARY KEY,
                document_id TEXT,
                chunk_index INTEGER,
                content TEXT,
                FOREIGN KEY(document_id) REFERENCES documents(id)
            )
        """)
        self.conn.commit()

    def register_document(self, doc_id: str, file_name: str, metadata: dict):

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO documents (id, file_name, file_hash, ingested_at, metadata_json)
            VALUES (?, ?, ?, ?, ?)
        """, (
            doc_id, 
            file_name, 
            metadata.get("file_hash", ""), 
            datetime.now(), 
            json.dumps(metadata)
        ))
        self.conn.commit()
        print(f"   📝 Registered document metadata: {file_name}")

    def document_exists(self, file_hash: str) -> bool:

        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM documents WHERE file_hash = ?", (file_hash,))
        return cursor.fetchone() is not None