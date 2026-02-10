import chromadb
from typing import List, Dict
from intrafact.config import CHROMA_DB_DIR

class VectorDB:
    def __init__(self, collection_name: str = "intrafact_store"):

        print(f"....Initialising {collection_name}....")
        self.client = chromadb.PersistentClient(path=str(CHROMA_DB_DIR))
        self.collection = self.client.get_or_create_collection(collection_name)

    def add_chunks(self, chunks: List[Dict]):
        if not chunks:
            return
        
        ids = [c["id"] for c in chunks]
        embeddings = [c["embedding"] for c in chunks]
        documents = [c["content"] for c in chunks]

        metadatas = []
        for c in chunks:
            meta = c["metadata"].copy()
            if "parent_id" in c:
                meta["parent_id"] = c["parent_id"]
            metadatas.append(meta)

        self.collection.upsert(
            ids = ids,
            embeddings= embeddings,
            documents= documents,
            metadatas= metadatas
        )
        print(f"....Stored {len(chunks)} vectors in VectorDB....")
     
    def count(self):
            return self.collection.count()
        
    
