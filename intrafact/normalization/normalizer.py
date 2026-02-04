import json
import uuid
from typing import Dict
from datetime import datetime, timezone
from pathlib import Path

from intrafact.config import PROCESSED_DATA_DIR

class TextNormalizer:
    def __init__(self) -> None:
        PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    def clean_text(self, raw_text: str) -> str:
        if not raw_text:
            return ""

        cleaned_lines = []
        for line in raw_text.splitlines():
            if line.strip():

                clean_line = " ".join(line.split())
                cleaned_lines.append(clean_line)
        
        return "\n\n".join(cleaned_lines)
    
    def normalize(self,raw_text: str, metadata: Dict) -> Dict:

        cleaned_text = self.clean_text(raw_text)

        knowledge_object = {
            "id": str(uuid.uuid4()),
            "content": cleaned_text,
            "metadata": {
                **metadata,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "status": "processed"
            }
          
        }
        return knowledge_object
    
    def save_object(self, knowledge_object: Dict, original_file_name: str) -> Path:

        safe_file_name = original_file_name.replace('.','_')
        full_file_name = f"{safe_file_name}_{knowledge_object['id']}.json"
        save_path = PROCESSED_DATA_DIR/full_file_name

        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(knowledge_object, f, indent=4, ensure_ascii=False)

        return save_path
