from ast import Dict
import json
import os
from typing import List, Optional


class SimpleJSONDB:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.data: Dict[str, Dict] = {} 
        self._load()


    def _load(self):
        """Force reload from disk"""
        if os.path.exists(self.db_file):
            with open(self.db_file, 'r') as f:
                try:
                    self.data = json.load(f)
                except json.JSONDecodeError:
                    self.data = {}

    def create_document(self, document: Dict, doc_id: str) -> None:
        self.data[doc_id] = document
        self._save()

    def read_document(self, doc_id: str) -> Optional[Dict]:
        self._load()  # Reload from disk before reading
        print(f"getting document from worker : ", self.data.get(doc_id))
        return self.data.get(doc_id)

    def read_all_documents(self) -> List[Dict]:
        self._load()
        return list(self.data.values())

    def query_documents(self, filter_func: callable) -> List[Dict]:
        return [doc for doc in self.data.values() if filter_func(doc)]

    def update_document(self, doc_id: str, updates: Dict, create_if_missing: bool = False) -> bool:
        if doc_id in self.data:
            self.data[doc_id].update(updates)
            self._save()
            return True
        elif create_if_missing:
            self.data[doc_id] = updates
            self._save()
            return True
        return False

    def delete_document(self, doc_id: str) -> bool:
        if doc_id in self.data:
            del self.data[doc_id]
            self._save()
            return True
        return False

    def clear_database(self) -> None:
        self.data = {}
        self._save()

    def _save(self) -> None:
        with open(self.db_file, 'w') as f:
            json.dump(self.data, f, indent=2)
