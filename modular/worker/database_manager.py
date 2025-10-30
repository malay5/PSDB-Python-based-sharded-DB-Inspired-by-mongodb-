from ast import Dict
import os
from typing import List, Optional
from .index import BPlusTree,BPlusTreeNode,IndexManager
from .simpleJSON import SimpleJSONDB
from .shard_metadata import ShardMetadata

class DatabaseManager:
    def __init__(self):
        self.databases: Dict[str, Dict] = {}  # {db_name: {'db': SimpleJSONDB, 'shards': ShardMetadata}}
        self.current_db: Optional[Dict] = None
        self.replica_metadata: Dict[str, Dict[str, List[str]]] = {}  # {db_name: {doc_id: [replica_workers]}}
    
    def add_replica(self, db_name: str, doc_id: str, worker_address: str) -> None:
        """Track which workers have replicas of which documents"""
        if db_name not in self.replica_metadata:
            self.replica_metadata[db_name] = {}
        if doc_id not in self.replica_metadata[db_name]:
            self.replica_metadata[db_name][doc_id] = []
        if worker_address not in self.replica_metadata[db_name][doc_id]:
            self.replica_metadata[db_name][doc_id].append(worker_address)
    
    def get_replicas(self, db_name: str, doc_id: str) -> List[str]:
        """Get list of workers with replicas of this document"""
        return self.replica_metadata.get(db_name, {}).get(doc_id, [])
    
    def create_database(self, db_name: str) -> SimpleJSONDB:
        if db_name in self.databases:
            raise ValueError(f"Database '{db_name}' already exists")
        self.databases[db_name] = {
            'db': SimpleJSONDB(f"{db_name}.json"),
            'shards': ShardMetadata()
        }
        
        return self.databases[db_name]['db']
    
    def use_database(self, db_name: str) -> SimpleJSONDB:
        
        if db_name not in self.databases:
            if os.path.exists(f"{db_name}.json"):
                self.databases[db_name] = {
                    'db': SimpleJSONDB(f"{db_name}.json"),
                    'shards': ShardMetadata(),
                    'index_manager': IndexManager()  # Initialize the IndexManager for the database
                }

            else:
                raise ValueError(f"Database '{db_name}' doesn't exist")
        self.current_db = self.databases[db_name]
        print(f"current db : " , self.current_db['db'])
        print(f"Type of current_db['db']: {type(self.current_db['db'])}")  # This should now print
        return self.current_db['db']
    
    # Inside DatabaseManager
    def create_document(self, document: Dict, doc_id: str) -> None:
        self.current_db['db'].create_document(document, doc_id)  # <- persists to disk!
        print(f"Type of current_db['db']: {type(self.current_db['db'])}")

        
        # Update B+ Tree indexes
        for field, index in self.current_db['index_manager'].indexes.items():
            if field in document:
                self.current_db['index_manager'].insert(field, document[field], doc_id)

    def read_document(self, doc_id: str) -> Optional[Dict]:
        """Read a document by its ID from the current database."""
        if not self.current_db:
            raise ValueError("No database selected. Call use_database first.")
        
        try:
            return self.current_db['db'].read_document(doc_id)
        except Exception as e:
            print(f"Error reading document {doc_id}: {e}")
            return None

