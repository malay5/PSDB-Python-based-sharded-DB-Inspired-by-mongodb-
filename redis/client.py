import grpc
import json
import database_pb2
import database_pb2_grpc
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
 
class DatabaseClient:
    def __init__(self, master_address="localhost:50050"):
        self.master_channel = grpc.insecure_channel(master_address)
        self.master_stub = database_pb2_grpc.DatabaseServiceStub(self.master_channel)
        self.current_db: Optional[str] = None
        self.worker_stub_cache = {}  # Cache for worker stubs
        self.executor = ThreadPoolExecutor(max_workers=10)

    def get_worker_address(self, worker_msg):
        """Extract worker address from different possible formats"""
        if isinstance(worker_msg, str):
            # Already a string address
            if ':' not in worker_msg:
                return f"localhost:{worker_msg}"
            return worker_msg
        elif hasattr(worker_msg, 'worker'):
            # Protobuf Worker message
            addr = worker_msg.worker
            if ':' not in addr:
                return f"localhost:{addr}"
            return addr
        else:
            raise ValueError(f"Invalid worker address format: {worker_msg}")

    def get_worker_stub(self, worker_address):
        """Get or create a worker stub with proper address handling"""
        try:
            # Normalize the address first
            normalized_addr = self.get_worker_address(worker_address)
            
            if normalized_addr not in self.worker_stub_cache:
                channel = grpc.insecure_channel(
                    normalized_addr,
                    options=[
                        ('grpc.so_reuseport', 1),
                        ('grpc.default_authority', 'localhost')
                    ]
                )
                self.worker_stub_cache[normalized_addr] = database_pb2_grpc.DatabaseServiceStub(channel)
            return self.worker_stub_cache[normalized_addr]
        except Exception as e:
            raise ValueError(f"Failed to create worker stub: {str(e)}")

    def execute_command(self, command: str) -> str:
        parts = command.strip().split()
        if not parts:
            return "Empty command"

        cmd = parts[0].lower()

        try:
            if cmd == "create" and len(parts) == 2:
                return self.create_database(parts[1])
            elif cmd == "use" and len(parts) == 2:
                return self.use_database(parts[1])
            elif cmd == "list":
                return self.list_databases()
            elif cmd == "delete" and len(parts) == 2:
                return self.delete_database(parts[1])
            elif cmd == "insert" and len(parts) >= 3:
                return self.create_document(parts[1], " ".join(parts[2:]))
            elif cmd == "read" and len(parts) == 2:
                return self.read_document(parts[1])
            elif cmd == "readall":
                return self.read_all_documents()
            elif cmd == "query" and len(parts) >= 2:
                return self.query_documents(" ".join(parts[1:]))
            elif cmd == "update" and len(parts) >= 3:
                return self.update_document(parts[1], " ".join(parts[2:]))
            elif cmd == "delete_doc" and len(parts) == 2:
                return self.delete_document(parts[1])
            elif cmd == "clear":
                return self.clear_database()
            elif cmd == "workers":
                return self.list_workers()
            else:
                return "Invalid command. Examples:\n" \
                       "  create mydb\n" \
                       "  use mydb\n" \
                       "  insert doc_id {'key':'value'}\n" \
                       "  read doc_id\n" \
                       "  query 'lambda doc: doc.get(\"key\") == \"value\"'"
        except grpc.RpcError as e:
            return f"RPC Error ({e.code().name}): {e.details()}"
        except Exception as e:
            return f"Error: {str(e)}"

    def create_database(self, db_name: str) -> str:
        response = self.master_stub.CreateDatabase(
            database_pb2.DatabaseName(name=db_name)
        )
        return response.message

    def use_database(self, db_name: str) -> str:
        try:
            # First verify with master that database exists
            db_list = self.master_stub.ListDatabases(database_pb2.Empty())
            if db_name not in db_list.names:
                print(f"database not found in list")
                return f"Database '{db_name}' not found"

            # Get primary worker from master
            worker = self.master_stub.GetPrimaryWorker(
                database_pb2.DatabaseName(name=db_name))
            
            if not worker or not worker.worker:
                return "No primary worker available"

            # IMPORTANT: Tell the MASTER to use the database, not the worker directly
            # This ensures proper replication and state tracking
            master_response = self.master_stub.UseDatabase(
                database_pb2.DatabaseName(name=db_name))
            
            if not master_response.success:
                return f"Master failed to use database: {master_response.message}"

            # # Now use it on the worker too
            # worker_stub = self.get_worker_stub(worker)
            # worker_response = worker_stub.UseDatabase(
            #     database_pb2.DatabaseName(name=db_name))
            
            # if worker_response.success:
            #     self.current_db = db_name
            #     self.current_worker = worker.worker
            #     return f"Using database '{db_name}' on worker {worker.worker}"
            
            # return f"Worker failed: {worker_response.message}"
        
        except grpc.RpcError as e:
            return f"RPC Error ({e.code().name}): {e.details()}"
        except Exception as e:
            return f"Error using database: {str(e)}"
    

    def list_databases(self) -> str:
        response = self.master_stub.ListDatabases(database_pb2.Empty())
        return "Databases:\n" + "\n".join(response.names) if response.names else "No databases"

    def list_workers(self) -> str:
        response = self.master_stub.ListWorkers(database_pb2.Empty())
        return "Workers:\n" + "\n".join(response.workers) if response.workers else "No workers available"

    def delete_database(self, db_name: str) -> str:
        # Get primary worker first
        worker = self.master_stub.GetPrimaryWorker(
            database_pb2.DatabaseName(name=db_name))
        
        if not worker.address:
            return f"Database '{db_name}' not found or no primary worker"
        
        worker_stub = self.get_worker_stub(worker.address)
        response = worker_stub.DeleteDatabase(
            database_pb2.DatabaseName(name=db_name))
        
        if response.success:
            # Only unassign if deletion was successful
            self.master_stub.UnassignDatabase(
                database_pb2.DatabaseName(name=db_name))
        
        return response.message

    def create_document(self, doc_id: str, document_json: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        try:
            # Validate JSON
            doc_data = json.loads(document_json)
            
            if not self.current_worker:
                # Fallback to getting primary worker
                worker = self.master_stub.GetPrimaryWorker(
                    database_pb2.DatabaseName(name=self.current_db))
                if not worker or not worker.worker:
                    return "No worker available for this database"
                self.current_worker = worker.worker
            
            worker_stub = self.get_worker_stub(self.current_worker)
            response = worker_stub.CreateDocument(
                database_pb2.DocumentRequest(
                    db_name=self.current_db,
                    document=json.dumps(doc_data),
                    doc_id=doc_id
                ),
                timeout=10
            )
            
            return f"Created document with ID: {response.doc_id}"
        except json.JSONDecodeError:
            return "Invalid JSON format"
        except ValueError as e:
            return f"Worker connection error: {str(e)}"
        except grpc.RpcError as e:
            return f"RPC Error ({e.code().name}): {e.details()}"
    
    def read_document(self, doc_id: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        try:
            # First try reading from current worker
            if self.current_worker:
                try:
                    worker_stub = self.get_worker_stub(self.current_worker)
                    response = worker_stub.ReadDocument(
                        database_pb2.DocumentID(
                            db_name=self.current_db,
                            doc_id=doc_id
                        )
                    )
                    if response.document:
                        return response.document
                except grpc.RpcError:
                    pass  # Fall through to location lookup

            # If not found, get document location from master
            location = self.master_stub.GetDocumentLocation(
                database_pb2.DocumentID(
                    db_name=self.current_db,
                    doc_id=doc_id
                )
            )
            
            if not location.worker:
                return "Document not found"
            
            worker_stub = self.get_worker_stub(location.worker)
            response = worker_stub.ReadDocument(
                database_pb2.DocumentID(
                    db_name=self.current_db,
                    doc_id=doc_id
                )
            )
            
            return response.document if response.document else "Document not found"
        except grpc.RpcError as e:
            return f"RPC Error ({e.code().name}): {e.details()}"
        except Exception as e:
            return f"Error: {str(e)}"
        

    def read_all_documents(self) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        try:
            # Get primary worker
            worker = self.master_stub.GetPrimaryWorker(
                database_pb2.DatabaseName(name=self.current_db))
            
            if not worker.worker:  # Changed from worker.address to worker.worker
                return "No primary worker available"
            
            worker_stub = self.get_worker_stub(worker)
            response = worker_stub.ReadAllDocuments(
                database_pb2.DatabaseName(name=self.current_db))
            
            if not response.documents:
                return "No documents found"
            
            # Get shard locations if any
            shards = worker_stub.GetShardLocations(
                database_pb2.DatabaseName(name=self.current_db))
            
            all_docs = list(response.documents)
            
            # Gather documents from shards if they exist
            for shard_worker in shards.workers:
                if shard_worker != worker.worker:  # Compare with worker.worker
                    try:
                        shard_stub = self.get_worker_stub(shard_worker)
                        shard_response = shard_stub.ReadAllDocuments(
                            database_pb2.DatabaseName(name=self.current_db))
                        all_docs.extend(shard_response.documents)
                    except Exception as e:
                        print(f"Warning: Failed to read from shard {shard_worker}: {str(e)}")
                        continue
            
            return "\n".join(all_docs)
        except grpc.RpcError as e:
            return f"RPC Error ({e.code().name}): {e.details()}"
        except Exception as e:
            return f"Error reading documents: {str(e)}"


    def query_documents(self, filter_expr: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        # Get primary worker
        worker = self.master_stub.GetPrimaryWorker(
            database_pb2.DatabaseName(name=self.current_db))
        
        if not worker.address:
            return "No primary worker available"
        
        worker_stub = self.get_worker_stub(worker.address)
        response = worker_stub.QueryDocuments(
            database_pb2.QueryRequest(
                db_name=self.current_db,
                filter_expr=filter_expr
            )
        )
        
        return "\n".join(response.documents) if response.documents else "No matching documents"

    
    def update_document(self, doc_id: str, updates_json: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        try:
            updates = json.loads(updates_json)
            
            # Get document primary
            primary = self.master_stub.GetDocumentPrimary(
                database_pb2.DocumentID(
                    db_name=self.current_db,
                    doc_id=doc_id
                )
            )
            
            if not primary.worker:
                print(f"Document not found or no primary assigned")
                return "Document not found or no primary assigned"
            
            try:
                worker_stub = self.get_worker_stub(primary)
                response = worker_stub.UpdateDocument(
                    database_pb2.UpdateRequest(
                        db_name=self.current_db,
                        doc_id=doc_id,
                        updates=json.dumps(updates)
                ))
                return response.message
            except grpc.RpcError as e:
                return f"Failed to update document: {e.details()}"
        except json.JSONDecodeError:
            return "Invalid JSON format"
        except Exception as e:
            return f"Error updating document: {str(e)}"


    def delete_document(self, doc_id: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        try:
            # Get document primary
            primary = self.master_stub.GetDocumentPrimary(
                database_pb2.DocumentID(
                    db_name=self.current_db,
                    doc_id=doc_id
                )
            )
            
            if not primary.worker:
                return "Document not found"
            
            # Get worker stub with proper address handling
            worker_stub = self.get_worker_stub(primary.worker)
            
            # Perform deletion
            response = worker_stub.DeleteDocument(
                database_pb2.DeleteRequest(
                    db_name=self.current_db,
                    doc_id=doc_id
                ),
                timeout=5  # Add timeout
            )
            
            if response.success:
                # Update local state if we deleted from current worker
                if primary.worker == self.current_worker:
                    # Optionally refresh local view
                    pass
                return "Document deleted successfully"
            return response.message
        
        except grpc.RpcError as e:
            return f"RPC Error ({e.code().name}): {e.details()}"
        except Exception as e:
            return f"Error deleting document: {str(e)}"
    
    def clear_database(self) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        # Get primary worker
        worker = self.master_stub.GetPrimaryWorker(
            database_pb2.DatabaseName(name=self.current_db))
        
        if not worker.address:
            return "No primary worker available"
        
        worker_stub = self.get_worker_stub(worker.address)
        response = worker_stub.ClearDatabase(
            database_pb2.DatabaseName(name=self.current_db))
        
        return response.message

def main():
    import argparse
    parser = argparse.ArgumentParser(description='PSDB Client')
    parser.add_argument('--master', default="localhost:50050",
                       help='Master node address (host:port)')
    args = parser.parse_args()

    client = DatabaseClient(master_address=args.master)
    print("PSDB Client - Distributed Document Database")
    print("Type 'exit' to quit. Commands:")
    print("  create <db>       - Create a new database")
    print("  use <db>          - Select a database")
    print("  list              - List all databases")
    print("  delete <db>       - Delete a database")
    print("  workers           - List all worker nodes")
    print("  insert <id> <json>- Create document with ID")
    print("  read <id>         - Read a document")
    print("  readall           - Read all documents")
    print("  query <expr>      - Query documents with Python lambda")
    print("  update <id> <json>- Update a document")
    print("  delete_doc <id>   - Delete a document")
    print("  clear             - Clear current database")

    while True:
        try:
            command = input("> ").strip()
            if command.lower() in ('exit', 'quit'):
                break
            result = client.execute_command(command)
            print(result)
        except KeyboardInterrupt:
            print("\nUse 'exit' to quit")
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
