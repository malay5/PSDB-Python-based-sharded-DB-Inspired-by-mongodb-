import time
import uuid
import grpc
import json
import database_pb2
import database_pb2_grpc
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

class DatabaseClient:
    current_master_addr: Optional[str] = None

    def __init__(self, master_addresses: List[str] = ["localhost:50050"]):
        self.master_addresses = master_addresses
        self.current_master_index = 0
        self.master_channel = None
        self.master_stub = None
        self.current_db: Optional[str] = None
        self.worker_stub_cache = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.connect_to_master()

    def connect_to_master(self):
        """Connect to the Raft leader, updating current_master_addr."""
        attempted_addresses = set()
        max_attempts = len(self.master_addresses) * 2  # Allow retrying each address twice
        attempt = 0

        while attempt < max_attempts:
            address = self.master_addresses[self.current_master_index % len(self.master_addresses)]
            if address in attempted_addresses:
                self.current_master_index = (self.current_master_index + 1) % len(self.master_addresses)
                attempt += 1
                continue

            try:
                # Create a new channel for this attempt
                channel = grpc.insecure_channel(
                    address,
                    options=[
                        ('grpc.enable_retries', 1),
                        ('grpc.keepalive_timeout_ms', 5000),
                    ]
                )
                stub = database_pb2_grpc.MasterServiceStub(channel)
                
                # Get the current leader information
                response = stub.GetLeader(database_pb2.Empty(), timeout=5)
                print(f"master is : " , response)
                # If the current node is not the leader, follow the redirect
                if hasattr(response, 'leader_address') and response.leader_address and response.leader_address != address:
                    print(f"Master at {address} is not leader; redirecting to {response.leader_address}")
                    
                    # Close the current channel before redirecting
                    channel.close()
                    
                    # Update the address to the leader's address
                    address = response.leader_address
                    print(f"final leader address is : " , address)
                    
                    # If the leader is in our master list, update the current index
                    if address in self.master_addresses:
                        self.current_master_index = self.master_addresses.index(address)
                    
                    # Create a new channel to the leader
                    channel = grpc.insecure_channel(
                        address,
                        options=[
                            ('grpc.enable_retries', 1),
                            ('grpc.keepalive_timeout_ms', 5000),
                        ]
                    )
                    stub = database_pb2_grpc.MasterServiceStub(channel)
                    
                    # Verify the new connection is indeed the leader
                    try:
                        leader_response = stub.GetLeader(database_pb2.Empty(), timeout=5)
                        if hasattr(leader_response, 'leader_addr') and leader_response.leader_addr and leader_response.leader_addr != address:
                            # If we're redirected again, continue the loop
                            attempted_addresses.add(address)
                            self.current_master_index = (self.current_master_index + 1) % len(self.master_addresses)
                            attempt += 1
                            channel.close()
                            continue
                    except grpc.RpcError:
                        # If verification fails, continue the loop
                        attempted_addresses.add(address)
                        self.current_master_index = (self.current_master_index + 1) % len(self.master_addresses)
                        attempt += 1
                        channel.close()
                        continue

                # If we get here, we're connected to the leader
                self.master_channel = channel
                self.master_stub = stub
                DatabaseClient.current_master_addr = address
                print(f"Connected to leader at {address}")
                return

            except grpc.RpcError as e:
                print(f"Failed to connect to master at {address}: {e.details()}")
                attempted_addresses.add(address)
                self.current_master_index = (self.current_master_index + 1) % len(self.master_addresses)
                attempt += 1
                if 'channel' in locals():
                    channel.close()
                continue

        raise ConnectionError("Could not connect to any master leader")
    

    def get_leader(self) -> str:
        """Return the current master address or rediscover the leader."""
        if DatabaseClient.current_master_addr:
            try:
                # Verify the current connection is still the leader
                response = self.master_stub.GetLeader(database_pb2.Empty(), timeout=2)
                
                # If we're not connected to the leader, reconnect
                if hasattr(response, 'leader_addr') and response.leader_addr and response.leader_addr != DatabaseClient.current_master_addr:
                    print(f"Current master {DatabaseClient.current_master_addr} is not leader; redirecting to {response.leader_addr}")
                    self.connect_to_master()
                
                return DatabaseClient.current_master_addr
            except grpc.RpcError:
                # If verification fails, reconnect
                self.connect_to_master()
                return DatabaseClient.current_master_addr
        
        # If no current connection, establish a new one
        self.connect_to_master()
        return DatabaseClient.current_master_addr


    def execute_command(self, command: str) -> str:
        parts = command.strip().split()
        if not parts:
            return "Empty command"

        cmd = parts[0].lower()
        try:
            # Before executing any command, ensure we're connected to the leader
            self.get_leader()
            
            # [Rest of the execute_command method remains unchanged...]
            if cmd == "create" and len(parts) >= 2:
                return self.create_database(parts[1], eval(" ".join(parts[2:])) if len(parts) > 2 else [])
            # [Other command handling remains unchanged...]
            
        except grpc.RpcError as e:
            if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                print(f"RPC Error ({e.code().name}): {e.details()}")
                try:
                    self.connect_to_master()
                    return self.execute_command(command)
                except ConnectionError as ce:
                    return f"Failed to reconnect: {str(ce)}"
            return f"RPC Error ({e.code().name}): {e.details()}"
        except Exception as e:
            return f"Error: {str(e)}"
        

    def get_worker_address(self, worker_msg):
        """Extract worker address from different possible formats."""
        if isinstance(worker_msg, str):
            if ':' not in worker_msg:
                return f"localhost:{worker_msg}"
            return worker_msg
        elif hasattr(worker_msg, 'worker'):
            addr = worker_msg.worker
            if ':' not in addr:
                return f"localhost:{addr}"
            return addr
        else:
            raise ValueError(f"Invalid worker address format: {worker_msg}")

    def get_worker_stub(self, worker_address):
        """Get or create a worker stub with proper address handling."""
        try:
            normalized_addr = self.get_worker_address(worker_address)
            if normalized_addr not in self.worker_stub_cache:
                channel = grpc.insecure_channel(
                    normalized_addr,
                    options=[
                        ('grpc.so_reuseport', 1),
                        ('grpc.default_authority', 'localhost')
                    ]
                )
                self.worker_stub_cache[normalized_addr] = database_pb2_grpc.WorkerServiceStub(channel)
            return self.worker_stub_cache[normalized_addr]
        except Exception as e:
            raise ValueError(f"Failed to create worker stub: {str(e)}")

    def execute_command(self, command: str) -> str:
        parts = command.strip().split()
        if not parts:
            return "Empty command"

        cmd = parts[0].lower()
        try:
            if cmd == "create" and len(parts) >= 2:
                return self.create_database(parts[1], eval(" ".join(parts[2:])) if len(parts) > 2 else [])
            elif cmd == "use" and len(parts) == 2:
                return self.use_database(parts[1])
            elif cmd == "list":
                return self.list_databases()
            elif cmd == "delete" and len(parts) == 2:
                return self.delete_database(parts[1])
            elif cmd == "insert" and len(parts) >= 2:
                return self.create_document(" ".join(parts[1:]))
            elif cmd == "read" and len(parts) == 2:
                return self.read_document(parts[1])
            elif cmd == "readall":
                return self.read_all_documents()
            elif cmd == "query" and len(parts) >= 2:
                expr = " ".join(parts[1:])
                if (expr.startswith('"') and expr.endswith('"')) or (expr.startswith("'") and expr.endswith("'")):
                    expr = expr[1:-1]
                return self.query_documents(expr)
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
                       "  insert {'key':'value'}\n" \
                       "  read doc_id\n" \
                       "  query 'lambda doc: doc.get(\"key\") == \"value\"'"
        except grpc.RpcError as e:
            if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                print(f"RPC Error ({e.code().name}): {e.details()}")
                try:
                    self.connect_to_master()
                    return self.execute_command(command)
                except ConnectionError as ce:
                    return f"Failed to reconnect: {str(ce)}"
            return f"RPC Error ({e.code().name}): {e.details()}"
        except Exception as e:
            return f"Error: {str(e)}"

    def create_database(self, db_name: str, indexes: Optional[List[str]] = None) -> str:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                response = stub.CreateDatabase(
                    database_pb2.DatabaseName(
                        name=db_name,
                        indexes=indexes if indexes else []
                    ),
                    timeout=5
                )
                return response.message
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to create database after {max_retries} attempts"

    def use_database(self, db_name: str) -> str:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                db_list = stub.ListDatabases(database_pb2.Empty())
                if db_name not in db_list.names:
                    return f"Database '{db_name}' not found"

                worker = stub.GetPrimaryWorker(database_pb2.DatabaseName(name=db_name))
                if not worker or not worker.worker:
                    return "No primary worker available"

                master_response = stub.UseDatabase(database_pb2.DatabaseName(name=db_name))
                if not master_response.success:
                    return f"Master failed to use database: {master_response.message}"

                worker_stub = self.get_worker_stub(worker.worker)
                worker_response = worker_stub.UseDatabase(database_pb2.DatabaseName(name=db_name))
                
                if worker_response.success:
                    self.current_db = db_name
                    self.current_worker = worker.worker
                    return f"Using database '{db_name}' on worker {worker.worker}"
                
                return f"Worker failed: {worker_response.message}"
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to use database after {max_retries} attempts"

    def list_databases(self) -> str:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                response = stub.ListDatabases(database_pb2.Empty())
                return "Databases:\n" + "\n".join(response.names) if response.names else "No databases"
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to list databases after {max_retries} attempts"

    def list_workers(self) -> str:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                response = stub.ListWorkers(database_pb2.Empty())
                return "Workers:\n" + "\n".join(response.workers) if response.workers else "No workers available"
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to list workers after {max_retries} attempts"

    def delete_database(self, db_name: str) -> str:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                response = stub.DeleteDatabase(database_pb2.DatabaseName(name=db_name))
                return response.message
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to delete database after {max_retries} attempts"

    def create_document(self, document_json: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                doc_data = json.loads(document_json)
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                if not self.current_worker:
                    worker = stub.GetPrimaryWorker(database_pb2.DatabaseName(name=self.current_db))
                    if not worker or not worker.worker:
                        return "No worker available for this database"
                    self.current_worker = worker.worker
                
                worker_stub = self.get_worker_stub(self.current_worker)
                doc_id = str(uuid.uuid4())
                response = worker_stub.CreateDocument(
                    database_pb2.DocumentRequest(
                        db_name=self.current_db,
                        document=json.dumps(doc_data),
                        doc_id=doc_id
                    ),
                    timeout=10
                )
                return f"Created document with ID: {response.doc_id}"
            except json.JSONDecodeError as e:
                return f"Invalid JSON format: {str(e)}. Ensure correct syntax, e.g., {{\"key\": \"value\"}}" 
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to create document after {max_retries} attempts"

    def read_document(self, doc_id: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                if self.current_worker:
                    try:
                        worker_stub = self.get_worker_stub(self.current_worker)
                        response = worker_stub.ReadDocument(
                            database_pb2.DocumentID(db_name=self.current_db, doc_id=doc_id)
                        )
                        if response.document:
                            return response.document
                    except grpc.RpcError:
                        pass
                location = stub.GetDocumentLocation(
                    database_pb2.DocumentID(db_name=self.current_db, doc_id=doc_id)
                )
                if not location.worker:
                    return "Document not found"
                
                worker_stub = self.get_worker_stub(location.worker)
                response = worker_stub.ReadDocument(
                    database_pb2.DocumentID(db_name=self.current_db, doc_id=doc_id)
                )
                return response.document if response.document else "Document not found"
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to read document after {max_retries} attempts"

    def read_all_documents(self) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                worker = stub.GetPrimaryWorker(database_pb2.DatabaseName(name=self.current_db))
                if not worker.worker:
                    return "No primary worker available"
                
                worker_stub = self.get_worker_stub(worker.worker)
                response = worker_stub.ReadAllDocuments(database_pb2.DatabaseName(name=self.current_db))
                if not response.documents:
                    return "No documents found"
                
                shards = worker_stub.GetShardLocations(database_pb2.DatabaseName(name=self.current_db))
                all_docs = list(response.documents)
                
                for shard_worker in shards.workers:
                    if shard_worker != worker.worker:
                        try:
                            shard_stub = self.get_worker_stub(shard_worker)
                            shard_response = shard_stub.ReadAllDocuments(
                                database_pb2.DatabaseName(name=self.current_db)
                            )
                            all_docs.extend(shard_response.documents)
                        except Exception as e:
                            print(f"Warning: Failed to read from shard {shard_worker}: {str(e)}")
                
                return "\n".join(all_docs)
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to read all documents after {max_retries} attempts"

    def query_documents(self, filter_expr: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                worker = stub.GetPrimaryWorker(database_pb2.DatabaseName(name=self.current_db))
                if not worker.worker:
                    return "No primary worker available"
                
                worker_stub = self.get_worker_stub(worker.worker)
                response = worker_stub.QueryDocuments(
                    database_pb2.QueryRequest(db_name=self.current_db, filter_expr=filter_expr)
                )
                return "\n".join(response.documents) if response.documents else "No matching documents"
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to query documents after {max_retries} attempts"

    def update_document(self, doc_id: str, updates_json: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                updates = json.loads(updates_json)
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                primary = stub.GetPrimaryWorker(
                    database_pb2.DocumentID(db_name=self.current_db, doc_id=doc_id)
                )
                if not primary.worker:
                    return "Document not found or no primary assigned"
                
                worker_stub = self.get_worker_stub(primary.worker)
                response = worker_stub.UpdateDocument(
                    database_pb2.UpdateRequest(
                        db_name=self.current_db,
                        doc_id=doc_id,
                        updates=json.dumps(updates)
                    )
                )
                return response.message
            except json.JSONDecodeError as e:
                return f"Invalid JSON format: {str(e)}. Ensure correct syntax, e.g., {{\"key\": \"value\"}}"
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to update document after {max_retries} attempts"

    def delete_document(self, doc_id: str) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                primary = stub.GetPrimaryWorker(
                    database_pb2.DocumentID(db_name=self.current_db, doc_id=doc_id)
                )
                if not primary.worker:
                    return "Document not found"
                
                worker_stub = self.get_worker_stub(primary.worker)
                response = worker_stub.DeleteDocument(
                    database_pb2.DeleteRequest(db_name=self.current_db, doc_id=doc_id),
                    timeout=5
                )
                return "Document deleted successfully" if response.success else response.message
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to delete document after {max_retries} attempts"

    def clear_database(self) -> str:
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not DatabaseClient.current_master_addr:
                    self.connect_to_master()
                stub = self.master_stub
                worker = stub.GetPrimaryWorker(database_pb2.DatabaseName(name=self.current_db))
                if not worker.worker:
                    return "No primary worker available"
                
                worker_stub = self.get_worker_stub(worker.worker)
                response = worker_stub.ClearDatabase(database_pb2.DatabaseName(name=self.current_db))
                return response.message
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    print(f"Attempt {attempt + 1}: RPC Error ({e.code().name}): {e.details()}")
                    try:
                        self.connect_to_master()
                        continue
                    except ConnectionError as ce:
                        return f"Failed to reconnect: {str(ce)}"
                return f"RPC Error ({e.code().name}): {e.details()}"
            except Exception as e:
                return f"Error: {str(e)}"
        return f"Failed to clear database after {max_retries} attempts"

def main():
    import argparse
    parser = argparse.ArgumentParser(description='PSDB Client')
    parser.add_argument('--masters', nargs='+', default=["localhost:50050"],
                       help='Master node addresses (host:port) separated by spaces')
    args = parser.parse_args()

    client = DatabaseClient(master_addresses=args.masters)
    print("PSDB Client - Distributed Document Database")
    print("Type 'exit' to quit. Commands:")
    print("  create <db>       - Create a new database")
    print("  use <db>          - Select a database")
    print("  list              - List all databases")
    print("  delete <db>       - Delete a database")
    print("  workers           - List all worker nodes")
    print("  insert <json>     - Create document")
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
