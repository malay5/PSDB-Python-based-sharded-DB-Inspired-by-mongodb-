import hashlib
import json
import os
import threading
from typing import List, Dict, Optional
import uuid
from datetime import datetime
from concurrent import futures
import grpc
import database_pb2
import database_pb2_grpc
import logging
import time
 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Worker")

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

class ShardMetadata:
    def __init__(self):
        self.document_shards: Dict[str, str] = {}  # {doc_id: worker_address}
        self.worker_channels: Dict[str, grpc.Channel] = {}
        self.shard_workers: set = set()

    def get_shard_worker(self, doc_id: str) -> Optional[str]:
        return self.document_shards.get(doc_id)

    def add_shard(self, doc_id: str, worker_address: str) -> None:
        self.document_shards[doc_id] = worker_address
        self.shard_workers.add(worker_address)
        if worker_address not in self.worker_channels:
            self.worker_channels[worker_address] = grpc.insecure_channel(worker_address)

    def get_all_shard_workers(self) -> List[str]:
        return list(self.shard_workers)

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
                    'shards': ShardMetadata()
                }
            else:
                raise ValueError(f"Database '{db_name}' doesn't exist")
        self.current_db = self.databases[db_name]
        print(f"current db : " , self.current_db['db'])
        return self.current_db['db']

class DatabaseService(database_pb2_grpc.DatabaseServiceServicer):
    def __init__(self, worker_address: str, master_addresses: str):
        self.manager = DatabaseManager()
        self.worker_address = worker_address
        self.master_addresses = master_addresses
        self.current_leader: Optional[str] = None
        self.known_workers: List[str] = [worker_address]
        self.master_channel: Optional[grpc.Channel] = None
        self.master_stub: Optional[database_pb2_grpc.DatabaseServiceStub] = None
        self.replica_channels: Dict[str, grpc.Channel] = {}  # Cache for replica channels
        
        # Initialize master connection
        self._discover_master_leader()
        self._register_with_master()  # Add this line
        self._discover_workers()
        
        # Start background threads
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
        self.heartbeat_thread.start()
        self.leader_monitor_thread = threading.Thread(target=self._monitor_leader, daemon=True)
        self.leader_monitor_thread.start()

    def _register_with_master(self, max_retries: int = 5) -> bool:
        """Register this worker with the master cluster"""
        for attempt in range(max_retries):
            try:
                if not self.current_leader:
                    self._discover_master_leader()
                    if not self.current_leader:
                        logger.warning(f"Registration attempt {attempt+1}: No master leader available")
                        time.sleep(1)
                        continue
                
                logger.info(f"Attempting to register with master at {self.current_leader}")
                channel = grpc.insecure_channel(self.current_leader)
                stub = database_pb2_grpc.DatabaseServiceStub(channel)

                response = stub.AddWorker(
                    database_pb2.Worker(worker=self.worker_address),
                    timeout=3
                )
                
                if response.success:
                    logger.info(f"Successfully registered worker {self.worker_address} with master")
                    self.master_channel = channel
                    self.master_stub = stub 
                    return True
                else:
                    logger.warning(f"Registration attempt {attempt+1} failed: {response.message}")
            except Exception as e:
                logger.warning(f"Registration attempt {attempt+1} failed with error: {str(e)}")
                self.current_leader = None  # Force rediscovery
            
            time.sleep(1)  # Wait before retrying
        
        logger.error(f"Failed to register worker after {max_retries} attempts")
        return False

    def _discover_master_leader(self) -> None:
        """Discover the current leader among master nodes"""
        for master_addr in self.master_addresses:
            try:
                channel = grpc.insecure_channel(master_addr)
                stub = database_pb2_grpc.DatabaseServiceStub(channel)
                response = stub.GetLeader(database_pb2.Empty())
                if response.leader_address:
                    self.current_leader = response.leader_address
                    self.master_channel = channel
                    self.master_stub = stub
                    logger.info(f"Connected to master leader at {self.current_leader}")
                    return
            except Exception as e:
                logger.warning(f"Failed to connect to master {master_addr}: {str(e)}")
                continue
        
        logger.error("Could not connect to any master leader")
        self.current_leader = None
        self.master_channel = None
        self.master_stub = None

    def _monitor_leader(self) -> None:
        """Continuously monitor leader status"""
        while True:
            if not self.current_leader:
                self._discover_master_leader()
                time.sleep(1)
                continue
            
            try:
                # Check if leader is still responsive
                self.master_stub.GetLeader(database_pb2.Empty(), timeout=1)
                time.sleep(5)  # Check every 5 seconds
            except:
                # Leader may have changed
                logger.warning("Lost connection to master leader, rediscovering...")
                self._discover_master_leader()
                time.sleep(1)

    def _discover_workers(self) -> None:
        """Discover other workers in the cluster"""
        if not self.master_stub:
            return
            
        for _ in range(5):  # Retry 5 times
            try:
                response = self.master_stub.ListWorkers(database_pb2.Empty())
                self.known_workers = list(response.workers)
                logger.info(f"Discovered workers: {self.known_workers}")
                break
            except Exception as e:
                logger.warning(f"Could not discover workers from master: {str(e)}")
                time.sleep(1)
        else:
            #self.known_workers = ["localhost:50051", "localhost:50052"]  # Fallback
            logger.info(f"Fallback to default workers: {self.known_workers}")

    def _send_heartbeats(self) -> None:
        """Send periodic heartbeat messages to master"""
        while True:
            if not self.current_leader or not self.master_stub:
                logger.info("No leader available, retrying discovery...")
                self._discover_master_leader()
                time.sleep(1)
                continue
            
            try:
                response = self.master_stub.Heartbeat(
                    database_pb2.HeartbeatRequest(
                        worker_address=self.worker_address
                    ),
                    timeout=2
                )
                if response.acknowledged:
                    logger.info("Heartbeat acknowledged by leader")
                else:
                    logger.warning("Heartbeat not acknowledged, leader may have changed")
                    self._discover_master_leader()
            except Exception as e:
                logger.error(f"Heartbeat failed: {str(e)}")
                self._discover_master_leader()
            
            time.sleep(5)  # Send heartbeat every 5 seconds

    def RegisterWorker(self, request, context):
        """Handle registration requests (for other workers registering with this one)"""
        # In a worker node, we might not need to handle registrations
        # since registration is handled by the master
        return database_pb2.OperationResponse(
            success=False,
            message="Worker registration should be done through the master node"
        )

    def GetLoadInfo(self, request, context):
        """Return information about this worker's load"""
        replica_count = sum(len(docs) for docs in self.manager.replica_metadata.values())
        return database_pb2.LoadInfo(replica_count=replica_count)
    
    def DecrementReplicaCount(self, request, context):
        """Decrement the reported replica count (used when replication fails)"""
        return database_pb2.OperationResponse(success=True)

    def CreateDatabase(self, request, context):
        try:
            db_file = f"{request.name}.json"
            
            # Double-check with file system
            if os.path.exists(db_file):
                try:
                    with open(db_file, 'r') as f:
                        json.load(f)  # Validate JSON
                    return database_pb2.OperationResponse(
                        success=True,
                        message=f"Database '{request.name}' already exists"
                    )
                except json.JSONDecodeError:
                    # Corrupted file - overwrite it
                    pass

            # Create with file lock
            with open(db_file, 'w') as f:
                json.dump({}, f)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk

            # Ensure immediate visibility in manager
            if request.name not in self.manager.databases:
                self.manager.databases[request.name] = {
                    'db': SimpleJSONDB(db_file),
                    'shards': ShardMetadata()
                }
                self.manager.current_db = self.manager.databases[request.name]

            # Verify creation
            if not os.path.exists(db_file):
                return database_pb2.OperationResponse(
                    success=False,
                    message="Failed to create database file"
                )

            return database_pb2.OperationResponse(
                success=True,
                message=f"Database '{request.name}' created"
            )
        except Exception as e:
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )
        
    def UseDatabase(self, request, context):
        try:
            db_file = f"{request.name}.json"
            print(f"Looking for database file at: {os.path.abspath(db_file)}")  # Add this
            print(f"File exists: {os.path.exists(db_file)}")  # Add this

            # Verify absolute path
            abs_path = os.path.abspath(db_file)
            print(f"Checking database at: {abs_path}")
            # Verify database exists and is valid
            if not os.path.exists(db_file):
                print(f"Database file '{request.name}.json' doesn't exist")
                return database_pb2.OperationResponse(
                    success=False,
                    message=f"Database file '{request.name}.json' doesn't exist"
                )
                
            try:
                with open(db_file, 'r') as f:
                    json.load(f)  # Verify valid JSON
            except Exception as e:
                print(f"Database file is corrupted: {str(e)}")
                return database_pb2.OperationResponse(
                    success=False,
                    message=f"Database file is corrupted: {str(e)}"
                )
            
            # Now use it
            self.manager.use_database(request.name)
            return database_pb2.OperationResponse(
                success=True,
                message=f"Using database '{request.name}'"
            )
        except Exception as e:
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )
    
    
    def ListDatabases(self, request, context):
        dbs = list(self.manager.databases.keys())
        return database_pb2.DatabaseList(names=dbs)
    
    def DeleteDatabase(self, request, context):
        try:
            if request.name in self.manager.databases:
                if os.path.exists(f"{request.name}.json"):
                    os.remove(f"{request.name}.json")
                del self.manager.databases[request.name]
                if (self.manager.current_db and 
                    self.manager.current_db['db'].db_file == f"{request.name}.json"):
                    self.manager.current_db = None
                return database_pb2.OperationResponse(
                    success=True,
                    message=f"Database '{request.name}' deleted"
                )
            return database_pb2.OperationResponse(
                success=False,
                message=f"Database '{request.name}' not found"
            )
        except Exception as e:
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )
    
    def CreateDocument(self, request, context):
        try:
            logger.info(f"CreateDocument received for db={request.db_name}, doc_id={request.doc_id}")
            
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            doc_id = request.doc_id or str(uuid.uuid4())
            doc_data = {
                **json.loads(request.document),
                '_id': doc_id,
                '_created_at': datetime.now().isoformat(),
                '_updated_at': datetime.now().isoformat(),
                '_primary': request.doc_id is not None,
                '_version': 1
            }
            
            # Store document locally
            self.manager.current_db['db'].create_document(doc_data, doc_id)
            
            # Get replica workers from master
            replicas_response = self.master_stub.GetDocumentReplicas(
                database_pb2.DocumentID(db_name=request.db_name, doc_id=doc_id)
            )
            replicas = list(replicas_response.workers)  # Properly access the workers field
            
            # Replicate to each replica worker
            successful_replicas = []
            for replica_addr in replicas:
                try:
                    if replica_addr not in self.replica_channels:
                        self.replica_channels[replica_addr] = grpc.insecure_channel(replica_addr)
                    
                    stub = database_pb2_grpc.DatabaseServiceStub(self.replica_channels[replica_addr])
                    response = stub.ReplicateDocument(
                        database_pb2.ReplicateRequest(
                            db_name=request.db_name,
                            doc_id=doc_id,
                            document=json.dumps(doc_data)
                        )
                    )
                    if response.success:
                        self.manager.add_replica(request.db_name, doc_id, replica_addr)
                        successful_replicas.append(replica_addr)
                    else:
                        self.master_stub.DecrementReplicaCount(database_pb2.Worker(worker=replica_addr))
                except Exception as e:
                    logger.error(f"Failed to replicate to {replica_addr}: {str(e)}")
                    self.master_stub.DecrementReplicaCount(database_pb2.Worker(worker=replica_addr))
            
            # Compensate if needed
            if len(successful_replicas) < min(3, len(replicas)):
                self._compensate_replicas(request.db_name, doc_id, doc_data, successful_replicas)
            
            return database_pb2.DocumentID(doc_id=doc_id)
            
        except Exception as e:
            logger.error(f"Error creating document: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentID()
    
    def _compensate_replicas(self, db_name: str, doc_id: str, doc_data: Dict, 
                            existing_replicas: List[str]) -> None:
        """Create additional replicas if initial replication failed"""
        try:
            workers = self.master_stub.ListWorkers(database_pb2.Empty()).workers
            available_workers = [
                w for w in workers 
                if w != self.worker_address 
                and w not in existing_replicas
            ]
            
            # Sort by load
            load_info = {}
            for w in available_workers:
                try:
                    channel = grpc.insecure_channel(w)
                    stub = database_pb2_grpc.DatabaseServiceStub(channel)
                    response = stub.GetLoadInfo(database_pb2.Empty())
                    load_info[w] = response.replica_count
                except:
                    continue
            
            sorted_workers = sorted(load_info.keys(), key=lambda w: load_info[w])
            needed = min(3, len(workers)-1) - len(existing_replicas)
            
            for w in sorted_workers[:needed]:
                try:
                    if w not in self.replica_channels:
                        self.replica_channels[w] = grpc.insecure_channel(w)
                    
                    stub = database_pb2_grpc.DatabaseServiceStub(self.replica_channels[w])
                    response = stub.ReplicateDocument(
                        database_pb2.ReplicateRequest(
                            db_name=db_name,
                            doc_id=doc_id,
                            document=json.dumps(doc_data)
                    ))
                    if response.success:
                        self.manager.add_replica(db_name, doc_id, w)
                except Exception as e:
                    logger.error(f"Failed compensatory replication to {w}: {str(e)}")
        except Exception as e:
            logger.error(f"Error in compensatory replication: {str(e)}")

    def ReplicateDocument(self, request, context):
        """Handle document replication from primary"""
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            doc_data = json.loads(request.document)
            doc_id = request.doc_id
            
            if doc_id in self.manager.current_db['db'].data:
                current_doc = self.manager.current_db['db'].data[doc_id]
                current_doc.update(doc_data)
                if '_primary' in doc_data:
                    current_doc['_primary'] = doc_data['_primary']
                current_doc['_updated_at'] = datetime.now().isoformat()
            else:
                doc_data['_created_at'] = datetime.now().isoformat()
                doc_data['_updated_at'] = datetime.now().isoformat()
                self.manager.current_db['db'].create_document(doc_data, doc_id)
            
            print(f"document replicated on this worker")
            return database_pb2.OperationResponse(success=True)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )

    def ReadDocument(self, request, context):
        try:
            logger.info(f"ReadDocument: db={request.db_name} doc_id={request.doc_id}")
            
            # Ensure correct database is loaded
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
                logger.info(f"Loaded database {request.db_name}")
            
            # Try local read first
            doc = self.manager.current_db['db'].read_document(request.doc_id)
            if doc:
                logger.info(f"Found document locally: {doc}")
                return database_pb2.DocumentResponse(document=json.dumps(doc))
            
            # If not found locally, try master for location
            try:
                worker_response = self.master_stub.GetDocumentLocation(
                    database_pb2.DocumentID(
                        db_name=request.db_name,
                        doc_id=request.doc_id
                    ), timeout=2)
                
                if worker_response.worker:
                    logger.info(f"Document located on worker: {worker_response.worker}")
                    if worker_response.worker not in self.replica_channels:
                        self.replica_channels[worker_response.worker] = grpc.insecure_channel(worker_response.worker)
                    
                    stub = database_pb2_grpc.DatabaseServiceStub(
                        self.replica_channels[worker_response.worker])
                    return stub.ReadDocument(request, timeout=2)
            
            except Exception as e:
                logger.error(f"Error contacting master or other worker: {str(e)}")
            
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentResponse()
            
        except Exception as e:
            logger.error(f"Error in ReadDocument: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentResponse()
    

    def ReadAllDocuments(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.name}.json":
                self.manager.use_database(request.name)
            docs = self.manager.current_db['db'].read_all_documents()
            return database_pb2.DocumentList(documents=[json.dumps(doc) for doc in docs])
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentList()

    def QueryDocuments(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            filter_func = eval(request.filter_expr)
            if not callable(filter_func):
                raise ValueError("Filter expression must be callable")
            docs = self.manager.current_db['db'].query_documents(filter_func)
            return database_pb2.DocumentList(documents=[json.dumps(doc) for doc in docs])
        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return database_pb2.DocumentList()

    def UpdateDocument(self, request, context):
        try:
            logger.info(f"UpdateDocument: db={request.db_name}, doc_id={request.doc_id}")
            
            # Step 1: Ensure correct database
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                logger.info(f"Switching to database {request.db_name}")
                self.manager.use_database(request.db_name)
            
            # Step 2: Retrieve document
            current_doc = self.manager.current_db['db'].read_document(request.doc_id)
            if not current_doc:
                logger.info(f"Document {request.doc_id} not found in {request.db_name}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found"
                )
            logger.info(f"Found document {request.doc_id}: {current_doc}")
            
            # Step 3: Check if primary
            if not current_doc.get('_primary', False):
                logger.info(f"Worker {self.worker_address} is not primary for {request.doc_id}")
                primary_worker = self.master_stub.GetDocumentPrimary(
                    database_pb2.DocumentID(db_name=request.db_name, doc_id=request.doc_id)
                )
                if not primary_worker.worker:
                    logger.warning("No primary worker found for document")
                    return database_pb2.OperationResponse(
                        success=False,
                        message="No primary worker found for document"
                    )
                logger.info(f"Forwarding update to primary {primary_worker.worker}")
                channel = self.replica_channels.get(primary_worker.worker) or grpc.insecure_channel(primary_worker.worker)
                stub = database_pb2_grpc.DatabaseServiceStub(channel)
                return stub.UpdateDocument(request)
            
            # Step 4: Perform local update (primary)
            updates = json.loads(request.updates)
            current_doc.update(updates)
            current_doc['_updated_at'] = datetime.now().isoformat()
            current_doc['_version'] = current_doc.get('_version', 0) + 1
            self.manager.current_db['db']._save()
            logger.info(f"Document {request.doc_id} updated locally on primary {self.worker_address}: {current_doc}")
            
            # Step 5: Replicate to secondaries
            replication_status = self._replicate_document(request.db_name, request.doc_id, current_doc)
            
            # Step 6: Return success regardless of replication outcome
            logger.info(f"Update completed: {replication_status}")
            return database_pb2.OperationResponse(
                success=True,
                message=replication_status
            )
        
        except Exception as e:
            logger.error(f"UpdateDocument failed: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(
                success=False,
                message=f"Update failed: {str(e)}"
            )
        

    def _replicate_document(self, db_name: str, doc_id: str, current_doc: dict) -> str:
        """Handle replication to secondary workers"""
        try:
            logger.info(f"Starting replication for {db_name}/{doc_id}")
            
            # Get replicas from master
            replicas = []
            try:
                replicas = self.master_stub.GetDocumentReplicas(
                    database_pb2.DocumentID(db_name=db_name, doc_id=doc_id),
                    timeout=2
                ).workers
                logger.info(f"Got replicas from master: {replicas}")
            except Exception as e:
                logger.error(f"Failed to get replicas from master: {str(e)}")
                return "Updated primary but failed to get replicas"
            
            replica_addresses = [addr for addr in replicas if addr != self.worker_address]
            if not replica_addresses:
                return "Updated primary with no replicas"
            
            replicate_request = database_pb2.ReplicateRequest(
                db_name=db_name,
                doc_id=doc_id,
                document=json.dumps(current_doc),
                is_update=True
            )
            
            successful_replicas = 0
            for replica_addr in replica_addresses:
                try:
                    if replica_addr not in self.replica_channels:
                        self.replica_channels[replica_addr] = grpc.insecure_channel(replica_addr)
                    
                    stub = database_pb2_grpc.DatabaseServiceStub(self.replica_channels[replica_addr])
                    response = stub.ReplicateDocument(replicate_request, timeout=2)
                    if response.success:
                        successful_replicas += 1
                        logger.info(f"Replicated to {replica_addr}")
                    else:
                        logger.warning(f"Replication failed to {replica_addr}: {response.message}")
                except Exception as e:
                    logger.error(f"Error replicating to {replica_addr}: {str(e)}")
            
            return f"Updated primary and {successful_replicas}/{len(replica_addresses)} replicas"
        
        except Exception as e:
            logger.error(f"Replication failed: {str(e)}", exc_info=True)
            return f"Updated primary, replication failed: {str(e)}"
        

    def DeleteDocument(self, request, context):
        try:
            logger.info(f"DeleteDocument: db={request.db_name}, doc_id={request.doc_id}")

            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)

            current_doc = self.manager.current_db['db'].read_document(request.doc_id)
            if not current_doc:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found"
                )

            # If not primary, forward to primary
            if not current_doc.get('_primary', False):
                primary_worker = self.master_stub.GetDocumentPrimary(
                    database_pb2.DocumentID(db_name=request.db_name, doc_id=request.doc_id)
                ).worker
                if not primary_worker:
                    return database_pb2.OperationResponse(
                        success=False,
                        message="No primary worker found for document"
                    )

                channel = self.replica_channels.get(primary_worker) or grpc.insecure_channel(primary_worker)
                stub = database_pb2_grpc.DatabaseServiceStub(channel)
                return stub.DeleteDocument(request)

            # We are primary - delete document
            success = self.manager.current_db['db'].delete_document(request.doc_id)

            # Delete shard metadata if exists
            if success and request.doc_id in self.manager.current_db['shards'].document_shards:
                del self.manager.current_db['shards'].document_shards[request.doc_id]

            # Notify replicas to delete
            replicas = self.master_stub.GetDocumentReplicas(
                database_pb2.DocumentID(db_name=request.db_name, doc_id=request.doc_id)
            ).workers
            replica_addresses = [addr for addr in replicas if addr != self.worker_address]

            delete_request = database_pb2.DeleteRequest(
                db_name=request.db_name,
                doc_id=request.doc_id
            )

            with futures.ThreadPoolExecutor(max_workers=5) as executor:
                for replica_addr in replica_addresses:
                    try:
                        channel = self.replica_channels.get(replica_addr) or grpc.insecure_channel(replica_addr)
                        stub = database_pb2_grpc.DatabaseServiceStub(channel)
                        executor.submit(stub.DeleteDocument, delete_request)
                    except Exception as e:
                        logger.warning(f"Failed to notify replica {replica_addr} for deletion: {str(e)}")

            return database_pb2.OperationResponse(
                success=success,
                message="Document deleted" if success else "Document not found"
            )

        except Exception as e:
            logger.error(f"Delete failed: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )

    def ClearDatabase(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.name}.json":
                self.manager.use_database(request.name)
            self.manager.current_db['db'].clear_database()
            self.manager.current_db['shards'] = ShardMetadata()
            return database_pb2.OperationResponse(
                success=True,
                message="Database cleared"
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )
    
    def GetShardLocations(self, request, context):
        try:
            if request.name not in self.manager.databases:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.WorkerList()
            workers = self.manager.databases[request.name]['shards'].get_all_shard_workers()
            if not workers:
                workers = [self.worker_address]
            return database_pb2.WorkerList(workers=workers)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.WorkerList()

def serve_worker(port: int, master_addresses: List[str]):
    """
    Start a worker node.
    
    Args:
        port: Port for worker gRPC service
        master_addresses: List of master node addresses (host:port)
    """
    worker_address = f"localhost:{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        DatabaseService(worker_address, master_addresses), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    logger.info(f"Worker node running on port {port}")
    logger.info(f"Master addresses: {master_addresses}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down worker node")
        server.stop(0)

if __name__ == '__main__':
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    master_addrs = sys.argv[2:] if len(sys.argv) > 2 else ["localhost:50050"]
    serve_worker(port, master_addrs)
