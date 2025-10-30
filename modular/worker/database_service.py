import ast
import hashlib
import json
import os
import threading
from typing import List, Dict, Optional
from typing import Tuple
import uuid
from datetime import datetime
from concurrent import futures
import grpc
import database_pb2
import database_pb2_grpc
import logging
import time
from .logger_util import logger
from .simpleJSON import SimpleJSONDB
from .shard_metadata import ShardMetadata
from .index import BPlusTree , BPlusTreeNode , IndexManager
from .database_manager import DatabaseManager
from .request_logger import log_request

class DatabaseService(database_pb2_grpc.WorkerServiceServicer):
    def __init__(self, worker_address: str, master_addresses: str):
        self.manager = DatabaseManager()
        self.worker_address = worker_address
        self.worker_port = int(worker_address.split(":")[-1])  # Extract port
        self.master_addresses = master_addresses
        self.current_leader: Optional[str] = None
        self.known_workers: List[str] = [worker_address]
        self.master_channel: Optional[grpc.Channel] = None
        self.master_stub: Optional[database_pb2_grpc.MasterServiceStub] = None
        self.replica_channels: Dict[str, grpc.Channel] = {}  # Cache for replica channels
        self.shard_metadata = ShardMetadata()  # Initialize shard metadata

        # Connection management
        self.leader_lock = threading.Lock()
        self.retry_backoff = 1.0  # Initial backoff in seconds
        self.max_backoff = 30.0   # Maximum backoff time
        self.heartbeat_interval = 5  # Seconds between heartbeats
        self.stop_flag = False  # Added missing stop_flag

        # Add these new attributes
        self.leader_validation_interval = 10  # Seconds between leader validations
        self.last_leader_validation = 0
        self.healthy_leader = False

        # Enhanced connection parameters
        self.connection_timeout = 3.0  # seconds
        self.max_reconnect_attempts = 5
        self.leader_retry_interval = 1.0
        self.max_port_attempts = 3  # Try multiple ports if needed
        self.leader_health_threshold = 3  # consecutive failures before marking unhealthy
        self.leader_health_count = 0
        self.channel_cache = {}  # To store and manage channels
        self.current_master_index = 0  # Add this line
        
        
        # Initialize master connection
        self._discover_master_leader()
        self._register_with_master()  # Add this line
        self._discover_workers()
        
        # Start background threads
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
        self.heartbeat_thread.start()
        self.leader_monitor_thread = threading.Thread(target=self._monitor_leader, daemon=True)
        self.leader_monitor_thread.start()

    def stop(self):
        """Clean up all channels on shutdown"""
        for channel in self.channel_cache.values():
            channel.close()
        self.channel_cache.clear()
        if self.master_channel:
            self.master_channel.close()

    def _try_connect_leader(self, leader_address: str) -> Optional[Tuple[grpc.Channel, database_pb2_grpc.MasterServiceStub]]:
        """Try multiple connection approaches to the leader"""
        # Try standard port first
        try:
            if leader_address in self.channel_cache:
                channel = self.channel_cache[leader_address]
            else:
                channel = grpc.insecure_channel(leader_address)
                self.channel_cache[leader_address] = channel
            
            stub = database_pb2_grpc.MasterServiceStub(channel)
            # Verify connection
            resp = stub.GetLeader(database_pb2.Empty(), timeout=2)
            if resp.leader_address == leader_address.split(':')[0]:
                return channel, stub
        except grpc.RpcError:
            pass

        # If standard port fails, try common alternatives
        possible_ports = [50050, 50051, 50052]  # Add all possible ports
        host = leader_address.split(':')[0]
        
        for port in possible_ports:
            addr = f"{host}:{port}"
            try:
                if addr in self.channel_cache:
                    channel = self.channel_cache[addr]
                else:
                    channel = grpc.insecure_channel(addr)
                    self.channel_cache[addr] = channel
                
                stub = database_pb2_grpc.MasterServiceStub(channel)
                resp = stub.GetLeader(database_pb2.Empty(), timeout=2)
                if resp.leader_address:
                    logger.info(f"Connected to leader on alternate port {port}")
                    return channel, stub
            except grpc.RpcError:
                continue

        return None, None


    def _create_secure_channel(self, address: str) -> grpc.Channel:
        """Create a channel with proper timeout and retry policies"""
        options = [
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', 1),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.http2.min_time_between_pings_ms', 10000),
            ('grpc.http2.min_ping_interval_without_data_ms', 5000),
        ]
        return grpc.insecure_channel(address, options=options)
    
    def _rediscover_leader(self) -> Optional[database_pb2_grpc.MasterServiceStub]:
        """Leader discovery with current_master_index"""
        last_error = None
        
        for i in range(len(self.master_addresses)):
            try:
                idx = (self.current_master_index + i) % len(self.master_addresses)
                addr = self.master_addresses[idx]
                
                channel = grpc.insecure_channel(addr)
                stub = database_pb2_grpc.MasterServiceStub(channel)
                
                leader_info = stub.GetLeader(database_pb2.Empty(), timeout=2)
                
                if not leader_info.leader_address:
                    continue
                    
                # Try connecting to reported leader
                leader_channel = grpc.insecure_channel(leader_info.leader_address)
                leader_stub = database_pb2_grpc.MasterServiceStub(leader_channel)
                
                # Verify leadership
                verify_response = leader_stub.GetLeader(database_pb2.Empty(), timeout=2)
                if verify_response.leader_address == leader_info.leader_address:
                    with self.leader_lock:
                        if self.master_channel:
                            self.master_channel.close()
                        
                        self.current_leader = leader_info.leader_address
                        self.master_channel = leader_channel
                        self.master_stub = leader_stub
                        self.current_master_index = idx
                        
                        logger.info(f"Connected to new leader at {self.current_leader}")
                        return leader_stub
                
            except grpc.RpcError as e:
                last_error = f"Failed to connect to {addr}: {e.details()}"
                continue
                
        logger.error(f"Could not discover leader. Last error: {last_error}")
        return None

    def _validate_leader(self, stub: database_pb2_grpc.MasterServiceStub) -> bool:
        """Robust leader validation with health tracking"""
        try:
            response = stub.GetLeader(
                database_pb2.Empty(), 
                timeout=self.connection_timeout
            )
            if response.leader_address == self.current_leader:
                self.leader_health_count = max(0, self.leader_health_count - 1)
                return True
            
            logger.info(f"Leader changed from {self.current_leader} to {response.leader_address}")
            return False
        except grpc.RpcError as e:
            self.leader_health_count += 1
            if self.leader_health_count >= self.leader_health_threshold:
                logger.warning(f"Leader validation failed {self.leader_health_count} times: {str(e)}")
                return False
            return True  # Give leader a few chances before marking unhealthy
        except Exception as e:
            logger.error(f"Unexpected validation error: {str(e)}")
            return False
        
    def _get_leader_stub(self) -> Optional[database_pb2_grpc.MasterServiceStub]:
        """Get validated leader stub with connection pooling"""
        if not self.current_leader:
            return self._rediscover_leader()
        
        try:
            if not self.master_channel or self.master_channel._channel.check_connectivity_state(False) != grpc.ChannelConnectivity.READY:
                self.master_channel = grpc.insecure_channel(self.current_leader)
                self.master_stub = database_pb2_grpc.MasterServiceStub(self.master_channel)
            
            # Quick validation
            response = self.master_stub.GetLeader(database_pb2.Empty(), timeout=1)
            if response.leader_address == self.current_leader:
                return self.master_stub
                
        except Exception as e:
            logger.warning(f"Leader stub validation failed: {str(e)}")
        
        return self._rediscover_leader()
    
    
    def _register_with_master(self, max_retries: int = 5) -> bool:
        """Register this worker with the master cluster"""
        for attempt in range(max_retries):
            if self.stop_flag:
                return False
            try:
                stub = self._get_leader_stub()
                if not stub:
                    logger.warning(f"Registration attempt {attempt+1}: No master leader available")
                    time.sleep(1)
                    continue
                
                logger.info(f"Attempting to register with master at {self.current_leader}")
                channel = grpc.insecure_channel(self.current_leader)
                stub = database_pb2_grpc.MasterServiceStub(channel)

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
                with self.leader_lock:
                    self.current_leader = None
                    self.master_stub = None
                    if self.master_channel:
                        self.master_channel.close()
                        self.master_channel = None
            
            time.sleep(1)  # Wait before retrying
        
        logger.error(f"Failed to register worker after {max_retries} attempts")
        return False

    def _discover_master_leader(self) -> None:
        """Discover the current leader among master nodes"""
        for master_addr in self.master_addresses:
            try:
                channel = grpc.insecure_channel(master_addr)
                stub = database_pb2_grpc.MasterServiceStub(channel)
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
        """Enhanced leader monitoring with better error handling"""
        while not self.stop_flag:
            try:
                # Get current leader stub with validation
                stub = self._get_leader_stub()
                
                if not stub:
                    logger.warning("No leader stub available, retrying...")
                    time.sleep(1)
                    continue

                # Check leader responsiveness
                try:
                    response = stub.GetLeader(database_pb2.Empty(), timeout=2)
                    if not response.leader_address:
                        raise grpc.RpcError("Empty leader response")
                    
                    # Verify we're still connected to the actual leader
                    if response.leader_address != self.current_leader:
                        logger.info(f"Leader changed from {self.current_leader} to {response.leader_address}")
                        with self.leader_lock:
                            self.current_leader = response.leader_address
                            if self.master_channel:
                                self.master_channel.close()
                            self.master_channel = grpc.insecure_channel(self.current_leader)
                            self.master_stub = database_pb2_grpc.MasterServiceStub(self.master_channel)
                    
                    time.sleep(self.heartbeat_interval)
                    
                except grpc.RpcError as e:
                    logger.warning(f"Leader check failed: {e.details()}")
                    with self.leader_lock:
                        self.current_leader = None
                        if self.master_channel:
                            self.master_channel.close()
                            self.master_channel = None
                        self.master_stub = None
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Unexpected error in leader monitor: {str(e)}")
                time.sleep(1)


    def _discover_workers(self) -> None:
        """Discover other workers in the cluster"""
        if not self.master_stub:
            return
            
        for _ in range(5):  # Retry 5 times
            try:
                stub = self._get_leader_stub()
                if not stub:
                    time.sleep(1)
                    continue

                response = stub.ListWorkers(database_pb2.Empty())
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
        """Heartbeat implementation without timestamp"""
        while not self.stop_flag:
            try:
                stub = self._get_leader_stub()
                if not stub:
                    time.sleep(1)
                    continue

                try:
                    response = stub.Heartbeat(
                        database_pb2.HeartbeatRequest(
                            worker_address=self.worker_address
                            # Remove timestamp if not in proto definition
                        ),
                        timeout=self.connection_timeout
                    )
                    
                    if response.acknowledged:
                        logger.info(f"Heartbeat acknowledged by leader.")
                        self.leader_health_count = max(0, self.leader_health_count - 1)
                    else:
                        self._handle_leader_failure()
                        
                except grpc.RpcError as e:
                    self._handle_leader_failure(e.code())
                    
            except Exception as e:
                logger.error(f"Unexpected heartbeat error: {str(e)}")
                self._handle_leader_failure()
            
            time.sleep(self.heartbeat_interval)

    def _update_database_list(self, db_name: str, action: str = 'add') -> None:
        """Maintain a single file with all database names"""
        db_list_file = "databases.json"
        try:
            # Read existing list
            db_list = []
            if os.path.exists(db_list_file):
                with open(db_list_file, 'r') as f:
                    db_list = json.load(f).get('databases', [])
            
            # Update list
            if action == 'add' and db_name not in db_list:
                db_list.append(db_name)
            elif action == 'remove' and db_name in db_list:
                db_list.remove(db_name)
            
            # Write back to file
            with open(db_list_file, 'w') as f:
                json.dump({'databases': db_list}, f)
                
        except Exception as e:
            logger.error(f"Failed to update database list: {str(e)}")



    def _handle_leader_failure(self, error_code=None):
        """Enhanced leader failure handling"""
        self.leader_health_count += 1
        
        if error_code == grpc.StatusCode.FAILED_PRECONDITION:
            logger.info("Connected node is not leader anymore")
        elif error_code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
            logger.warning("Leader connection issue detected")
        
        # Reset connection if threshold reached
        if self.leader_health_count >= self.leader_health_threshold:
            with self.leader_lock:
                self.current_leader = None
                if self.master_channel:
                    try:
                        self.master_channel.close()
                    except:
                        pass
                    self.master_channel = None
                self.master_stub = None
            logger.warning("Reset leader connection due to repeated failures")


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
            log_request(self.worker_port, f"CreateDatabase request: db={request.name}")
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

            # Create empty database file
            with open(db_file, 'w') as f:
                json.dump({}, f)
                f.flush()
                os.fsync(f.fileno())

            # Update the central database list
            self._update_database_list(request.name, 'add')

            # Initialize IndexManager
            index_manager = IndexManager()

            # Create indexes passed from master
            for field in request.indexes:
                index_manager.create_index(field)

            # Store database in manager
            if request.name not in self.manager.databases:
                self.manager.databases[request.name] = {
                    'db': SimpleJSONDB(db_file),
                    'index_manager': index_manager,
                    'shards': ShardMetadata()
                }
                self.manager.current_db = self.manager.databases[request.name]
                print(f"Database at worker side stored: {self.manager.current_db}")

            # Verify file creation
            if not os.path.exists(db_file):
                return database_pb2.OperationResponse(
                    success=False,
                    message="Failed to create database file"
                )

            return database_pb2.OperationResponse(
                success=True,
                message=f"Database '{request.name}' created with indexes: {list(request.indexes)}"
            )

        except Exception as e:
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )


    def UseDatabase(self, request, context):
        try:
            db_file = f"{request.name}.json"
            log_request(self.worker_port, f"UseDatabase request: db={request.name}")
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
            print(f"database created by worker")
            print(f"Type of current_db['db']: {type(self.manager.current_db['db'])}")


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
        try:
            db_list_file = "databases.json"
            if os.path.exists(db_list_file):
                with open(db_list_file, 'r') as f:
                    db_list = json.load(f).get('databases', [])
                return database_pb2.DatabaseList(names=db_list)
            return database_pb2.DatabaseList(names=list(self.manager.databases.keys()))
        except Exception as e:
            logger.error(f"Error listing databases: {str(e)}")
            return database_pb2.DatabaseList(names=list(self.manager.databases.keys()))
    
    def DeleteDatabase(self, request, context):
        try:
            if request.name in self.manager.databases:
                if os.path.exists(f"{request.name}.json"):
                    os.remove(f"{request.name}.json")
                del self.manager.databases[request.name]
                if (self.manager.current_db and 
                    self.manager.current_db['db'].db_file == f"{request.name}.json"):
                    self.manager.current_db = None
                 # Update the central database list
                self._update_database_list(request.name, 'remove')
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
            
            #if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
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
            log_request(self.worker_port, f"CreateDocument request: db={request.db_name}, doc_id={request.doc_id}, content={doc_data}")

            
            # Store document locally
            # self.manager.current_db['db'].create_document(doc_data, doc_id)
            # Correct:
            self.manager.current_db['db'].create_document(doc_data, doc_id)

             # Update Indexes (B+ Tree) for relevant fields
            for field, index in self.manager.current_db['index_manager'].indexes.items():
                if field in doc_data:
                    index.insert(doc_data[field], doc_id)

            
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

                    print(f"secondary worker : " , self.replica_channels[replica_addr])
                    stub = database_pb2_grpc.WorkerServiceStub(self.replica_channels[replica_addr])
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
                    stub = database_pb2_grpc.WorkerServiceStub(channel)
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
                    
                    stub = database_pb2_grpc.WorkerServiceStub(self.replica_channels[w])
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
            logger.info(f"Replicating document {request.doc_id} to {self.worker_address}")

            # Ensure the right DB is selected
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)

            db = self.manager.current_db['db']
            doc_data = json.loads(request.document)
            doc_id = request.doc_id

            existing_doc = db.read_document(doc_id)

            if existing_doc:
                logger.info(f"Document {doc_id} exists, updating...")
                existing_doc.update(doc_data)
                if '_primary' in doc_data:
                    existing_doc['_primary'] = doc_data['_primary']
                existing_doc['_updated_at'] = datetime.now().isoformat()
                db._save()
            else:
                logger.info(f"Document {doc_id} does not exist, creating...")
                doc_data['_created_at'] = datetime.now().isoformat()
                doc_data['_updated_at'] = datetime.now().isoformat()
                db.create_document(doc_data, doc_id)

            logger.info(f"Replication successful for document {doc_id}")
            return database_pb2.OperationResponse(success=True)

        except Exception as e:
            logger.error(f"Replication failed: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )


    def ReadDocument(self, request, context):
        try:
            logger.info(f"ReadDocument: db={request.db_name} doc_id={request.doc_id}")
            log_request(self.worker_port, f"ReadDocument: db={request.db_name}, doc_id={request.doc_id}")

            
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
                    
                    stub = database_pb2_grpc.WorkerServiceStub(
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
            log_request(self.worker_port, f"ReadAllDocument")
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.name}.json":
                self.manager.use_database(request.name)
            docs = self.manager.current_db['db'].read_all_documents()
            return database_pb2.DocumentList(documents=[json.dumps(doc) for doc in docs])
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentList()
        



    def _parse_simple_filter(self, filter_expr: str) -> tuple:
        """Parse simple field comparison filters like:
        - "lambda doc: doc.get('name') == 'pina'"
        - "lambda doc: doc['age'] > 30"
        Returns (field_name, operator, value) or (None, None, None) if not a simple filter
        """
        # Remove the lambda prefix if present
        expr = filter_expr.strip()
        if expr.startswith("lambda doc:"):
            expr = expr[len("lambda doc:"):].strip()
        
        # Parse comparison operations
        ops = [
            ('==', 'eq'),
            ('>', 'gt'),
            ('<', 'lt'),
            ('>=', 'gte'),
            ('<=', 'lte')
        ]
        
        for op_symbol, op_name in ops:
            if op_symbol in expr:
                parts = expr.split(op_symbol, 1)
                if len(parts) == 2:
                    left, right = parts
                    field = self._extract_field_name(left.strip())
                    if field:
                        try:
                            value = ast.literal_eval(right.strip())
                            return (field, op_name, value)
                        except (ValueError, SyntaxError):
                            pass
        return (None, None, None)
    

    def _extract_field_name(self, expr: str) -> Optional[str]:
        """Extract field name from expressions like:
        - doc.get('name')
        - doc['age']
        - doc.name
        """
        # Handle doc.get('field')
        if expr.startswith("doc.get(") and expr.endswith(")"):
            arg = expr[len("doc.get("):-1].strip()
            if (arg.startswith("'") and arg.endswith("'")) or (arg.startswith('"') and arg.endswith('"')):
                return arg[1:-1]
        
        # Handle doc['field']
        elif expr.startswith("doc[") and expr.endswith("]"):
            arg = expr[len("doc["):-1].strip()
            if (arg.startswith("'") and arg.endswith("'")) or (arg.startswith('"') and arg.endswith('"')):
                return arg[1:-1]
        
        # Handle doc.field
        elif expr.startswith("doc."):
            return expr[len("doc."):]
        
        return None
    

    def _create_filter_func(self, filter_expr: str) -> Optional[callable]:
        """Create a filter function from the expression without using eval()"""
        field, op, value = self._parse_simple_filter(filter_expr)
        if field and op and value is not None:
            if op == 'eq':
                return lambda doc: doc.get(field) == value
            elif op == 'gt':
                return lambda doc: doc.get(field) > value
            elif op == 'lt':
                return lambda doc: doc.get(field) < value
            elif op == 'gte':
                return lambda doc: doc.get(field) >= value
            elif op == 'lte':
                return lambda doc: doc.get(field) <= value
        
        # For more complex filters, we could implement a simple parser
        # But for now, we'll just return None which means "match all"
        return None



    def QueryDocuments(self, request, context):
        try:
            logger.info(f"QueryDocuments called on db={request.db_name} with filter_expr={request.filter_expr}")
            log_request(self.worker_port, f"QueryDocuments called on db={request.db_name} with filter_expr={request.filter_expr}")
            self.manager.use_database(request.db_name)

            filter_expr = request.filter_expr
            results = []

            # First try to parse as a simple field comparison that can use the index
            field, op, value = self._parse_simple_filter(filter_expr)
            
            if field and op and value is not None:
                # Check if we have an index for this field
                index = self.manager.current_db['index_manager'].get_index(field)
                if index:
                    logger.info(f"Using index for field '{field}' with op '{op}' and value '{value}'")
                    doc_ids = index.search(op, value)
                    for doc_id in doc_ids:
                        doc = self.manager.current_db['db'].read_document(doc_id)
                        if doc:  # Verify document still exists
                            results.append(json.dumps(doc))
                    return database_pb2.DocumentList(documents=results)
                else:
                    logger.info(f"No index available for field '{field}'")

            # Fallback to brute-force filtering without eval
            all_docs = self.manager.current_db['db'].read_all_documents()
            filter_func = self._create_filter_func(filter_expr)
            
            if filter_func:
                for doc in all_docs:
                    if filter_func(doc):
                        results.append(json.dumps(doc))
            else:
                # If we can't parse the filter, return all documents
                results = [json.dumps(doc) for doc in all_docs]

            return database_pb2.DocumentList(documents=results)

        except Exception as e:
            logger.error(f"QueryDocuments failed: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.DocumentList(documents=[])


    def apply_local_update(self, db_name: str, doc_id: str, updates: Dict):
        """Applies an update to a document locally without replication or contacting master"""
        self.UseDatabase(database_pb2.DatabaseName(name=db_name), None)
        try:
            # Read the document
            #document = self.manager.current_db['db'].read_document(doc_id)
            document = self.ReadDocument(database_pb2.DocumentID(db_name=db_name, doc_id=doc_id),None)
            print(f"document locally readed : " , document)
            # Apply updates
            for key, value in updates.items():
                document[key] = value
            # Write updated document
            self.manager.write_document(doc_id, document)
            logger.info(f"Locally updated document {doc_id} in DB {db_name}")
        except Exception as e:
            logger.error(f"Failed to apply local update to document {doc_id} in DB {db_name}: {e}")



    def UpdateDocument(self, request, context):
        try:
            logger.info(f"UpdateDocument: db={request.db_name}, doc_id={request.doc_id}")

            # Step 1: Ensure correct database
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                logger.info(f"Switching to database {request.db_name}")
                self.manager.use_database(request.db_name)
                if not self.manager.current_db:
                    logger.error(f"Failed to switch to database {request.db_name}")
                    if context is not None:
                        context.set_code(grpc.StatusCode.INTERNAL)
                        context.set_details(f"Database {request.db_name} not found")
                    return database_pb2.OperationResponse(success=False, message=f"Database {request.db_name} not found")

            # Step 2: Retrieve document
            logger.debug(f"Attempting to read document {request.doc_id} from {self.manager.current_db['db'].db_file}")
            current_doc = self.manager.current_db['db'].read_document(request.doc_id)
            if not current_doc:
                logger.warning(f"Document {request.doc_id} not found in {request.db_name}")
                if context is not None:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details("Document not found")
                else:
                    logger.info(f"Document {request.doc_id} not found during log replay")
                return database_pb2.OperationResponse(success=False, message="Document not found")

            logger.info(f"Found document {request.doc_id}: {current_doc}")

            # Step 3: Check if primary
            if not current_doc.get('_primary', False):
                logger.info(f"Worker {self.worker_address} is not primary for {request.doc_id}")
                primary_worker = self.master_stub.GetDocumentPrimary(
                    database_pb2.DocumentID(db_name=request.db_name, doc_id=request.doc_id)
                )
                if not primary_worker.worker:
                    logger.warning("No primary worker found for document")
                    return database_pb2.OperationResponse(success=False, message="No primary worker found for document")

                primary_addr = primary_worker.worker
                if primary_addr not in self.replica_channels:
                    self.replica_channels[primary_addr] = grpc.insecure_channel(primary_addr)
                stub = database_pb2_grpc.WorkerServiceStub(self.replica_channels[primary_addr])
                return stub.UpdateDocument(request)

            # Step 4: Prepare index update
            updates = json.loads(request.updates)
            log_request(self.worker_port, f"UpdateDocument: db={request.db_name}, doc_id={request.doc_id}, updates={updates}")
            index_manager = self.manager.current_db['index_manager']
            for field, index in index_manager.indexes.items():
                if field in current_doc:
                    index.delete(current_doc[field], request.doc_id)  # Remove old value

            # Step 5: Perform update
            current_doc.update(updates)
            current_doc['_updated_at'] = datetime.now().isoformat()
            current_doc['_version'] = current_doc.get('_version', 0) + 1
            self.manager.current_db['db']._save()
            logger.debug(f"Document {request.doc_id} updated locally: {current_doc}")

            # Step 6: Insert updated values into index
            for field, index in index_manager.indexes.items():
                if field in updates:
                    index.insert(updates[field], request.doc_id)

            logger.info(f"Document {request.doc_id} updated locally on primary {self.worker_address}: {current_doc}")

            # Step 7: Replicate to secondaries
            replication_status = self._replicate_document(request.db_name, request.doc_id, current_doc)

            return database_pb2.OperationResponse(success=True, message=replication_status)

        except Exception as e:
            logger.error(f"UpdateDocument failed: {str(e)}", exc_info=True)
            if context is not None:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
            return database_pb2.OperationResponse(success=False, message=f"Update failed: {str(e)}")
        
        
        



    def UpdateLogDocument(self, request, context):
        try:
            logger.info(f"UpdateDocument: db={request.db_name}, doc_id={request.doc_id}")

            # Step 1: Ensure correct database
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                logger.info(f"Switching to database {request.db_name}")
                self.manager.use_database(request.db_name)
                if not self.manager.current_db:
                    logger.error(f"Failed to switch to database {request.db_name}")
                    if context is not None:
                        context.set_code(grpc.StatusCode.INTERNAL)
                        context.set_details(f"Database {request.db_name} not found")
                    return database_pb2.OperationResponse(success=False, message=f"Database {request.db_name} not found")

            # Step 2: Retrieve document
            logger.debug(f"Attempting to read document {request.doc_id} from {self.manager.current_db['db'].db_file}")
            current_doc = self.manager.current_db['db'].read_document(request.doc_id)
            if not current_doc:
                logger.warning(f"Document {request.doc_id} not found in {request.db_name}")
                if context is not None:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details("Document not found")
                else:
                    logger.info(f"Document {request.doc_id} not found during log replay")
                return database_pb2.OperationResponse(success=False, message="Document not found")

            logger.info(f"Found document {request.doc_id}: {current_doc}")

            # Step 3: Prepare index update
            updates = json.loads(request.updates)
            log_request(self.worker_port, f"UpdateDocument: db={request.db_name}, doc_id={request.doc_id}, updates={updates}")
            index_manager = self.manager.current_db['index_manager']
            for field, index in index_manager.indexes.items():
                if field in current_doc:
                    index.delete(current_doc[field], request.doc_id)  # Remove old value

            # Step 4: Perform update
            current_doc.update(updates)
            current_doc['_updated_at'] = datetime.now().isoformat()
            current_doc['_version'] = current_doc.get('_version', 0) + 1
            self.manager.current_db['db']._save()
            logger.debug(f"Document {request.doc_id} updated locally: {current_doc}")

            # Step 5: Insert updated values into index
            for field, index in index_manager.indexes.items():
                if field in updates:
                    index.insert(updates[field], request.doc_id)

            logger.info(f"Document {request.doc_id} updated locally: {current_doc}")
            return database_pb2.OperationResponse(success=True, message="Document updated locally")

        except Exception as e:
            logger.error(f"UpdateDocument failed: {str(e)}", exc_info=True)
            if context is not None:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
            return database_pb2.OperationResponse(success=False, message=f"Update failed: {str(e)}")






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
                    
                    stub = database_pb2_grpc.WorkerServiceStub(self.replica_channels[replica_addr])
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
            log_request(self.worker_port, f"DeleteDocument: db={request.db_name}, doc_id={request.doc_id}")

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
                stub = database_pb2_grpc.WorkerServiceStub(channel)
                return stub.DeleteDocument(request)

            # We are primary - delete document locally
            success = self.manager.current_db['db'].delete_document(request.doc_id)

            if not success:
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found or already deleted"
                )

            # Notify replicas to delete the document
            replicas = self.master_stub.GetDocumentReplicas(
                database_pb2.DocumentID(db_name=request.db_name, doc_id=request.doc_id)
            ).workers

            replica_addresses = [addr for addr in replicas if addr != self.worker_address]

            delete_request = database_pb2.DeleteRequest(
                db_name=request.db_name,
                doc_id=request.doc_id
            )

            ack_count = 0
            with futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_replica = {
                    executor.submit(self._send_delete_to_replica, addr, delete_request): addr
                    for addr in replica_addresses
                }
                for future in futures.as_completed(future_to_replica):
                    replica = future_to_replica[future]
                    try:
                        response = future.result()
                        if response.success:
                            ack_count += 1
                    except Exception as e:
                        logger.warning(f"Replica {replica} failed to delete document: {str(e)}")

            logger.info(f"Delete acknowledgments received: {ack_count}/{len(replica_addresses)}")

            # You may optionally mark the document as deleted in metadata if needed
            # or do cleanup of local tracking (not shard metadata)

            return database_pb2.OperationResponse(
                success=True,
                message="Document deleted"
            )

        except Exception as e:
            logger.error(f"Delete failed: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )
        
    def _send_delete_to_replica(self, replica_addr, delete_request):
        channel = self.replica_channels.get(replica_addr)
        if not channel:
            channel = grpc.insecure_channel(replica_addr)
            self.replica_channels[replica_addr] = channel

        stub = database_pb2_grpc.WorkerServiceStub(channel)
        return stub.DeleteDocument(delete_request)




    def ClearDatabase(self, request, context):
        try:
            log_request(self.worker_port, f"ClearDatabase: db={request.db_name}")
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.name}.json":
                self.manager.use_database(request.name)
            self.manager.current_db['db'].clear_database()
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
