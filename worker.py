import hashlib
import json
import os
import threading
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
    def __init__(self, db_file):
        self.db_file = db_file
        self.data = {}
        if os.path.exists(db_file):
            with open(db_file, 'r') as f:
                self.data = json.load(f)

    def create_document(self, document, doc_id):
        self.data[doc_id] = document
        self._save()

    def read_document(self, doc_id):
        return self.data.get(doc_id)

    def read_all_documents(self):
        return list(self.data.values())

    def query_documents(self, filter_func):
        return [doc for doc in self.data.values() if filter_func(doc)]

    def update_document(self, doc_id, updates, create_if_missing=False):
        if doc_id in self.data:
            self.data[doc_id].update(updates)
            self._save()
            return True
        elif create_if_missing:
            self.data[doc_id] = updates
            self._save()
            return True
        return False

    def delete_document(self, doc_id):
        if doc_id in self.data:
            del self.data[doc_id]
            self._save()
            return True
        return False

    def clear_database(self):
        self.data = {}
        self._save()

    def _save(self):
        with open(self.db_file, 'w') as f:
            json.dump(self.data, f, indent=2)

class ShardMetadata:
    def __init__(self):
        self.document_shards = {}
        self.worker_channels = {}
        self.shard_workers = set()

    def get_shard_worker(self, doc_id):
        return self.document_shards.get(doc_id)

    def add_shard(self, doc_id, worker_address):
        self.document_shards[doc_id] = worker_address
        self.shard_workers.add(worker_address)
        if worker_address not in self.worker_channels:
            self.worker_channels[worker_address] = grpc.insecure_channel(worker_address)

    def get_all_shard_workers(self):
        return list(self.shard_workers)

class DatabaseManager:
    def __init__(self):
        self.databases = {}
        self.current_db = None
        self.replica_metadata = {}  # {db_name: {doc_id: [replica_workers]}}
        
    
    def add_replica(self, db_name, doc_id, worker_address):
        """Track which workers have replicas of which documents"""
        if db_name not in self.replica_metadata:
            self.replica_metadata[db_name] = {}
        if doc_id not in self.replica_metadata[db_name]:
            self.replica_metadata[db_name][doc_id] = []
        if worker_address not in self.replica_metadata[db_name][doc_id]:
            self.replica_metadata[db_name][doc_id].append(worker_address)
    
    def get_replicas(self, db_name, doc_id):
        """Get list of workers with replicas of this document"""
        return self.replica_metadata.get(db_name, {}).get(doc_id, [])
    
    def create_database(self, db_name):
        if db_name in self.databases:
            raise ValueError(f"Database '{db_name}' already exists")
        self.databases[db_name] = {
            'db': SimpleJSONDB(f"{db_name}.json"),
            'shards': ShardMetadata()
        }
        return self.databases[db_name]['db']
    
    def use_database(self, db_name):
        if db_name not in self.databases:
            if os.path.exists(f"{db_name}.json"):
                self.databases[db_name] = {
                    'db': SimpleJSONDB(f"{db_name}.json"),
                    'shards': ShardMetadata()
                }
            else:
                raise ValueError(f"Database '{db_name}' doesn't exist")
        self.current_db = self.databases[db_name]
        return self.current_db['db']

class DatabaseService(database_pb2_grpc.DatabaseServiceServicer):
    def __init__(self, worker_address):
        self.manager = DatabaseManager()
        self.worker_address = worker_address
        self.known_workers = [worker_address]
        self.master_channel = grpc.insecure_channel("localhost:50050")
        self.master_stub = database_pb2_grpc.DatabaseServiceStub(self.master_channel)
        self._discover_workers()
        self.replica_channels = {}  # Cache for replica channels
        # Heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def _check_master_connection(self):
        for _ in range(3):  # Retry 3 times
            try:
                self.master_stub.ListWorkers(database_pb2.Empty(), timeout=2)
                return True
            except:
                time.sleep(1)
        return False

    def _discover_workers(self):
        for _ in range(5):  # Retry 5 times
            try:
                response = self.master_stub.ListWorkers(database_pb2.Empty())
                self.known_workers = list(response.workers)
                logger.info(f"Discovered workers: {self.known_workers}")
                break
            except Exception as e:
                logger.warning(f"Could not discover workers from master: {str(e)}")
                time.sleep(1)  # Wait before retrying
        else:
            self.known_workers = ["localhost:50051", "localhost:50052"]
            logger.info(f"Fallback to default workers: {self.known_workers}")

    def _send_heartbeats(self):
        """Thread to send periodic heartbeat messages to master"""
        while True:
            try:
                response = self.master_stub.Heartbeat(
                    database_pb2.HeartbeatRequest(
                        worker_address=self.worker_address,
                        timestamp=int(time.time())
                    )
                )
                if response.acknowledged:
                    logger.info(f"Heartbeat sent to master from {self.worker_address}")
                else:
                    logger.warning(f"Heartbeat not acknowledged by master from {self.worker_address}")
            except Exception as e:
                logger.error(f"Failed to send heartbeat from {self.worker_address}: {str(e)}")
            time.sleep(5)  # Send heartbeat every 5 seconds
            

    def GetLoadInfo(self, request, context):
        """Return information about this worker's load"""
        replica_count = sum(len(docs) for docs in self.manager.replica_metadata.values())
        return database_pb2.LoadInfo(replica_count=replica_count)
    
    def DecrementReplicaCount(self, request, context):
        """Decrement the reported replica count (used when replication fails)"""
        # This worker doesn't actually track its own count in master,
        # but we implement it for protocol completeness
        return database_pb2.OperationResponse(success=True)

    def CreateDatabase(self, request, context):
        try:
            self.manager.create_database(request.name)
            return database_pb2.OperationResponse(success=True, message=f"Database '{request.name}' created")
        except Exception as e:
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def UseDatabase(self, request, context):
        try:
            self.manager.use_database(request.name)
            return database_pb2.OperationResponse(success=True, message=f"Using database '{request.name}'")
        except Exception as e:
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def ListDatabases(self, request, context):
        dbs = list(self.manager.databases.keys())
        return database_pb2.DatabaseList(names=dbs)
    
    def DeleteDatabase(self, request, context):
        try:
            if request.name in self.manager.databases:
                if os.path.exists(f"{request.name}.json"):
                    os.remove(f"{request.name}.json")
                del self.manager.databases[request.name]
                if self.manager.current_db and self.manager.current_db['db'].db_file == f"{request.name}.json":
                    self.manager.current_db = None
                return database_pb2.OperationResponse(success=True, message=f"Database '{request.name}' deleted")
            return database_pb2.OperationResponse(success=False, message=f"Database '{request.name}' not found")
        except Exception as e:
            return database_pb2.OperationResponse(success=False, message=str(e))
    
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
                '_primary': True,
                '_version': 1  # Add version counter
            }
            
            # Store document locally
            self.manager.current_db['db'].create_document(doc_data, doc_id)
            
            # Get replica workers from master (load-balanced)
            replicas = self.master_stub.GetDocumentReplicas(
                database_pb2.DocumentID(db_name=request.db_name, doc_id=doc_id))
            
            # Replicate to each replica worker
            successful_replicas = []
            for replica_addr in replicas.workers:
                try:
                    if replica_addr not in self.replica_channels:
                        self.replica_channels[replica_addr] = grpc.insecure_channel(replica_addr)
                    
                    stub = database_pb2_grpc.DatabaseServiceStub(self.replica_channels[replica_addr])
                    response = stub.ReplicateDocument(
                        database_pb2.ReplicateRequest(
                            db_name=request.db_name,
                            doc_id=doc_id,
                            document=json.dumps(doc_data)
                    ))
                    if response.success:
                        self.manager.add_replica(request.db_name, doc_id, replica_addr)
                        successful_replicas.append(replica_addr)
                    else:
                        # If replication fails, inform master to decrement count
                        self.master_stub.decrement_replica_count(database_pb2.Worker(worker=replica_addr))
                except Exception as e:
                    logger.error(f"Failed to replicate to {replica_addr}: {str(e)}")
                    self.master_stub.decrement_replica_count(database_pb2.Worker(worker=replica_addr))
            
            # If we couldn't create enough replicas, try to compensate
            if len(successful_replicas) < min(3, len(replicas.workers)):
                self._compensate_replicas(request.db_name, doc_id, doc_data, successful_replicas)
            
            return database_pb2.DocumentID(doc_id=doc_id)
            
        except Exception as e:
            logger.error(f"Error creating document: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentID()
    
    def _compensate_replicas(self, db_name, doc_id, doc_data, existing_replicas):
        """Try to create additional replicas if initial replication failed"""
        try:
            # Get current list of all workers
            workers = self.master_stub.ListWorkers(database_pb2.Empty()).workers
            available_workers = [w for w in workers 
                              if w != self.worker_address 
                              and w not in existing_replicas]
            
            # Sort by load (we need to get load info from master)
            load_info = {}
            for w in available_workers:
                try:
                    channel = grpc.insecure_channel(w)
                    stub = database_pb2_grpc.DatabaseServiceStub(channel)
                    response = stub.GetLoadInfo(database_pb2.Empty())
                    load_info[w] = response.replica_count
                except:
                    continue
            
            # Sort workers by load
            sorted_workers = sorted(load_info.keys(), key=lambda w: load_info[w])
            
            # Try to create additional replicas on least loaded workers
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
        """Handle document replication including primary status changes"""
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            doc_data = json.loads(request.document)
            doc_id = request.doc_id
            
            # If document exists, update it; otherwise, create it
            if doc_id in self.manager.current_db['db'].data:
                current_doc = self.manager.current_db['db'].data[doc_id]
                current_doc.update(doc_data)
                # Preserve primary status if not being explicitly set
                if '_primary' in doc_data:
                    current_doc['_primary'] = doc_data['_primary']
                current_doc['_updated_at'] = datetime.now().isoformat()
            else:
                # New document - set primary status from request
                doc_data['_created_at'] = datetime.now().isoformat()
                doc_data['_updated_at'] = datetime.now().isoformat()
                self.manager.current_db['db'].create_document(doc_data, doc_id)
            
            return database_pb2.OperationResponse(success=True)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))
        
    def _replicate_document(self, db_name, doc_id, document_json):
        """Helper to replicate document to all replicas"""
        try:
            replicas = self.master_stub.GetDocumentReplicas(
                database_pb2.DocumentID(db_name=db_name, doc_id=doc_id)
            ).workers
            
            for replica_addr in replicas:
                if replica_addr != self.worker_address:  # Skip self
                    try:
                        if replica_addr not in self.replica_channels:
                            self.replica_channels[replica_addr] = grpc.insecure_channel(replica_addr)
                        
                        stub = database_pb2_grpc.DatabaseServiceStub(self.replica_channels[replica_addr])
                        response = stub.ReplicateDocument(
                            database_pb2.ReplicateRequest(
                                db_name=db_name,
                                doc_id=doc_id,
                                document=document_json
                            ),
                            timeout=3
                        )
                        logger.info(f"Replicated to {replica_addr}: {response.message}")
                    except Exception as e:
                        logger.error(f"Failed to replicate to {replica_addr}: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to get replicas from master: {str(e)}")



    def ReadDocument(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            # Check if we have the document locally
            doc = self.manager.current_db['db'].read_document(request.doc_id)
            if doc:
                return database_pb2.DocumentResponse(document=json.dumps(doc))
            
            # If not found locally, check with master for location
            worker_response = self.master_stub.GetDocumentLocation(
                database_pb2.DocumentID(
                    db_name=request.db_name,
                    doc_id=request.doc_id
                ))
            
            if not worker_response.worker:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.DocumentResponse()
            
            # Forward to the correct worker
            if worker_response.worker not in self.replica_channels:
                self.replica_channels[worker_response.worker] = grpc.insecure_channel(worker_response.worker)
            
            stub = database_pb2_grpc.DatabaseServiceStub(self.replica_channels[worker_response.worker])
            return stub.ReadDocument(request)
            
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentResponse()
        
    def _select_shard_worker(self, doc_id):
        if not self.known_workers:
            return self.worker_address
        
        worker_hashes = [(int(hashlib.md5(worker.encode()).hexdigest(), 16), worker) for worker in self.known_workers]
        worker_hashes.sort()
        doc_hash = int(hashlib.md5(doc_id.encode()).hexdigest(), 16)
        for hash_val, worker in worker_hashes:
            if doc_hash <= hash_val:
                return worker
        return worker_hashes[0][1]

   
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
            
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            # Check if we're the primary worker for this document
            current_doc = self.manager.current_db['db'].read_document(request.doc_id)
            if not current_doc:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(success=False, message="Document not found")
            
            # If we're not primary, forward to primary
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
                return stub.UpdateDocument(request)
            
            # We are the primary - perform the update
            updates = json.loads(request.updates)
            
            # Apply updates locally
            current_doc.update(updates)
            current_doc['_updated_at'] = datetime.now().isoformat()
            current_doc['_version'] = current_doc.get('_version', 0) + 1
            self.manager.current_db['db']._save()
            
            # Get all replica workers (excluding self)
            replicas = self.master_stub.GetDocumentReplicas(
                database_pb2.DocumentID(db_name=request.db_name, doc_id=request.doc_id)
            ).workers
            replica_addresses = [addr for addr in replicas if addr != self.worker_address]
            
            # Prepare the replication request
            replicate_request = database_pb2.ReplicateRequest(
                db_name=request.db_name,
                doc_id=request.doc_id,
                document=json.dumps(current_doc),
                is_update=True
            )
            
            # Replicate to all secondaries (async for better performance)
            successful_replicas = 0
            replication_futures = []
            
            with futures.ThreadPoolExecutor(max_workers=5) as executor:
                for replica_addr in replica_addresses:
                    try:
                        channel = self.replica_channels.get(replica_addr) or grpc.insecure_channel(replica_addr)
                        stub = database_pb2_grpc.DatabaseServiceStub(channel)
                        
                        # Submit replication asynchronously
                        future = executor.submit(
                            stub.ReplicateDocument,
                            replicate_request,
                            timeout=2
                        )
                        replication_futures.append((replica_addr, future))
                    except Exception as e:
                        logger.error(f"Failed to initiate replication to {replica_addr}: {str(e)}")
                
                # Wait for all replications to complete
                for replica_addr, future in replication_futures:
                    try:
                        response = future.result()
                        if response.success:
                            successful_replicas += 1
                    except Exception as e:
                        logger.error(f"Replication failed to {replica_addr}: {str(e)}")
            
            # If too many replicas failed, consider this a partial failure
            if successful_replicas < len(replica_addresses) // 2:  # Less than half succeeded
                logger.warning(f"Update partially failed - only {successful_replicas}/{len(replica_addresses)} replicas updated")
                return database_pb2.OperationResponse(
                    success=False,
                    message=f"Primary updated but only {successful_replicas}/{len(replica_addresses)} replicas succeeded"
                )
            
            return database_pb2.OperationResponse(
                success=True,
                message=f"Updated primary and {successful_replicas}/{len(replica_addresses)} replicas"
            )
            
        except Exception as e:
            logger.error(f"Update failed: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))
        

    def DeleteDocument(self, request, context):
        try:
            logger.info(f"DeleteDocument: db={request.db_name}, doc_id={request.doc_id}")

            # Load the database if not already loaded
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)

            # Check if the document exists locally
            current_doc = self.manager.current_db['db'].read_document(request.doc_id)
            if not current_doc:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(success=False, message="Document not found")

            # If not primary, forward request to primary
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

            # We are the primary — delete the document
            success = self.manager.current_db['db'].delete_document(request.doc_id)

            # Also delete shard metadata
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
            return database_pb2.OperationResponse(success=False, message=str(e))


    def ClearDatabase(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.name}.json":
                self.manager.use_database(request.name)
            self.manager.current_db['db'].clear_database()
            self.manager.current_db['shards'] = ShardMetadata()
            return database_pb2.OperationResponse(success=True, message="Database cleared")
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))
    
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

def serve(port):
    worker_address = f"localhost:{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        DatabaseService(worker_address), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
     # Register with master
    try:
        master_channel = grpc.insecure_channel("localhost:50050")
        master_stub = database_pb2_grpc.DatabaseServiceStub(master_channel)
        response = master_stub.RegisterWorker(database_pb2.Worker(worker=worker_address))
        if response.success:
            print(f"Worker node running on port {port} and registered with master")
        else:
            print(f"Worker node running on port {port} but registration failed: {response.message}")
    except Exception as e:
        print(f"Worker node running on port {port} but failed to register with master: {str(e)}")
    
    server.wait_for_termination()
    

if __name__ == '__main__':
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    serve(port)
