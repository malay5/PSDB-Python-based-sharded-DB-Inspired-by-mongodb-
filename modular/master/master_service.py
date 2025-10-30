from datetime import datetime
import random
import uuid
import grpc
import json
import hashlib
from concurrent import futures
import threading
import time
import database_pb2
import database_pb2_grpc
import logging
from typing import Dict, List, Optional, Tuple
from enum import Enum
import os
import redis
import uuid
import time
from .distributed_lock import DistributedLock
from .raft_node import RaftRole
from .logger_util import logger, LogEntry
from .worker_node import WorkerNode
from .raft_node import RaftNode
from .master_node import MasterNode


class MasterService(database_pb2_grpc.MasterServiceServicer):
    def __init__(self, master_node: MasterNode):
        self.master = master_node
        self.db_list_file = "databases.json"
        self._initialize_db_file()

    def _initialize_db_file(self):
        """Initialize databases.json if it doesn't exist."""
        with self.master.lock:
            if not os.path.exists(self.db_list_file):
                with open(self.db_list_file, 'w') as f:
                    json.dump({"databases": []}, f)
                logger.info(f"Initialized {self.db_list_file}")

    def _read_db_file(self) -> Dict:
        """Read databases.json with thread safety."""
        with self.master.lock:
            try:
                with open(self.db_list_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error reading {self.db_list_file}: {str(e)}")
                return {"databases": []}

    def _write_db_file(self, data: Dict):
        """Write to databases.json with thread safety."""
        with self.master.lock:
            try:
                with open(self.db_list_file, 'w') as f:
                    json.dump(data, f, indent=2)
            except Exception as e:
                logger.error(f"Error writing {self.db_list_file}: {str(e)}")
                raise

    def InstallSnapshot(self, request, context):
        """Handle snapshot installation from leader."""
        if request.term < self.master.raft_node.state.current_term:
            return database_pb2.InstallSnapshotResponse(
                term=self.master.raft_node.state.current_term,
                success=False
            )
        
        if request.term > self.master.raft_node.state.current_term:
            self.master.raft_node.state.current_term = request.term
            self.master.raft_node._step_down()
        
        try:
            snapshot_data = json.loads(request.data)
            with self.master.lock:
                # Rebuild workers
                self.master.workers = {
                    addr: WorkerNode(addr) for addr in snapshot_data['state']['workers']
                }
                for addr, stats in snapshot_data['state']['workers'].items():
                    if addr in self.master.workers:
                        self.master.workers[addr].load = stats['load']
                        self.master.workers[addr].replica_count = stats['replica_count']
                
                # Restore document shards
                self.master.document_shards = snapshot_data['state']['document_shards']
                
                # Restore database assignments to JSON file
                db_list = {
                    "databases": [
                        {
                            "name": db_name,
                            "primary_worker": worker_addr,
                            "indexes": [],
                            "created_at": datetime.now().isoformat()
                        }
                        for db_name, worker_addr in snapshot_data['state']['database_assignments'].items()
                    ]
                }
                self._write_db_file(db_list)
            
            self.master.raft_node.state.last_applied = request.last_included_index
            self.master.raft_node.state.commit_index = request.last_included_index
            
            snapshot_file = f"snapshot_{self.master.raft_node.node_id}.json"
            with open(snapshot_file, 'w') as f:
                json.dump(snapshot_data, f)
            
            return database_pb2.InstallSnapshotResponse(
                term=self.master.raft_node.state.current_term,
                success=True
            )
        except Exception as e:
            logger.error(f"Failed to install snapshot: {str(e)}")
            return database_pb2.InstallSnapshotResponse(
                term=self.master.raft_node.state.current_term,
                success=False
            )

    def GetLeader(self, request, context):
        leader_id = self.master.raft_node.state.leader_id
        leader_addr = self.master.raft_node.get_leader_addr()
        return database_pb2.LeaderInfo(
            leader_id=leader_id or "",
            leader_address=leader_addr or ""
        )

    def is_leader(self):
        return self.master.raft_node.state.role == RaftRole.Leader

    def DecrementReplicaCount(self, request, context):
        addr = request.worker_addr
        if addr in self.master.workers:
            self.master.workers[addr].replica_count -= 1
            logger.info(f"Decremented replica count for {addr}")
        else:
            logger.warning(f"Attempted to decrement replica count for unknown worker: {addr}")
        return database_pb2.DecrementResponse(success=True)
    
    def Heartbeat(self, request, context):
        try:
            if not self.is_leader():
                logger.info(f"Rejecting heartbeat from {request.worker_address} - not leader")
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return database_pb2.HeartbeatResponse(acknowledged=False)
            
            logger.info(f"Received heartbeat from worker {request.worker_address}")
            worker_address = request.worker_address
            if worker_address not in self.master.workers:
                logger.info(f"Adding new worker {worker_address} from heartbeat")
                self.master.workers[worker_address] = WorkerNode(worker_address)
                if self.is_leader():
                    self.master._replicate_operation("add_worker", worker_address)
            
            worker = self.master.workers[worker_address]
            worker.last_heartbeat = time.time()
            worker.health = True
            worker.failed_checks = 0
        
            return database_pb2.HeartbeatResponse(acknowledged=True)
        except Exception as e:
            logger.error(f"Failed to process heartbeat: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Server error: {str(e)}")
            raise

    def RequestVote(self, request, context):
        logger.info(f"Node {self.master.raft_node.node_id} received vote request from {request.candidate_id} for term {request.term}")
        response = database_pb2.VoteResponse(
            term=self.master.raft_node.state.current_term,
            vote_granted=False
        )

        if request.term > self.master.raft_node.state.current_term:
            logger.info(f"Node {self.master.raft_node.node_id} updating term to {request.term} and becoming follower")
            self.master.raft_node.state.current_term = request.term
            self.master.raft_node.state.role = RaftRole.Follower
            self.master.raft_node.state.voted_for = None
            self.master.raft_node.state.last_heartbeat = time.time()

        if request.term == self.master.raft_node.state.current_term:
            if self.master.raft_node.state.voted_for is None or self.master.raft_node.state.voted_for == request.candidate_id:
                if self.master.raft_node.state.role == RaftRole.Candidate and request.candidate_id > self.master.raft_node.node_id:
                    logger.info(f"Node {self.master.raft_node.node_id} stepping down to follower to favor candidate {request.candidate_id}")
                    self.master.raft_node.state.role = RaftRole.Follower
                    self.master.raft_node.state.voted_for = request.candidate_id
                    self.master.raft_node.state.last_heartbeat = time.time()
                    response.vote_granted = True
                    logger.info(f"Node {self.master.raft_node.node_id} granted vote to {request.candidate_id} for term {request.term}")
                elif self.master.raft_node.state.role != RaftRole.Candidate:
                    self.master.raft_node.state.voted_for = request.candidate_id
                    self.master.raft_node.state.last_heartbeat = time.time()
                    response.vote_granted = True
                    logger.info(f"Node {self.master.raft_node.node_id} granted vote to {request.candidate_id} for term {request.term}")
            else:
                logger.info(f"Node {self.master.raft_node.node_id} denied vote to {request.candidate_id}: already voted for {self.master.raft_node.state.voted_for}")
        elif request.term < self.master.raft_node.state.current_term:
            logger.info(f"Node {self.master.raft_node.node_id} denied vote to {request.candidate_id}: lower term {request.term} < {self.master.raft_node.state.current_term}")

        response.term = self.master.raft_node.state.current_term
        return response
    
    def AppendEntries(self, request, context):
        entries = [
            LogEntry(term=e.term, index=e.index, data=e.data)
            for e in request.entries
        ]
        
        if request.term < self.master.raft_node.state.current_term:
            return database_pb2.AppendEntriesResponse(
                term=self.master.raft_node.state.current_term,
                success=False
            )
        
        if request.term > self.master.raft_node.state.current_term:
            self.master.raft_node.state.current_term = request.term
            self.master.raft_node.state.role = RaftRole.Follower
            self.master.raft_node.state.voted_for = None
            self.master.raft_node._step_down()
        
        self.master.raft_node.state.leader_id = request.leader_id
        self.master.raft_node.state.last_heartbeat = time.time()
        if self.master.raft_node.state.role != RaftRole.Follower:
            self.master.raft_node.state.role = RaftRole.Follower
            logger.info(f"Node {self.master.raft_node.node_id} became follower for leader {request.leader_id}")
            if self.master._on_state_change:
                self.master._on_state_change(self.master.state)
        
        return database_pb2.AppendEntriesResponse(
            term=self.master.raft_node.state.current_term,
            success=True
        )

    def AddWorker(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        success = self.master.add_worker(request.worker)
        return database_pb2.OperationResponse(
            success=success,
            message="Worker registered successfully" if success else "Worker registration failed"
        )

    def RemoveWorker(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        
        success = self.master.remove_worker(request.worker)
        return database_pb2.OperationResponse(
            success=success,
            message="Worker removed successfully" if success else "Worker removal failed"
        )

    def ListWorkers(self, request, context):
        with self.master.lock:
            workers = list(self.master.workers.keys())
        return database_pb2.WorkerList(workers=workers)

    def AssignDatabase(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        
        db_list = self._read_db_file()
        for db in db_list["databases"]:
            if db["name"] == request.db_name:
                return database_pb2.OperationResponse(
                    success=False,
                    message=f"Database '{request.db_name}' already exists"
                )
        
        if request.worker_address not in self.master.workers:
            return database_pb2.OperationResponse(
                success=False,
                message=f"Worker '{request.worker_address}' not found"
            )
        
        operation_data = {
            "op": "assign_database",
            "args": [request.db_name, request.worker_address],
            "timestamp": time.time()
        }
        future = self.master.raft_node.propose(json.dumps(operation_data).encode('utf-8'))
        
        if future.result(timeout=5):
            db_list["databases"].append({
                "name": request.db_name,
                "primary_worker": request.worker_address,
                "indexes": [],
                "created_at": datetime.now().isoformat()
            })
            self._write_db_file(db_list)
            self.master.workers[request.worker_address].load += 1
            logger.info(f"Database {request.db_name} assigned to {request.worker_address}")
            return database_pb2.OperationResponse(
                success=True,
                message="Database assigned successfully"
            )
        else:
            logger.warning("Raft replication failed for database assignment")
            return database_pb2.OperationResponse(
                success=False,
                message="Failed to assign database due to replication error"
            )

    def UnassignDatabase(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        
        db_list = self._read_db_file()
        new_db_list = [db for db in db_list["databases"] if db["name"] != request.name]
        
        if len(new_db_list) == len(db_list["databases"]):
            return database_pb2.OperationResponse(
                success=False,
                message=f"Database '{request.name}' not found"
            )
        
        operation_data = {
            "op": "unassign_database",
            "args": [request.name],
            "timestamp": time.time()
        }
        future = self.master.raft_node.propose(json.dumps(operation_data).encode('utf-8'))
        
        if future.result(timeout=5):
            db_list["databases"] = new_db_list
            self._write_db_file(db_list)
            logger.info(f"Database {request.name} unassigned")
            return database_pb2.OperationResponse(
                success=True,
                message="Database unassigned successfully"
            )
        else:
            logger.warning("Raft replication failed for database unassignment")
            return database_pb2.OperationResponse(
                success=False,
                message="Failed to unassign database due to replication error"
            )

    def AssignDocument(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        
        success = self.master.assign_document(request.db_name, request.doc_id, request.worker_address)
        return database_pb2.OperationResponse(
            success=success,
            message="Document assigned successfully" if success else "Document assignment failed"
        )

    def GetDocumentReplicas(self, request, context):
        try:
            replicas = self.master.get_document_replicas(request.db_name, request.doc_id)
            return database_pb2.WorkerList(workers=replicas)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.WorkerList()

    def GetDocumentPrimary(self, request, context):
        worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
        if not worker_addr:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        
        try:
            channel = grpc.insecure_channel(worker_addr)
            stub = database_pb2_grpc.MasterServiceStub(channel)
            doc_response = stub.ReadDocument(request)
            if doc_response.document:
                doc = json.loads(doc_response.document)
                if doc.get('_primary', False):
                    return database_pb2.Worker(worker=worker_addr)
        except:
            pass
        
        return self.SetDocumentPrimary(
            database_pb2.SetPrimaryRequest(
                db_name=request.db_name,
                doc_id=request.doc_id,
                worker_address=worker_addr
            ),
            context
        )

    def SetDocumentPrimary(self, request, context):
        try:
            channel = grpc.insecure_channel(request.worker_address)
            stub = database_pb2_grpc.MasterServiceStub(channel)
            
            doc_response = stub.ReadDocument(database_pb2.DocumentID(
                db_name=request.db_name,
                doc_id=request.doc_id
            ))
            
            if not doc_response.document:
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found on target worker"
                )
            
            doc = json.loads(doc_response.document)
            doc['_primary'] = True
            doc['_updated_at'] = datetime.now().isoformat()
            
            stub.ReplicateDocument(database_pb2.ReplicateRequest(
                db_name=request.db_name,
                doc_id=request.doc_id,
                document=json.dumps(doc)
            ))
            
            replicas = self.GetDocumentReplicas(database_pb2.DocumentID(
                db_name=request.db_name,
                doc_id=request.doc_id
            )).workers
            
            for replica in replicas:
                if replica != request.worker_address:
                    try:
                        channel = grpc.insecure_channel(replica)
                        stub = database_pb2_grpc.MasterServiceStub(channel)
                        doc['_primary'] = False
                        stub.ReplicateDocument(database_pb2.ReplicateRequest(
                            db_name=request.db_name,
                            doc_id=request.doc_id,
                            document=json.dumps(doc)
                        ))
                    except:
                        continue
            
            return database_pb2.OperationResponse(
                success=True,
                message=f"Worker {request.worker_address} set as primary for document {request.doc_id}"
            )
        except Exception as e:
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )

    def ReplicateDocument(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db.db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            doc_data = json.loads(request.document)
            doc_id = request.doc_id
            
            existing_doc = self.manager.current_db.read_document(doc_id)
            
            if existing_doc:
                existing_doc.update(doc_data)
                existing_doc['_updated_at'] = datetime.now().isoformat()
                if '_version' in doc_data:
                    existing_doc['_version'] = doc_data['_version']
                self.manager.current_db._save()
            else:
                doc_data['_created_at'] = datetime.now().isoformat()
                doc_data['_updated_at'] = datetime.now().isoformat()
                self.manager.current_db.create_document(doc_data, doc_id)
            
            return database_pb2.OperationResponse(success=True)
        except Exception as e:
            logger.error(f"Replication failed: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )

    def CreateDatabase(self, request, context):
        if not self.master.is_leader():
            leader_addr = self.master.raft_node.get_leader_addr()
            if leader_addr:
                try:
                    channel = grpc.insecure_channel(leader_addr)
                    stub = database_pb2_grpc.MasterServiceStub(channel)
                    return stub.CreateDatabase(request)
                except Exception as e:
                    logger.error(f"Failed to forward to leader: {str(e)}")
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    return database_pb2.OperationResponse(
                        success=False,
                        message=f"Failed to forward to leader: {str(e)}"
                    )
            return database_pb2.OperationResponse(
                success=False,
                message="No leader available"
            )

        logger.info(f"Creating database {request.name}")
        
        db_list = self._read_db_file()
        for db in db_list["databases"]:
            if db["name"] == request.name:
                return database_pb2.OperationResponse(
                    success=False,
                    message=f"Database '{request.name}' already exists"
                )
        
        worker = None
        start_time = time.time()
        while time.time() - start_time < 10:
            worker = self.master._select_worker_for_db(request.name)
            if worker:
                break
            time.sleep(0.5)
        
        if not worker:
            return database_pb2.OperationResponse(
                success=False,
                message="No available workers"
            )

        max_retries = 3
        for attempt in range(max_retries):
            try:
                worker_response = worker.stub.CreateDatabase(request, timeout=5)
                logger.info(f"Creating database {request.name} with indexes: {list(request.indexes)}")
                
                if not worker_response.success:
                    continue

                operation_data = {
                    'op': 'create_database',
                    'args': [request.name, worker.address, list(request.indexes)],
                    'timestamp': time.time()
                }
                
                future = self.master.raft_node.propose(
                    json.dumps(operation_data).encode('utf-8'))
                
                if future.result(timeout=5):
                    logger.info("Raft commit succeeded for database creation")
                    db_list["databases"].append({
                        "name": request.name,
                        "primary_worker": worker.address,
                        "indexes": list(request.indexes),
                        "created_at": datetime.now().isoformat()
                    })
                    self._write_db_file(db_list)
                    self.master.workers[worker.address].load += 1
                    logger.info(f"Database {request.name} created and assigned to {worker.address}")
                    return database_pb2.OperationResponse(
                        success=True,
                        message=f"Database '{request.name}' created"
                    )
                else:
                    logger.warning("Raft replication failed, rolling back")
                    worker.stub.DeleteDatabase(
                        database_pb2.DatabaseName(name=request.name),
                        timeout=2
                    )
                    continue

            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed: {str(e)}")
                time.sleep(0.5 * (attempt + 1))

        return database_pb2.OperationResponse(
            success=False,
            message=f"Failed to create database after {max_retries} attempts"
        )

    def GetPrimaryWorker(self, request, context):
        db_list = self._read_db_file()
        for db in db_list["databases"]:
            if db["name"] == request.name:
                print(f"workers list : " ,self.master.workers)
                if db["primary_worker"] in self.master.workers:
                    return database_pb2.Worker(worker=db["primary_worker"])
                else:
                    logger.warning(f"Primary worker {db['primary_worker']} for {request.name} not found")
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return database_pb2.Worker()
        context.set_code(grpc.StatusCode.NOT_FOUND)
        return database_pb2.Worker()

    def UseDatabase(self, request, context):
        try:
            db_list = self._read_db_file()
            for db in db_list["databases"]:
                if db["name"] == request.name:
                    worker_addr = db["primary_worker"]
                    worker = self.master.workers.get(worker_addr)
                    if worker:
                        return worker.stub.UseDatabase(request)
            
            worker = None
            for w in self.master.workers.values():
                try:
                    response = w.stub.ListDatabases(database_pb2.Empty(), timeout=2)
                    if request.name in response.names:
                        worker = w
                        break
                except:
                    continue
            
            if worker:
                operation_data = {
                    "op": "assign_database",
                    "args": [request.name, worker.address],
                    "timestamp": time.time()
                }
                future = self.master.raft_node.propose(json.dumps(operation_data).encode('utf-8'))
                
                if future.result(timeout=5):
                    db_list["databases"].append({
                        "name": request.name,
                        "primary_worker": worker.address,
                        "indexes": [],
                        "created_at": datetime.now().isoformat()
                    })
                    self._write_db_file(db_list)
                    self.master.workers[worker.address].load += 1
                    return worker.stub.UseDatabase(request)
                else:
                    return database_pb2.OperationResponse(
                        success=False,
                        message="Failed to assign database due to replication error"
                    )
            
            return database_pb2.OperationResponse(
                success=False,
                message=f"Database '{request.name}' doesn't exist"
            )
            
        except Exception as e:
            logger.error(f"Error using database: {str(e)}")
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )

    def ListDatabases(self, request, context):
        try:
            db_list = self._read_db_file()
            db_names = [db["name"] for db in db_list["databases"]]
            logger.info(f"List of databases from file: {db_names}")
            return database_pb2.DatabaseList(names=db_names)
        except Exception as e:
            logger.error(f"Error reading database list: {str(e)}")
            return database_pb2.DatabaseList(names=[])

    def DeleteDatabase(self, request, context):
        db_list = self._read_db_file()
        target_db = None
        for db in db_list["databases"]:
            if db["name"] == request.name:
                target_db = db
                break
        
        if not target_db:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        
        worker_addr = target_db["primary_worker"]
        worker = self.master.workers.get(worker_addr)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Primary worker not found")
        
        try:
            response = worker.stub.DeleteDatabase(request)
            if response.success:
                operation_data = {
                    "op": "unassign_database",
                    "args": [request.name],
                    "timestamp": time.time()
                }
                future = self.master.raft_node.propose(json.dumps(operation_data).encode('utf-8'))
                
                if future.result(timeout=5):
                    db_list["databases"] = [db for db in db_list["databases"] if db["name"] != request.name]
                    self._write_db_file(db_list)
                    logger.info(f"Database {request.name} deleted")
                    return response
                else:
                    logger.warning("Raft replication failed for database deletion")
                    return database_pb2.OperationResponse(
                        success=False,
                        message="Failed to delete database due to replication error"
                    )
            return response
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))

    def CreateDocument(self, request, context):
        worker = self.GetPrimaryWorker(database_pb2.DatabaseName(name=request.db_name), context)
        if not worker.worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentID()
        try:
            worker_node = self.master.workers.get(worker.worker)
            if not worker_node:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.DocumentID()
            response = worker_node.stub.CreateDocument(request)
            if response.doc_id:
                replicas = self.master.get_document_replicas(request.db_name, response.doc_id)
                for replica_addr in replicas:
                    try:
                        replica_worker = self.master.workers[replica_addr]
                        replica_worker.stub.ReplicateDocument(
                            database_pb2.ReplicateRequest(
                                db_name=request.db_name,
                                doc_id=response.doc_id,
                                document=request.document
                            ),
                            timeout=3
                        )
                    except Exception as e:
                        logger.error(f"Failed to replicate to {replica_addr}: {str(e)}")
                        self.master.decrement_replica_count(replica_addr)
            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(e.details())
            return database_pb2.DocumentID()
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentID()

    def GetDocumentLocation(self, request, context):
        worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
        if not worker_addr:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        return database_pb2.Worker(worker=worker_addr)

    def ReadDocument(self, request, context):
        worker = self.GetPrimaryWorker(database_pb2.DatabaseName(name=request.db_name), context)
        if not worker.worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentResponse()
        try:
            worker_node = self.master.workers.get(worker.worker)
            if not worker_node:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.DocumentResponse()
            return worker_node.stub.ReadDocument(request)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.DocumentResponse()

    def ReadAllDocuments(self, request, context):
        worker = self.GetPrimaryWorker(database_pb2.DatabaseName(name=request.name), context)
        if not worker.worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentList()
        try:
            worker_node = self.master.workers.get(worker.worker)
            if not worker_node:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.DocumentList()
            shard_locations = worker_node.stub.GetShardLocations(
                database_pb2.DatabaseName(name=request.name)
            )
            all_documents = []
            for shard_worker_addr in shard_locations.workers:
                shard_worker = self.master.workers.get(shard_worker_addr)
                if shard_worker:
                    response = shard_worker.stub.ReadAllDocuments(request)
                    all_documents.extend(response.documents)
            return database_pb2.DocumentList(documents=all_documents)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentList()

    def QueryDocuments(self, request, context):
        worker = self.GetPrimaryWorker(database_pb2.DatabaseName(name=request.db_name), context)
        if not worker.worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentList()
        try:
            worker_node = self.master.workers.get(worker.worker)
            if not worker_node:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.DocumentList()
            shard_locations = worker_node.stub.GetShardLocations(
                database_pb2.DatabaseName(name=request.db_name))
            matching_docs = []
            for shard_worker_addr in shard_locations.workers:
                shard_worker = self.master.workers.get(shard_worker_addr)
                if shard_worker:
                    response = shard_worker.stub.QueryDocuments(request)
                    matching_docs.extend(response.documents)
            return database_pb2.DocumentList(documents=matching_docs)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentList()

    def UpdateDocument(self, request, context):
        try:
            logger.info(f"UpdateDocument: db={request.db_name}, doc_id={request.doc_id}")
            
            worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
            if not worker_addr:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Document '{request.doc_id}' not found on worker")
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found"
                )
            
            try:
                channel = grpc.insecure_channel(worker_addr)
                stub = database_pb2_grpc.MasterServiceStub(channel)
                
                doc_response = stub.ReadDocument(database_pb2.DocumentID(
                    db_name=request.db_name,
                    doc_id=request.doc_id
                ))
                
                if not doc_response.document:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return database_pb2.OperationResponse(
                        success=False,
                        message="Document not found on worker"
                    )
                
                doc = json.loads(doc_response.document)
                if not doc.get('_primary', False):
                    primary_worker = self.GetDocumentPrimary(
                        database_pb2.DocumentID(
                            db_name=request.db_name,
                            doc_id=request.doc_id
                        ),
                        context
                    ).worker
                    
                    if primary_worker:
                        channel = grpc.insecure_channel(primary_worker)
                        stub = database_pb2_grpc.MasterServiceStub(channel)
                        return stub.UpdateDocument(request)
                    
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                    return database_pb2.OperationResponse(
                        success=False,
                        message="No primary worker found for document"
                    )
                
                updates = json.loads(request.updates)
                doc.update(updates)
                doc['_updated_at'] = datetime.now().isoformat()
                doc['_version'] = doc.get('_version', 0) + 1
                
                update_response = stub.UpdateDocument(
                    database_pb2.UpdateRequest(
                        db_name=request.db_name,
                        doc_id=request.doc_id,
                        updates=json.dumps(updates)
                ))
                
                if not update_response.success:
                    return update_response
                
                replicas = self.master.get_document_replicas(request.db_name, request.doc_id)
                successful_replicas = 0
                
                for replica_addr in replicas:
                    if replica_addr != worker_addr:
                        try:
                            channel = grpc.insecure_channel(replica_addr)
                            stub = database_pb2_grpc.MasterServiceStub(channel)
                            replicate_response = stub.ReplicateDocument(
                                database_pb2.ReplicateRequest(
                                    db_name=request.db_name,
                                    doc_id=request.doc_id,
                                    document=json.dumps(doc),
                                    timeout=2
                                ))
                            if replicate_response.success:
                                successful_replicas += 1
                        except Exception as e:
                            logger.error(f"Failed to replicate to {replica_addr}: {str(e)}")
                
                return database_pb2.OperationResponse(
                    success=True,
                    message=f"Updated primary and {successful_replicas}/{len(replicas)} replicas"
                )
                
            except Exception as e:
                logger.error(f"Update failed: {str(e)}")
                context.set_code(grpc.StatusCode.INTERNAL)
                return database_pb2.OperationResponse(
                    success=False,
                    message=str(e)
                )
                
        except Exception as e:
            logger.error(f"Error in UpdateDocument: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )



    def UpdateLogDocument(self, request, context):
        try:
            print(f"update log document")
            logger.info(f"UpdateDocument: db={request.db_name}, doc_id={request.doc_id}")
            
            # Get the worker address where the document resides
            worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
            if not worker_addr:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Document '{request.doc_id}' not found on worker")
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found"
                )
            
            try:
                channel = grpc.insecure_channel(worker_addr)
                stub = database_pb2_grpc.MasterServiceStub(channel)
                
                # Read the document from the worker
                doc_response = stub.ReadDocument(database_pb2.DocumentID(
                    db_name=request.db_name,
                    doc_id=request.doc_id
                ))
                
                if not doc_response.document:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return database_pb2.OperationResponse(
                        success=False,
                        message="Document not found on worker"
                    )
                
                # Apply the updates
                updates = json.loads(request.updates)
                updated_doc = json.loads(doc_response.document)
                updated_doc.update(updates)
                updated_doc['_updated_at'] = datetime.now().isoformat()
                updated_doc['_version'] = updated_doc.get('_version', 0) + 1
                
                # Send the update request back to the worker
                update_response = stub.UpdateLogDocument(
                    database_pb2.UpdateRequest(
                        db_name=request.db_name,
                        doc_id=request.doc_id,
                        updates=json.dumps(updates)
                    )
                )
                
                return update_response

            except Exception as e:
                logger.error(f"Update failed: {str(e)}")
                context.set_code(grpc.StatusCode.INTERNAL)
                return database_pb2.OperationResponse(
                    success=False,
                    message=str(e)
                )
                
        except Exception as e:
            logger.error(f"Error in UpdateDocument: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )




    def DeleteDocument(self, request, context):
        worker = self.GetPrimaryWorker(database_pb2.DatabaseName(name=request.db_name), context)
        if not worker.worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        try:
            worker_node = self.master.workers.get(worker.worker)
            if not worker_node:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(success=False, message="Worker not found")
            return worker_node.stub.DeleteDocument(request)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))

    def ClearDatabase(self, request, context):
        worker = self.GetPrimaryWorker(database_pb2.DatabaseName(name=request.name), context)
        if not worker.worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        try:
            worker_node = self.master.workers.get(worker.worker)
            if not worker_node:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(success=False, message="Worker not found")
            return worker_node.stub.ClearDatabase(request)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))
