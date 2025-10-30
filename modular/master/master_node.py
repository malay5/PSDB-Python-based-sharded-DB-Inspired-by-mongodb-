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
from .worker_node import WorkerNode
from .raft_node import RaftNode ,RaftState
from .logger_util import logger
from .raft_node import RaftRole

class MasterNode:
    def __init__(self, node_id: str, raft_addr: str, redis_client, peers: Dict[str, str], service_port: int):
        self.node_id = node_id
        self.service_port = service_port  # Store service port for get_leader_addr
        self.workers: Dict[str, WorkerNode] = {}
        self.database_assignments: Dict[str, str] = {}
        self.document_shards: Dict[str, Dict[str, str]] = {}
        self.lock = threading.Lock()
        # Persistent state for recovery
        self.state_file = f"raft_state_{node_id}.json"
        self._load_state()
        self.stop_flag = False

        # Add collection tracking
        self.collection_assignments: Dict[str, Dict[str, str]] = {}  # {db_name: {collection_name: worker_address}}
        self.document_shards: Dict[str, Dict[str, Dict[str, str]]] = {}  # {db_name: {collection_name: {doc_id: worker_address}}}
        
        self.raft_node = RaftNode(
            node_id=node_id,
            addr=raft_addr,
            peers=peers,
            redis_client=redis_client,
            on_leader_change=self._on_leader_change,
            on_state_change=self._on_state_change,
            on_apply=self._on_apply        )
        self.raft_node.set_master_node(self)  # Add this line
        self.raft_node.redis_client = redis_client  # Set redis_client after initialization
        self.raft_node.service_port = service_port  # Pass service port to RaftNode
        
        self.health_thread = None
        self.raft_thread = threading.Thread(target=self._run_raft_loop, daemon=True)
        self.raft_thread.start()
        self._start_health_check()  # New method to start health check


    def assign_collection(self, db_name: str, collection_name: str, worker_address: str) -> bool:
        """Assign a collection to a worker"""
        if db_name not in self.collection_assignments:
            self.collection_assignments[db_name] = {}
        self.collection_assignments[db_name][collection_name] = worker_address
        return True
        
    def get_collection_worker(self, db_name: str, collection_name: str) -> Optional[str]:
        """Get worker assigned to a collection"""
        print(f"list of workers : " ,self.collection_assignments.get(db_name, {}).get(collection_name))
        return self.collection_assignments.get(db_name, {}).get(collection_name)
    
    def get_document_worker(self, db_name: str, collection_name: str, doc_id: str) -> Optional[str]:
        """Get worker for a specific document"""
        if db_name not in self.document_shards:
            self.document_shards[db_name] = {}
        if collection_name not in self.document_shards[db_name]:
            self.document_shards[db_name][collection_name] = {}
            
        if doc_id not in self.document_shards[db_name][collection_name]:
            worker = self.get_collection_worker(db_name, collection_name)
            if worker:
                self.document_shards[db_name][collection_name][doc_id] = worker
                
        return self.document_shards[db_name][collection_name].get(doc_id)

    def _load_state(self):
        """Load persisted state from disk"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    
                    # Reconstruct workers
                    self.workers = {}
                    for addr, worker_data in state.get('workers', {}).items():
                        worker = WorkerNode(addr)
                        worker.health = worker_data.get('health', True)
                        worker.load = worker_data.get('load', 0)
                        worker.replica_count = worker_data.get('replica_count', 0)
                        worker.last_heartbeat = worker_data.get('last_heartbeat', time.time())
                        self.workers[addr] = worker
                    
                    self.database_assignments = state.get('database_assignments', {})
                    self.document_shards = state.get('document_shards', {})
            except Exception as e:
                logger.error(f"Failed to load state: {str(e)}")

    def _save_state(self):
        """Persist current state to disk"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump({
                    'workers': {
                        addr: {
                            'address': worker.address,
                            'health': worker.health,
                            'load': worker.load,
                            'replica_count': worker.replica_count,
                            'last_heartbeat': worker.last_heartbeat
                        }
                        for addr, worker in self.workers.items()
                    },
                    'database_assignments': self.database_assignments,
                    'document_shards': self.document_shards
                }, f, indent=2)  # Added indent for better readability
        except Exception as e:
            logger.error(f"Failed to save state: {str(e)}")


    def _run_raft_loop(self):
        self.raft_node.run()

    def _on_leader_change(self, new_leader_id: Optional[str]):
        if new_leader_id:
            logger.info(f"New leader elected: {new_leader_id}")
            if new_leader_id == self.node_id:
                # We became leader - start health checks
                self._start_health_check()
                # Immediately verify all workers
                self._verify_all_workers()
            else:
                # We're no longer leader - stop health checks
                self.stop_health_check()

    def _start_health_check(self):
        """Start or restart the health check thread"""
        # First stop any existing health check
        self.stop_health_check()
        
        # Only start if we're the leader
        if not self.is_leader():
            logger.warning("Attempted to start health check when not leader")
            return
            
        # Reset stop flag and create new thread
        self.stop_flag = False
        self.health_thread = threading.Thread(
            target=self._health_check, 
            daemon=True,
            name=f"HealthCheck-{self.node_id}"
        )
        self.health_thread.start()
        logger.info("Health check thread started for leader node")


    def stop_health_check(self):
        """Stop the health check thread"""
        if self.health_thread:
            self.stop_flag = True
            self.health_thread.join(timeout=1)
            if self.health_thread.is_alive():
                logger.warning("Health check thread did not stop gracefully")
            self.health_thread = None

    def _recover_state(self):
        """Recover state after becoming leader"""
        try:
            # Try to load from snapshot first
            snapshot_file = f"snapshot_{self.node_id}.json"
            if os.path.exists(snapshot_file):
                with open(snapshot_file, 'r') as f:
                    snapshot = json.load(f)
                    
                    # Apply the snapshot state
                    with self.master.lock:
                        # Rebuild workers
                        self.master.workers = {
                            addr: WorkerNode(addr) for addr in snapshot['state']['workers']
                        }
                        # Restore worker stats
                        for addr, stats in snapshot['state']['workers'].items():
                            if addr in self.master.workers:
                                self.master.workers[addr].load = stats['load']
                                self.master.workers[addr].replica_count = stats['replica_count']
                        
                        # Restore assignments
                        self.master.database_assignments = snapshot['state']['database_assignments']
                        self.master.document_shards = snapshot['state']['document_shards']
                    
                    logger.info("Recovered state from snapshot")
                    return
            
            # Fall back to replaying the log if no snapshot exists
            logger.info("No snapshot found, replaying entire log")
            for entry in self.raft_node.state.log:
                if entry.index > 0:  # Skip dummy entry at index 0
                    try:
                        operation_data = json.loads(entry.data.decode())
                        self.master.apply_operation(operation_data['op'], operation_data['args'])
                    except Exception as e:
                        logger.error(f"Failed to replay log entry {entry.index}: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Failed to recover state: {str(e)}")


    def _on_state_change(self, new_state: RaftState):
        logger.info(f"Node state changed to: {new_state.role}")
    
        # Save state regardless of role change
        self._save_state()
        
        # Handle role-specific behavior
        if new_state.role == RaftRole.Leader:
            logger.info("This node has become leader - starting health checks")
            self._start_health_check()
            self._verify_all_workers()
        else:
            logger.info("This node is no longer leader - stopping health checks")
            self.stop_health_check()
        self._save_state()


    def is_leader(self) -> bool:
        """Check if this node is currently the leader"""
        return self.raft_node.state.role == RaftRole.Leader

    def _on_apply(self, data: bytes):
        try:
            operation_data = json.loads(data.decode())
            self.apply_operation(operation_data['op'], operation_data['args'])
            self._save_state()
        except Exception as e:
            logger.error(f"Failed to apply operation: {str(e)}")

    # def is_leader(self) -> bool:
    #     return self.raft_node.state.role == RaftRole.Leader

    def _replicate_operation(self, operation: str, *args) -> bool:
        if not self.is_leader():
            return False
        
        operation_data = json.dumps({
            'op': operation,
            'args': args,
            'timestamp': time.time()
        })
        
        try:
            future = self.raft_node.propose(operation_data.encode('utf-8'))
            return future.result(timeout=2.0)
        except Exception as e:
            logger.error(f"Failed to replicate operation: {str(e)}")
            return False
        
    def _select_worker_for_db(self, db_name: str) -> Optional[WorkerNode]:
        """Select the best worker for a new database"""
        with self.lock:
            # Get all healthy workers
            healthy_workers = [
                w for w in self.workers.values() 
                if w.health
            ]
            
            if not healthy_workers:
                print(f"there is no healthy worker")
                return None
            
            # Sort by load, then by replica count
            sorted_workers = sorted(
                healthy_workers,
                key=lambda w: (w.load, w.replica_count))
            
            # Select the least loaded worker
            selected = sorted_workers[0]
            logger.info(f"Selected worker {selected.address} for database {db_name}")
            return selected

    def apply_operation(self, operation: str, args: list):
            
            if operation == "worker_healthy":
                address = args[0]
                if address in self.workers:
                    self.workers[address].health = True
                    self.workers[address].failed_checks = 0
                    
            elif operation == "worker_unhealthy":
                address = args[0]
                if address in self.workers:
                    self.workers[address].health = False
            
            elif operation == "create_database":
                db_name, worker_address = args
                if db_name not in self.database_assignments:
                    self.database_assignments[db_name] = worker_address
                    if worker_address in self.workers:
                        self.workers[worker_address].load += 1
                    logger.info(f"Created database {db_name} on {worker_address} (replicated)")
                    self._save_state()  # Persist the state change
    
        
            if operation == "add_worker":
                address = args[0]
                if address not in self.workers:
                    self.workers[address] = WorkerNode(address)
                    logger.info(f"Added worker {address} (replicated)")
            
            elif operation == "remove_worker":
                address = args[0]
                if address in self.workers:
                    # Reassign any databases this worker was handling
                    dbs_to_reassign = [
                        db for db, worker in self.database_assignments.items()
                        if worker == address
                    ]
                    
                    for db in dbs_to_reassign:
                        new_worker = self._select_worker_for_db(db)
                        if new_worker:
                            self.database_assignments[db] = new_worker.address
                            new_worker.load += 1
                    
                    del self.workers[address]
                    logger.info(f"Removed worker {address} (replicated)")

            
            elif operation == "assign_database":
                db_name, worker_address = args
                if worker_address in self.workers:
                    self.database_assignments[db_name] = worker_address
                    self.workers[worker_address].load += 1
                    logger.info(f"Assigned database {db_name} to {worker_address} (replicated)")
            
            elif operation == "unassign_database":
                db_name = args[0]
                if db_name in self.database_assignments:
                    worker_address = self.database_assignments[db_name]
                    if worker_address in self.workers:
                        self.workers[worker_address].load -= 1
                    del self.database_assignments[db_name]
                    logger.info(f"Unassigned database {db_name} (replicated)")
            
            elif operation == "assign_document":
                db_name, doc_id, worker_address = args
                if db_name not in self.document_shards:
                    self.document_shards[db_name] = {}
                self.document_shards[db_name][doc_id] = worker_address
                logger.info(f"Assigned document {doc_id} to {worker_address} (replicated)")

                
    def add_worker(self, address: str) -> bool:

        # Normalize worker address
        if not address.startswith("localhost:"):
            address = f"localhost:{address.split(':')[-1]}"
        
        with self.lock:
            if address not in self.workers:
                self.workers[address] = WorkerNode(address)
                logger.info(f"Added new worker: {address}")
                if self.is_leader():
                    return self._replicate_operation("add_worker", address)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.MasterServiceStub(channel)
                            return stub.AddWorker(database_pb2.Worker(worker=address)).success
                        except:
                            pass
            else:
                # Update existing worker
                self.workers[address].last_heartbeat = time.time()
                self.workers[address].health = True
            return False
        
    def remove_worker(self, address: str) -> bool:
        
        if address in self.workers:
                if self.is_leader():
                    return self._replicate_operation("remove_worker", address)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.MasterServiceStub(channel)
                            return stub.RemoveWorker(database_pb2.Worker(worker=address)).success
                        except:
                            pass
        return False

    def assign_database(self, db_name: str, worker_address: str) -> bool:
        
        if worker_address in self.workers:
                if self.is_leader():
                    return self._replicate_operation("assign_database", db_name, worker_address)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.MasterServiceStub(channel)
                            return stub.AssignDatabase(
                                database_pb2.DatabaseAssignment(
                                    db_name=db_name,
                                    worker_address=worker_address
                                )
                            ).success
                        except:
                            pass
        return False

    def unassign_database(self, db_name: str) -> bool:
        
        if db_name in self.database_assignments:
                if self.is_leader():
                    return self._replicate_operation("unassign_database", db_name)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.MasterServiceStub(channel)
                            return stub.UnassignDatabase(
                                database_pb2.DatabaseName(name=db_name)
                            ).success
                        except:
                            pass
        return False

    def assign_document(self, db_name: str, doc_id: str, worker_address: str) -> bool:
        
        if worker_address in self.workers:
                if self.is_leader():
                    return self._replicate_operation("assign_document", db_name, doc_id, worker_address)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.MasterServiceStub(channel)
                            return stub.AssignDocument(
                                database_pb2.DocumentAssignment(
                                    db_name=db_name,
                                    doc_id=doc_id,
                                    worker_address=worker_address
                                )
                            ).success
                        except:
                            pass
        return False

    def get_primary_worker(self, db_name: str) -> Optional[WorkerNode]:
        
            return self.workers.get(self.database_assignments.get(db_name))

    def get_document_worker(self, db_name: str, doc_id: str) -> Optional[str]:
        
            if db_name not in self.document_shards:
                self.document_shards[db_name] = {}
            
            if doc_id not in self.document_shards[db_name]:
                if not self.workers:
                    return None
                
                workers = sorted(self.workers.keys())
                hash_val = int(hashlib.md5(doc_id.encode()).hexdigest(), 16)
                worker_idx = hash_val % len(workers)
                worker_address = workers[worker_idx]
                
                if not self.assign_document(db_name, doc_id, worker_address):
                    return None
            
            return self.document_shards[db_name].get(doc_id)

    def get_document_replicas(self, db_name: str, doc_id: str) -> List[str]:
        
            primary_worker = self.get_primary_worker(db_name)
            primary_address = primary_worker.address if primary_worker else None
            workers = [
                w.address for w in self.workers.values()
                if w.health and w.address != primary_address
            ]
            if len(workers) < 1:
                return []
            replica_count = min(3, len(workers))
            workers_sorted = sorted(workers, key=lambda addr: self.workers[addr].replica_count)
            selected = workers_sorted[:replica_count]
            for addr in selected:
                self.workers[addr].replica_count += 1
            return selected

    def decrement_replica_count(self, worker_address: str):
        
            if worker_address in self.workers:
                self.workers[worker_address].replica_count = max(
                    0, self.workers[worker_address].replica_count - 1
                )

    def _verify_all_workers(self):
        """Immediately verify all workers when becoming leader"""
        with self.lock:
            for address, worker in list(self.workers.items()):
                try:
                    with worker.lock:
                        worker.stub.GetLoadInfo(database_pb2.Empty(), timeout=1)
                        worker.health = True
                        worker.failed_checks = 0
                        worker.last_heartbeat = time.time()
                except:
                    with worker.lock:
                        worker.health = False
        logger.info("Completed initial worker verification")

    def _health_check(self):
        """Improved health check with leader verification"""
        logger.info("Health check thread running")
        while not self.stop_flag and self.is_leader():  # Only run if we're leader
            try:
                current_time = time.time()
                responsive_workers = set()

                with self.lock:
                    for address, worker in list(self.workers.items()):
                        #try:
                            with worker.lock:
                                if current_time - worker.last_checked < 2:
                                    continue

                                worker.last_checked = current_time
                                response = worker.stub.GetLoadInfo(database_pb2.Empty(), timeout=2)
                                print(f"response of health checks for worker : " , response)
                                worker.failed_checks = 0
                                worker.last_heartbeat = current_time
                                worker.health = True
                                responsive_workers.add(address)

                        # except Exception as e:
                        #     with worker.lock:
                        #         worker.failed_checks += 1
                        #         if worker.failed_checks >= 2:
                        #             worker.health = False
                        #             logger.warning(f"Worker {address} marked unhealthy")

                # Dynamic sleep based on cluster health
                unhealthy_count = sum(1 for w in self.workers.values() if not w.health)
                sleep_time = max(1.0, 3.0 - unhealthy_count)
                time.sleep(sleep_time)

            except Exception as e:
                logger.error(f"Health check error: {str(e)}")
                time.sleep(1)

        logger.info("Health check thread stopping")


    def _replicate_worker_status(self, worker_address: str, is_healthy: bool):
        """Replicate worker status changes across the cluster"""
        if self.is_leader():
            operation = "worker_healthy" if is_healthy else "worker_unhealthy"
            self._replicate_operation(operation, worker_address)
