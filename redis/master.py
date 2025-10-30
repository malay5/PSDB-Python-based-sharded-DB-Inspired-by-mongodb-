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

class DistributedLock:
    def __init__(self, redis_client, lock_key='raft_lock'):
        self.redis = redis_client
        self.lock_key = lock_key
        self.lock_value = str(uuid.uuid4())

    def acquire(self, timeout=5):
        return self.redis.set(self.lock_key, self.lock_value, nx=True, ex=timeout)

    def release(self):
        lock_value = self.redis.get(self.lock_key)
        if lock_value and lock_value.decode() == self.lock_value:
            self.redis.delete(self.lock_key)

# Somewhere at the top of your file
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

class RaftRole(Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

class LogEntry:
    def __init__(self, term: int, index: int, data: bytes):
        self.term = term
        self.index = index
        self.data = data

class RaftState:
    def __init__(self, role: RaftRole = RaftRole.Follower):
        self.role = role
        self.leader_id = None
        self.current_term = 0
        self.voted_for = None
        self.election_timeout = random.uniform(5.0, 10.0)
        self.last_heartbeat = time.time()
        self.commit_index = 0
        self.last_applied = 0
        self.log: List[LogEntry] = []
        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Master")

class WorkerNode:
    def __init__(self, address):
        self.address = address
        self.channel = grpc.insecure_channel(address)
        self.stub = database_pb2_grpc.DatabaseServiceStub(self.channel)
        self.health = True
        self.load = 0
        self.replica_count = 0
        self.last_heartbeat = time.time()

class RaftNode:
    def __init__(self, node_id: str, addr: str, peers: Dict[str, str],
                 on_leader_change=None, on_state_change=None, on_apply=None):
        global redis_client
        self.master_node = None  # Add this line
        self.node_id = node_id
        self.redis_client = redis_client
        self.addr = addr  # Raft port, not used for gRPC directly
        self.peers = peers  # Now maps to service ports
        self.heartbeat_interval = 5.0  # Leader sends heartbeats every 5s
        self.election_timeout_min = 5.0  # Minimum election timeout
        self.election_timeout_max = 10.0  # Maximum election timeout
        self.rpc_timeout = 3.0  # Timeout for RPC calls
        # Leader stability checks
        self.min_leader_stability = 15.0  # Minimum time to remain leader
        self.last_leadership_change = 0
        self.reset_election_timeout()

        self.peer_channels = {
            peer_id: grpc.insecure_channel(peer_addr)
            for peer_id, peer_addr in peers.items()
        }
        self.peer_stubs = {
            peer_id: database_pb2_grpc.DatabaseServiceStub(channel)
            for peer_id, channel in self.peer_channels.items()
        }
        self.on_leader_change = on_leader_change
        self.on_state_change = on_state_change
        self.on_apply = on_apply
        self.state = RaftState(RaftRole.Follower)
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(5.0, 10.0)
        self.stop_flag = False
        self.lock = threading.Lock()
        self.last_snapshot_time = time.time()
        self.snapshot_interval = 60.0  # Seconds between snapshots

        # Initialize log with a dummy entry at index 0
        self.state.log.append(LogEntry(term=0, index=0, data=b''))

    def set_master_node(self, master_node):
        """Set reference to master node for snapshots"""
        self.master_node = master_node

    def reset_election_timeout(self):
        """Set a new random election timeout"""
        self.election_timeout = random.uniform(
            self.election_timeout_min, 
            self.election_timeout_max
        )

    def run(self):
        """Raft event loop"""
        while not self.stop_flag:
            time.sleep(0.1)
            current_time = time.time()

            # Apply committed entries to state machine
            self._apply_committed_entries()

            # Take periodic snapshots
            if current_time - self.last_snapshot_time > self.snapshot_interval:
                self._take_snapshot()
                self.last_snapshot_time = current_time            
            
            if self.state.role == RaftRole.Follower:
                    if current_time - self.state.last_heartbeat > self.state.election_timeout:
                        self._become_candidate()
                
            elif self.state.role == RaftRole.Candidate:
                    if current_time - self.state.last_heartbeat > self.state.election_timeout:
                        self._start_election()
                
            elif self.state.role == RaftRole.Leader:
                    if current_time - self.state.last_heartbeat >  self.heartbeat_interval:
                        self._send_heartbeats()
                        self.state.last_heartbeat = current_time

    def _apply_committed_entries(self):
        """Apply committed entries to the state machine"""
        with self.lock:
            while self.state.last_applied < self.state.commit_index:
                self.state.last_applied += 1
                entry = self.state.log[self.state.last_applied]
                if self.on_apply:
                    self.on_apply(entry.data)

    def _take_snapshot(self):
        """Take a snapshot of the current state and compact the log"""
        if not self.master_node:
            logger.warning("Cannot take snapshot - no master node reference")
            return
        with self.lock:
            if self.state.last_applied <= 0:
                return  # Nothing to snapshot
            
            try:
                # Create snapshot data
                snapshot_data = {
                    'last_included_index': self.state.last_applied,
                    'last_included_term': self.state.log[self.state.last_applied].term,
                    'state': {
                        'workers': {addr: {'load': w.load, 'replica_count': w.replica_count}
                                for addr, w in self.master.workers.items()},
                        'database_assignments': self.master.database_assignments,
                        'document_shards': self.master.document_shards
                    }
                }
                
                # Save snapshot to disk
                snapshot_file = f"snapshot_{self.node_id}.json"
                with open(snapshot_file, 'w') as f:
                    json.dump(snapshot_data, f)
                
                # Compact the log (keep only entries after last_applied)
                if self.state.last_applied > 0:
                    self.state.log = self.state.log[self.state.last_applied:]
                    logger.info(f"Compacted log, new length: {len(self.state.log)}")
                    
            except Exception as e:
                logger.error(f"Failed to take snapshot: {str(e)}")

    def _become_candidate(self):
        lock = DistributedLock(self.redis_client)  # redis_client = redis.StrictRedis()
        if not lock.acquire():
            return  # Another node is already becoming a candidate
        try:
            """Transition to candidate state and start election"""
            # Only start an election if no leader has been seen recently
            if time.time() - self.state.last_heartbeat < self.election_timeout_min:
                return
            
            # Random backoff to stagger candidacy (0.5–2.0 seconds)
            backoff = random.uniform(0.5, 2.0)
            logger.debug(f"Node {self.node_id} applying candidacy backoff of {backoff:.2f}s")
            time.sleep(backoff)

            #Recheck state after backoff
            if self.state.role != RaftRole.Follower or time.time() - self.state.last_heartbeat < self.election_timeout_min:
                    logger.debug(f"Node {self.node_id} aborting candidacy after backoff")
                    return
            
            self.state.role = RaftRole.Candidate
            self.state.current_term += 1
            self.state.voted_for = self.node_id
            self.state.last_heartbeat = time.time()
            self.state.election_timeout = random.uniform(.15, 10.0)
                
            if self.on_state_change:
                    self.on_state_change(self.state)
                
            logger.info(f"Node {self.node_id} becoming candidate in term {self.state.current_term}")
            self._start_election()
        finally:
            lock.release()

    
    def _start_election(self):
        """Proper election with RequestVote RPCs"""
        with self.lock:
            if self.state.role != RaftRole.Candidate:
                logger.info(f"Node {self.node_id} aborting election: no longer candidate")
                return

            logger.info(f"Node {self.node_id} starting election in term {self.state.current_term}")
            votes = 1  # Vote for self
            total_nodes = len(self.peers) + 1
            responsive_nodes = 1  # Count self
            reachable_peers = 0  # Track reachable peers

             # Get last log entry info
            last_log_index = len(self.state.log) - 1
            last_log_term = self.state.log[-1].term if self.state.log else 0

            #Check which peers are reachable
            for peer_id, stub in self.peer_stubs.items():
                try:
                    stub.GetLeader(database_pb2.Empty(), timeout=1.0)
                    reachable_peers += 1
                except:
                    logger.debug(f"Node {self.node_id} found peer {peer_id} unreachable")
                    continue

            effective_nodes = max(2, reachable_peers + 1)
            required_votes = (effective_nodes // 2) + 1
            logger.debug(f"Node {self.node_id} calculated majority: {required_votes}/{effective_nodes} votes required")

            if not self.peers:
                logger.info(f"Node {self.node_id} is the only node, becoming leader")
                self._become_leader()
                return

            for peer_id, stub in self.peer_stubs.items():
                try:
                    logger.info(f"Node {self.node_id} requesting vote from {peer_id}")
                    response = stub.RequestVote(
                        database_pb2.VoteRequest(
                            term=self.state.current_term,
                            candidate_id=self.node_id,
                            last_log_index=last_log_index,
                            last_log_term=last_log_term
                        ),
                        timeout=self.rpc_timeout
                    )
                    responsive_nodes += 1
                    logger.info(f"Node {self.node_id} received response from {peer_id}: term={response.term}, vote_granted={response.vote_granted}")

                    if response.term > self.state.current_term:
                        self.state.current_term = response.term
                        self.state.role = RaftRole.Follower
                        self.state.voted_for = None
                        self.state.last_heartbeat = time.time()
                        logger.info(f"Node {self.node_id} stepping down to follower due to higher term {response.term}")
                        return

                    if response.vote_granted:
                        votes += 1
                        logger.info(f"Node {self.node_id} received vote from {peer_id}")
                except Exception as e:
                    logger.warning(f"Node {self.node_id} failed to get vote from {peer_id}: {str(e)}")
                    continue

            logger.info(f"Node {self.node_id} election result: {votes}/{responsive_nodes} votes, required={required_votes}")
            if votes >= required_votes:
                logger.info(f"Node {self.node_id} received majority votes ({votes}/{total_nodes})")
                self._become_leader()
            else:
                logger.info(f"Node {self.node_id} did not receive majority votes ({votes}/{total_nodes}), remaining candidate")
                # Randomize timeout to avoid split votes
                self.state.election_timeout = random.uniform(5.0, 10.0)
                self.state.last_heartbeat = time.time()


    def _become_leader(self):
        """Transition to leader state"""
        if time.time() - self.last_leadership_change < self.min_leader_stability:
            logger.warning(f"Leadership change too frequent! Remaining candidate.")
            return
        self.state.role = RaftRole.Leader
        self.state.leader_id = self.node_id
        self.state.last_heartbeat = time.time()
        self.last_leadership_change = time.time()

        # Initialize leader state
        self.state.next_index = {
                peer_id: len(self.state.log)
                for peer_id in self.peers
            }
        self.state.match_index = {
                peer_id: 0
                for peer_id in self.peers
            }
            
            
        if self.on_leader_change:
                self.on_leader_change(self.node_id)
        if self.on_state_change:
                self.on_state_change(self.state)
            
        logger.info(f"Node {self.node_id} elected as leader for term {self.state.current_term}")
        self._send_heartbeats()

    def _send_heartbeats(self):
        """Send AppendEntries RPCs to all followers"""
        if self.state.role != RaftRole.Leader:
            return
        successful_responses = 0
        responses = []
        logger.info(f"Node {self.node_id} sending heartbeats as leader for term {self.state.current_term}")
        for peer_id, stub in self.peer_stubs.items():
            #try:
                # Prepare the request
                next_idx = self.state.next_index.get(peer_id, 0)
                prev_log_index = next_idx - 1
                prev_log_term = 0
                if prev_log_index >= 0 and prev_log_index < len(self.state.log):
                    prev_log_term = self.state.log[prev_log_index].term
                
                entries = []
                if next_idx < len(self.state.log):
                    entries = [
                        database_pb2.LogEntry(
                            term=e.term,
                            index=e.index,
                            data=e.data
                        )
                        for e in self.state.log[next_idx:]
                    ]
                    
                response = stub.AppendEntries(
                    database_pb2.AppendEntriesRequest(
                        term=self.state.current_term,
                        leader_id=self.node_id,
                        prev_log_index=prev_log_index,
                        prev_log_term=prev_log_term,
                        entries=entries,
                        leader_commit=self.state.commit_index
                    ),
                    timeout=self.rpc_timeout
                )

                print(f"response for heartbeat : " ,response)
                if response.success:
                     # Update nextIndex and matchIndex for follower
                    self.state.next_index[peer_id] = next_idx + len(entries)
                    self.state.match_index[peer_id] = self.state.next_index[peer_id] - 1
                    
                    # Update commit index if majority has replicated
                    self._update_commit_index()
                    successful_responses += 1
                    responses.append(response.success)
                else :
                    if response.term > self.state.current_term:
                        self._step_down(response.term)
                        return
                    else:
                        # Decrement nextIndex and retry
                        self.state.next_index[peer_id] = max(0, next_idx - 1)
                #logger.info(f"Heartbeat sent to {peer_id}, response: success={response.success}")
            # except Exception as e:
            #     responses.append(False)
            #     logger.warning(f"Failed to send heartbeat to {peer_id}: {str(e)}")
            #     continue

        # If less than half of peers respond, step down
        if successful_responses < (len(self.peers) // 2):
            self._step_down()

    def _update_commit_index(self):
        """Update commit index based on matchIndex of followers"""
        if self.state.role != RaftRole.Leader:
            return
            
        # Get all match indices including our own
        match_indices = list(self.state.match_index.values()) + [len(self.state.log) - 1]
        match_indices.sort()
        
        # Find the median (majority)
        new_commit_index = match_indices[len(match_indices) // 2]
        
        # Only commit entries from current term
        if new_commit_index > self.state.commit_index and (
            new_commit_index < len(self.state.log) and 
            self.state.log[new_commit_index].term == self.state.current_term
        ):
            self.state.commit_index = new_commit_index


    def _step_down(self,new_term=None ):
        """Step down to follower state"""
        with self.lock:
            if new_term is not None and new_term > self.state.current_term:
                self.state.current_term = new_term
                
            logger.info(f"Node {self.node_id} stepping down to follower in term {self.state.current_term}")
            self.state.role = RaftRole.Follower
            self.state.leader_id = None
            self.state.voted_for = None
            self.state.last_heartbeat = time.time()
            self.reset_election_timeout()
            
            if self.on_state_change:
                self.on_state_change(self.state)


    def propose(self, data: bytes):
        """Propose a new command to be replicated"""
        future = futures.Future()
        
        if self.state.role != RaftRole.Leader:
            future.set_result(False)
            return future
            
        with self.lock:
            # Ensure data is bytes
            if not isinstance(data, bytes):
                try:
                    data = json.dumps(data).encode('utf-8')
                except:
                    data = str(data).encode('utf-8')
            # Append entry to local log
            entry = LogEntry(
                term=self.state.current_term,
                index=len(self.state.log),
                data=data
            )
            self.state.log.append(entry)
            
            # Immediately send to followers
            self._send_heartbeats()
            
            # Wait for commit (simplified - in reality would need to wait async)
            start_time = time.time()
            while time.time() - start_time < self.rpc_timeout:
                if entry.index <= self.state.commit_index:
                    future.set_result(True)
                    return future
                time.sleep(0.1)
            
            future.set_result(False)
            return future

    def stop(self):
        self.stop_flag = True

    def get_leader_addr(self):
        
        if self.state.leader_id:
                if self.state.leader_id == self.node_id:
                    # Use service port for leader address
                    return self.addr.replace(str(self.addr.split(':')[-1]), str(self.service_port))
                return self.peers.get(self.state.leader_id, "")
        return None

class MasterNode:
    def __init__(self, node_id: str, raft_addr: str, peers: Dict[str, str], service_port: int):
        self.node_id = node_id
        self.service_port = service_port  # Store service port for get_leader_addr
        self.workers: Dict[str, WorkerNode] = {}
        self.database_assignments: Dict[str, str] = {}
        self.document_shards: Dict[str, Dict[str, str]] = {}
        self.lock = threading.Lock()
        # Persistent state for recovery
        self.state_file = f"raft_state_{node_id}.json"
        self._load_state()
        global redis_client
        
        self.raft_node = RaftNode(
            node_id=node_id,
            addr=raft_addr,
            peers=peers,
            on_leader_change=self._on_leader_change,
            on_state_change=self._on_state_change,
            on_apply=self._on_apply        )
        self.raft_node.set_master_node(self)  # Add this line
        self.raft_node.redis_client = redis_client  # Set redis_client after initialization
        self.raft_node.service_port = service_port  # Pass service port to RaftNode
        
        self.health_thread = threading.Thread(target=self._health_check, daemon=True)
        self.health_thread.start()
        self.raft_thread = threading.Thread(target=self._run_raft_loop, daemon=True)
        self.raft_thread.start()

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
                # We became leader - ensure our state is up to date
                self._recover_state()
        else:
            logger.warning("No leader currently available")

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
            
            if operation == "create_database":
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
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
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
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
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
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
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
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
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
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
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

    def _health_check(self):
        while True:
            time.sleep(5)
            current_time = time.time()
            
            with self.lock:
                # Mark unresponsive workers
                for address, worker in list(self.workers.items()):
                    if current_time - worker.last_heartbeat > 15:
                        logger.warning(f"Worker {address} marked unhealthy")
                        worker.health = False
                    
                    # Verify worker responsiveness
                    try:
                        worker.stub.ListDatabases(database_pb2.Empty(), timeout=2)
                        worker.health = True
                        worker.last_heartbeat = current_time
                    except:
                        worker.health = False

class MasterService(database_pb2_grpc.DatabaseServiceServicer):
    def __init__(self, master_node: MasterNode):
        self.master = master_node

    def InstallSnapshot(self, request, context):
        """Handle snapshot installation from leader"""
        if request.term < self.master.raft_node.state.current_term:
            return database_pb2.InstallSnapshotResponse(
                term=self.master.raft_node.state.current_term,
                success=False
            )
        
        # Update term if needed
        if request.term > self.master.raft_node.state.current_term:
            self.master.raft_node.state.current_term = request.term
            self.master.raft_node._step_down()
        
        try:
            # Apply the snapshot
            snapshot_data = json.loads(request.data)
            with self.master.lock:
                # Rebuild workers
                self.master.workers = {
                    addr: WorkerNode(addr) for addr in snapshot_data['state']['workers']
                }
                # Restore worker stats
                for addr, stats in snapshot_data['state']['workers'].items():
                    if addr in self.master.workers:
                        self.master.workers[addr].load = stats['load']
                        self.master.workers[addr].replica_count = stats['replica_count']
                
                # Restore assignments
                self.master.database_assignments = snapshot_data['state']['database_assignments']
                self.master.document_shards = snapshot_data['state']['document_shards']
            
            # Update log and commit index
            self.master.raft_node.state.last_applied = request.last_included_index
            self.master.raft_node.state.commit_index = request.last_included_index
            
            # Save snapshot to disk
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
    
    def Heartbeat(self, request, context):
        try:
            if not self.is_leader():
                logger.info(f"Rejecting heartbeat from {request.worker_address} - not leader")
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return database_pb2.HeartbeatResponse(acknowledged=False)
            logger.info(f"Received heartbeat from worker {request.worker_address}")
            response = database_pb2.HeartbeatResponse(acknowledged=True)
            return response
        except Exception as e:
            logger.error(f"Failed to process heartbeat: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Server error: {str(e)}")
            raise  # Let gRPC handle the error properly

    def RequestVote(self, request, context):
        """Handle vote requests from other nodes"""
        logger.info(f"Node {self.master.raft_node.node_id} received vote request from {request.candidate_id} for term {request.term}")
        response = database_pb2.VoteResponse(
                term=self.master.raft_node.state.current_term,
                vote_granted=False
            )

            # Step down if higher term
        if request.term > self.master.raft_node.state.current_term:
                logger.info(f"Node {self.master.raft_node.node_id} updating term to {request.term} and becoming follower")
                self.master.raft_node.state.current_term = request.term
                self.master.raft_node.state.role = RaftRole.Follower
                self.master.raft_node.state.voted_for = None
                self.master.raft_node.state.last_heartbeat = time.time()

            # Grant vote if:
            # - Same or higher term
            # - Haven't voted or voted for this candidate
        if request.term == self.master.raft_node.state.current_term:
                if self.master.raft_node.state.voted_for is None or self.master.raft_node.state.voted_for == request.candidate_id:
                    # Prefer candidate with lower node_id to resolve ties
                    if self.master.raft_node.state.role == RaftRole.Candidate and request.candidate_id > self.master.raft_node.node_id:
                        logger.info(f"Node {self.node_id} stepping down to follower to favor candidate {request.candidate_id}")
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
            
             # Convert protobuf entries to LogEntry objects
            entries = []
            for e in request.entries:
                entries.append(LogEntry(
                    term=e.term,
                    index=e.index,
                    data=e.data
                ))

            
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
        
        success = self.master.assign_database(request.db_name, request.worker_address)
        return database_pb2.OperationResponse(
            success=success,
            message="Database assigned successfully" if success else "Database assignment failed"
        )

    def UnassignDatabase(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        
        success = self.master.unassign_database(request.name)
        return database_pb2.OperationResponse(
            success=success,
            message="Database unassigned successfully" if success else "Database unassignment failed"
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
            # Return WorkerList with the replica addresses
            return database_pb2.WorkerList(workers=replicas)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.WorkerList()

    def GetDocumentPrimary(self, request, context):
        """Get the primary worker for a specific document"""
        worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
        if not worker_addr:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        
        try:
            # Verify the document exists and is marked as primary
            channel = grpc.insecure_channel(worker_addr)
            stub = database_pb2_grpc.DatabaseServiceStub(channel)
            doc_response = stub.ReadDocument(request)
            if doc_response.document:
                doc = json.loads(doc_response.document)
                if doc.get('_primary', False):
                    return database_pb2.Worker(worker=worker_addr)
        except:
            pass
        
        # If no primary found, select one
        return self.SetDocumentPrimary(
            database_pb2.SetPrimaryRequest(
                db_name=request.db_name,
                doc_id=request.doc_id,
                worker_address=worker_addr
            ),
            context
        )

    def SetDocumentPrimary(self, request, context):
        """Set a specific worker as primary for a document"""
        try:
            channel = grpc.insecure_channel(request.worker_address)
            stub = database_pb2_grpc.DatabaseServiceStub(channel)
            
            # Get current document
            doc_response = stub.ReadDocument(database_pb2.DocumentID(
                db_name=request.db_name,
                doc_id=request.doc_id
            ))
            
            if not doc_response.document:
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found on target worker"
                )
            
            # Update document to mark as primary
            doc = json.loads(doc_response.document)
            doc['_primary'] = True
            doc['_updated_at'] = datetime.now().isoformat()
            
            # Update on primary worker
            stub.ReplicateDocument(database_pb2.ReplicateRequest(
                db_name=request.db_name,
                doc_id=request.doc_id,
                document=json.dumps(doc)
            ))
            
            # Get replicas and update them to mark as non-primary
            replicas = self.GetDocumentReplicas(database_pb2.DocumentID(
                db_name=request.db_name,
                doc_id=request.doc_id
            )).workers
            
            for replica in replicas:
                if replica != request.worker_address:
                    try:
                        channel = grpc.insecure_channel(replica)
                        stub = database_pb2_grpc.DatabaseServiceStub(channel)
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
        """Handle document replication from primary"""
        try:
            # Ensure we're using the correct database
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            doc_data = json.loads(request.document)
            doc_id = request.doc_id
            
            # Check if document exists
            existing_doc = self.manager.current_db['db'].read_document(doc_id)
            
            if existing_doc:
                # Merge updates
                existing_doc.update(doc_data)
                existing_doc['_updated_at'] = datetime.now().isoformat()
                if '_version' in doc_data:
                    existing_doc['_version'] = doc_data['_version']
                self.manager.current_db['db']._save()
            else:
                # Create new document if it doesn't exist
                doc_data['_created_at'] = datetime.now().isoformat()
                doc_data['_updated_at'] = datetime.now().isoformat()
                self.manager.current_db['db'].create_document(doc_data, doc_id)
            
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
                    stub = database_pb2_grpc.DatabaseServiceStub(channel)
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
        
        # Check if database already exists
        if request.name in self.master.database_assignments:
            return database_pb2.OperationResponse(
                success=False,
                message=f"Database '{request.name}' already exists"
            )
        
        # Select a worker for this database
        worker = None
        start_time = time.time()
        while time.time() - start_time < 10:  # Wait up to 10 seconds for workers
            worker = self.master._select_worker_for_db(request.name)
            if worker:
                break
            time.sleep(0.5)
        
        if not worker:
            return database_pb2.OperationResponse(
                success=False,
                message="No available workers"
            )

        # Create with verification
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 1. Create on worker
                worker_response = worker.stub.CreateDatabase(
                    request, 
                    timeout=5
                )
                
                if not worker_response.success:
                    continue  # Will retry

                # 2. Update master state
                with self.master.lock:
                    # Create the operation data
                    operation_data = {
                        'op': 'create_database',
                        'args': [request.name, worker.address],
                        'timestamp': time.time()
                    }
                    
                    # Propose to Raft cluster
                    future = self.master.raft_node.propose(
                        json.dumps(operation_data).encode('utf-8'))
                    
                    if future.result(timeout=5):
                        # Only update local state if Raft replication succeeds
                        self.master.database_assignments[request.name] = worker.address
                        self.master.workers[worker.address].load += 1
                        logger.info(f"Database {request.name} created and assigned to {worker.address}")
                        return database_pb2.OperationResponse(
                            success=True,
                            message=f"Database '{request.name}' created"
                        )
                    else:
                        logger.warning("Raft replication failed, rolling back")
                        # Rollback worker creation if Raft fails
                        worker.stub.DeleteDatabase(
                            database_pb2.DatabaseName(name=request.name),
                            timeout=2
                        )
                        continue

            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed: {str(e)}")
                time.sleep(0.5 * (attempt + 1))  # Exponential backoff

        return database_pb2.OperationResponse(
            success=False,
            message=f"Failed to create database after {max_retries} attempts"
        )



    def _select_worker_for_db(self, db_name: str) -> Optional[WorkerNode]:
        """Select the best worker for a new database"""
        with self.lock:
            # Get all healthy workers with lowest load
            healthy_workers = [
                w for w in self.workers.values() 
                if w.health
            ]
            
            if not healthy_workers:
                return None
            
            # Sort by load, then by replica count
            sorted_workers = sorted(
                healthy_workers,
                key=lambda w: (w.load, w.replica_count)
            )
            
            # Select the least loaded worker
            selected = sorted_workers[0]
            logger.info(f"Selected worker {selected.address} for database {db_name}")
            return selected
    

    def GetPrimaryWorker(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        return database_pb2.Worker(worker=worker.address)

    def GetDocumentPrimary(self, request, context):
        worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
        if not worker_addr:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        
        try:
            channel = grpc.insecure_channel(worker_addr)
            stub = database_pb2_grpc.DatabaseServiceStub(channel)
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
            stub = database_pb2_grpc.DatabaseServiceStub(channel)
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
                        stub = database_pb2_grpc.DatabaseServiceStub(channel)
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
            return database_pb2.OperationResponse(success=False, message=str(e))

    def UseDatabase(self, request, context):
        try:
            print(f"Master knows about these databases: {self.master.database_assignments.keys()}")  # Add this
            # First check local assignments
            if request.name in self.master.database_assignments:
                worker_addr = self.master.database_assignments[request.name]
                worker = self.master.workers.get(worker_addr)
                if worker:
                    print(f"master send request to worker : " , worker)
                    return worker.stub.UseDatabase(request)
            
            # If not found in assignments, check if file exists (for recovery)
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
                # Reassign the database
                if not self.master.assign_database(request.name, worker.address):
                    return database_pb2.OperationResponse(
                        success=False,
                        message="Failed to assign database"
                    )
                return worker.stub.UseDatabase(request)
            
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
            print(f"list of db : " , list(self.master.database_assignments.keys()))
            return database_pb2.DatabaseList(names=list(self.master.database_assignments.keys()))

    def DeleteDatabase(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        try:
            response = worker.stub.DeleteDatabase(request)
            if response.success:
                self.master.unassign_database(request.name)
            return response
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))

    def CreateDocument(self, request, context):
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentID()
        try:
            response = worker.stub.CreateDocument(request)
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
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentResponse()
        return worker.stub.ReadDocument(request)

    def ReadAllDocuments(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentList()
        try:
            shard_locations = worker.stub.GetShardLocations(
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
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentList()
        try:
            shard_locations = worker.stub.GetShardLocations(
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
            
            # Step 1: Get document location
            worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
            if not worker_addr:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found"
                )
            
            # Step 2: Check if this is the primary worker
            try:
                channel = grpc.insecure_channel(worker_addr)
                stub = database_pb2_grpc.DatabaseServiceStub(channel)
                
                # Get current document to check primary status
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
                    # Forward to primary if this isn't it
                    primary_worker = self.master.stub.GetDocumentPrimary(
                        database_pb2.DocumentID(
                            db_name=request.db_name,
                            doc_id=request.doc_id
                        )
                    ).worker
                    
                    if primary_worker:
                        channel = grpc.insecure_channel(primary_worker)
                        stub = database_pb2_grpc.DatabaseServiceStub(channel)
                        return stub.UpdateDocument(request)
                    
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                    return database_pb2.OperationResponse(
                        success=False,
                        message="No primary worker found for document"
                    )
                
                # Step 3: Perform the update
                updates = json.loads(request.updates)
                doc.update(updates)
                doc['_updated_at'] = datetime.now().isoformat()
                doc['_version'] = doc.get('_version', 0) + 1
                
                # Update locally first
                update_response = stub.UpdateDocument(
                    database_pb2.UpdateRequest(
                        db_name=request.db_name,
                        doc_id=request.doc_id,
                        updates=json.dumps(updates)
                ))
                
                if not update_response.success:
                    return update_response
                
                # Step 4: Replicate to secondaries
                replicas = self.master.get_document_replicas(request.db_name, request.doc_id)
                successful_replicas = 0
                
                for replica_addr in replicas:
                    if replica_addr != worker_addr:
                        try:
                            channel = grpc.insecure_channel(replica_addr)
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
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

    def DeleteDocument(self, request, context):
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        return worker.stub.DeleteDocument(request)

    def ClearDatabase(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        return worker.stub.ClearDatabase(request)

def serve_master(node_id: str, raft_port: int, service_port: int, peers: Dict[str, str]):
    raft_addr = f"localhost:{raft_port}"
    master_node = MasterNode(
        node_id=node_id,
        raft_addr=raft_addr,
        peers=peers,
        service_port=service_port
    )
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        MasterService(master_node), server
    )
    server.add_insecure_port(f'[::]:{service_port}')
    server.start()
    
    logger.info(f"Master node {node_id} running:")
    logger.info(f"- Raft port: {raft_port}")
    logger.info(f"- Service port: {service_port}")
    logger.info(f"- Peers: {peers}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down master node")
        master_node.raft_node.stop()
        server.stop(0)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Start a PSDB master node')
    parser.add_argument('--node-id', required=True, help='Unique node ID')
    parser.add_argument('--raft-port', type=int, required=True, help='Raft communication port (unused)')
    parser.add_argument('--service-port', type=int, required=True, help='gRPC service port')
    parser.add_argument('--peers', nargs='*', default=[], 
                        help='List of peer node_id:host:service_port (space separated)')
    
    args = parser.parse_args()
    
    peers = {}
    for peer in args.peers:
        peer_id, addr = peer.split(':', 1)
        peers[peer_id] = addr
    
    serve_master(
        node_id=args.node_id,
        raft_port=args.raft_port,
        service_port=args.service_port,
        peers=peers
    )
