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
from .logger_util import logger,LogEntry
from .distributed_lock import DistributedLock



class RaftRole(Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

class RaftState:
    def __init__(self, role: RaftRole = RaftRole.Follower):
        self.role = role
        self.leader_id = None
        self.current_term = 0
        self.voted_for = None
        self.election_timeout = random.uniform(3.0, 5.0)
        self.last_heartbeat = time.time()
        self.commit_index = 0
        self.last_applied = 0
        self.log: List[LogEntry] = []
        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}


class RaftNode:
    def __init__(self, node_id: str, addr: str, peers: Dict[str, str], redis_client,
                 on_leader_change=None, on_state_change=None, on_apply=None):
        self.master_node = None  # Add this line
        self.node_id = node_id
        self.redis_client = redis_client
        self.addr = addr  # Raft port, not used for gRPC directly
        self.peers = peers  # Now maps to service ports
        self.heartbeat_interval = 3.0  # Leader sends heartbeats every 5s
        self.election_timeout_min = 3.0  # Minimum election timeout
        self.election_timeout_max = 5.0  # Maximum election timeout
        self.rpc_timeout = 3.0  # Timeout for RPC calls
        # Leader stability checks
        self.min_leader_stability = 15.0  # Minimum time to remain leader
        self.last_leadership_change = 0
        self.reset_election_timeout()
        self.unreachable_peers = set()  # Track failed peers
        self.peer_reconnect_interval = 30.0  # Try failed peers every 30s

        self.peer_channels = {
            peer_id: grpc.insecure_channel(peer_addr)
            for peer_id, peer_addr in peers.items()
        }
        self.peer_stubs = {
            peer_id: database_pb2_grpc.MasterServiceStub(channel)
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
        self.election_timeout = random.uniform(3.0, 5.0)
        self.stop_flag = False
        self.lock = threading.Lock()
        self.last_snapshot_time = time.time()
        self.snapshot_interval = 60.0  # Seconds between snapshots

        # Initialize log with a dummy entry at index 0
        self.state.log.append(LogEntry(term=0, index=0, data=b''))


    def _check_unreachable_peers(self):
        """Periodically check if unreachable peers have recovered"""
        if not hasattr(self, 'unreachable_peers') or not self.unreachable_peers:
            return
            
        for peer_id in list(self.unreachable_peers):
            try:
                stub = self.peer_stubs[peer_id]
                stub.GetLeader(database_pb2.Empty(), timeout=1.0)
                logger.info(f"Peer {peer_id} has recovered, removing from unreachable set")
                self.unreachable_peers.remove(peer_id)
            except:
                continue

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
            self.state.election_timeout = random.uniform(3.0, 5.0)
                
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

            effective_nodes =  reachable_peers + 1
            required_votes = (effective_nodes // 2) + 1
            logger.debug(f"Node {self.node_id} calculated majority: {required_votes}/{effective_nodes} votes required")

            if not self.peers:
                logger.info(f"Node {self.node_id} is the only node, becoming leader")
                self._become_leader()
                return

            for peer_id, stub in self.peer_stubs.items():
                try:
                    # Skip peers we know are unreachable
                    if hasattr(self, 'unreachable_peers') and peer_id in self.unreachable_peers:
                        continue
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
                self.state.election_timeout = random.uniform(3.0, 5.0)
                self.state.last_heartbeat = time.time()


    def _become_leader(self):
        """Transition to leader state"""
         # Only allow leadership change if it's been stable for min period
        if (hasattr(self, '_last_leadership_change') and 
        (time.time() - self._last_leadership_change < self.min_leader_stability)):
            logger.warning("Leadership change too frequent! Remaining candidate.")
            return
            
        self._last_leadership_change = time.time()
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
        reachable_peers = 0  # Track actually reachable peers
    
        responses = []
        logger.info(f"Node {self.node_id} sending heartbeats as leader for term {self.state.current_term}")
        for peer_id, stub in self.peer_stubs.items():
            try:
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

                # Skip if we know this peer is down (optional optimization)
                if hasattr(self, 'unreachable_peers') and peer_id in self.unreachable_peers:
                    continue

                reachable_peers += 1
                try : 
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
                except grpc.RpcError as e:
                    # Mark peer as unreachable temporarily
                    if not hasattr(self, 'unreachable_peers'):
                        self.unreachable_peers = set()
                    self.unreachable_peers.add(peer_id)
                    logger.warning(f"Heartbeat to {peer_id} failed: {str(e)}")
                    continue
                #logger.info(f"Heartbeat sent to {peer_id}, response: success={response.success}")
            # except Exception as e:
            #     responses.append(False)
            #     logger.warning(f"Failed to send heartbeat to {peer_id}: {str(e)}")
            #     continue
            except Exception as e:
                logger.error(f"Unexpected error preparing heartbeat for {peer_id}: {str(e)}")
                continue

        # Dynamic quorum calculation based on reachable peers
        effective_quorum_size = max(1, (reachable_peers + 1) // 2)  # +1 for self
        logger.debug(f"Effective quorum: {successful_responses+1}/{reachable_peers+1} (required: {effective_quorum_size})")

        if (successful_responses + 1) < effective_quorum_size:
            if not hasattr(self, '_heartbeat_failures'):
                self._heartbeat_failures = 0
            self._heartbeat_failures += 1

          # Only step down after multiple consecutive failures
            if self._heartbeat_failures >= 3:
                logger.warning(f"Lost contact with majority of cluster, stepping down")
                self._step_down()

        # # If less than half of peers respond, step down
        # if successful_responses < (len(self.peers) // 2):
        #     # Add counter for consecutive failures
        #     if not hasattr(self, '_heartbeat_failures'):
        #         self._heartbeat_failures = 0
        #     self._heartbeat_failures += 1
            
        #     # Only step down after 3 consecutive failures
        #     if self._heartbeat_failures >= 3:
        #         self._step_down()
        else:
            # Reset failure counter if we succeeded
            if hasattr(self, '_heartbeat_failures'):
                self._heartbeat_failures = 0


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
                print("Entry index:", entry.index)
                print("Commit index:", self.state.commit_index)
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
