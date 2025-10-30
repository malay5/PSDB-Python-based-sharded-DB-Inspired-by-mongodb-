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
from .logger_util import logger
from .worker_node import WorkerNode
from .raft_node import RaftNode
from .master_node import MasterNode
from .master_service import MasterService

# Somewhere at the top of your file
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
# Pass it to DistributedLock
lock = DistributedLock(redis_client)

def serve_master(node_id: str, raft_port: int, service_port: int, peers: Dict[str, str]):
    raft_addr = f"localhost:{raft_port}"
    master_node = MasterNode(
        node_id=node_id,
        raft_addr=raft_addr,
        peers=peers,
        redis_client=redis_client,
        service_port=service_port
    )
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_MasterServiceServicer_to_server(
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
