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

class WorkerNode:
    def __init__(self, address):
        self.address = address
        self.channel = grpc.insecure_channel(address)
        self.stub = database_pb2_grpc.WorkerServiceStub(self.channel)
        self.health = True
        self.load = 0
        self.replica_count = 0
        self.last_heartbeat = time.time()
        self.lock = threading.Lock()
        self.failed_checks = 0
        self.last_checked = 0
        self.latency = 0
