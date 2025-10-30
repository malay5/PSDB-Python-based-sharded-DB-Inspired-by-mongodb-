import ast
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
from .logger_util import logger
from .simpleJSON import SimpleJSONDB
from .shard_metadata import ShardMetadata
from .index import BPlusTree , BPlusTreeNode , IndexManager
from .database_manager import DatabaseManager
from .database_service import DatabaseService


def process_log_file(port: int):
    """Check for and process log file for this worker if it exists"""
    log_dir = "logs"
    log_file = os.path.join(log_dir, f"worker_{port}_request_log.txt")

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        logger.info(f"Created log directory: {log_dir}")
        return

    if not os.path.exists(log_file):
        logger.info(f"No existing log file found at {log_file}")
        return

    logger.info(f"Found log file {log_file}, processing operations...")

    try:
        manager = DatabaseManager()

        # Read and process the log file directly without creating a temp file
        with open(log_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    timestamp, operation = line.split(' - ', 1)

                    if operation.startswith("UseDatabase request:"):
                        db_name = operation.split('db=')[1]
                        manager.use_database(db_name)

                    elif operation.startswith("CreateDatabase request:"):
                        db_name = operation.split('db=')[1]
                        manager.create_database(db_name)
                        manager.use_database(db_name)

                    elif operation.startswith("CreateDocument request:"):
                        parts = operation.split(', ')
                        db_name = parts[0].split('db=')[1]
                        doc_id = parts[1].split('doc_id=')[1]
                        content_str = ','.join(parts[2:]).split('content=')[1]
                        content = ast.literal_eval(content_str)

                        manager.use_database(db_name)
                        manager.create_document(content, doc_id)
                        logger.info(f"Recreated document {doc_id} in DB {db_name} during log recovery.")

                    elif operation.startswith("UpdateDocument:"):
                        parts = operation.split(', ')
                        db_name = parts[0].split('db=')[1]
                        doc_id = parts[1].split('doc_id=')[1].strip()  # Trim whitespace
                        updates_str = ','.join(parts[2:]).split('updates=')[1]
                        updates = ast.literal_eval(updates_str)

                        manager.use_database(db_name)
                        db = manager.current_db['db']

                        try:
                            doc = db.read_document(doc_id)
                            if not doc:
                                # If document doesn't exist, create it with a consistent structure
                                logger.warning(f"Document {doc_id} not found. Creating new document.")
                                doc = {
                                    **updates,
                                    "_id": doc_id,
                                    "_created_at": datetime.now().isoformat(),
                                    "_updated_at": datetime.now().isoformat(),
                                    "_version": 1,
                                    "_primary": False  # Adjust based on your logic
                                }
                                db.create_document(doc, doc_id)
                            else:
                                # Document exists, apply updates
                                doc.update(updates)
                                doc['_updated_at'] = datetime.now().isoformat()
                                doc['_version'] = doc.get('_version', 0) + 1
                                db.create_document(doc, doc_id)  # Overwrite with updated content

                            logger.info(f"Processed document {doc_id} in DB {db_name}.")
                        except Exception as e:
                            logger.error(f"Failed to process document {doc_id}: {e}")

                except Exception as e:
                    logger.error(f"Failed to process log line '{line}': {str(e)}")
                    continue

        logger.info(f"Successfully processed all operations from log file. Original log file remains unchanged.")

    except Exception as e:
        logger.error(f"Error processing log file: {str(e)}")

        


def serve_worker(port: int, master_addresses: List[str]):
    """
    Start a worker node.
    
    Args:
        port: Port for worker gRPC service
        master_addresses: List of master node addresses (host:port)
    """
    # First process any existing log file
    process_log_file(port)
    worker_address = f"localhost:{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_WorkerServiceServicer_to_server(
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
