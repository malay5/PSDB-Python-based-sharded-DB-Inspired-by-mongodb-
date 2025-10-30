from ast import Dict
from typing import List, Optional

import grpc


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
 
