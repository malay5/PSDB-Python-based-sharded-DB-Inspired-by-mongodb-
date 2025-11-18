-----

# PSDB: Python Sharded Distributed Database

   

**PSDB** is a distributed, fault-tolerant document-oriented database built from scratch in Python. It utilizes **gRPC** for internode communication and implements the **Raft Consensus Algorithm** to ensure strong consistency and high availability across the master cluster.

Designed as a lightweight, sharded storage system, PSDB supports dynamic worker node registration, document sharding, and an interactive CLI for managing data.

-----

## 🏗 Architecture

PSDB follows a **Leader-Follower** architecture managed by a Raft consensus cluster.

### High-Level Design

The system consists of three main components:

1.  **Master Cluster (Raft Node):** Handles metadata, leader election, and coordinates database assignments.
2.  **Worker Nodes:** Responsible for actual data storage, indexing (B+ Tree), and query processing.
3.  **Client:** An interactive CLI that connects to the cluster leader to execute commands.

### The Raft Implementation

The Master nodes maintain consistency using the Raft algorithm.

-----

## 🚀 Key Features

  * **Distributed Consensus (Raft):** Implements Leader Election, Log Replication, and Heartbeats to ensure cluster consistency.
  * **Sharding:**
      * **Database Sharding:** Databases are assigned to specific workers based on load.
      * **Document Sharding:** Documents within a database are distributed across workers using consistent hashing.
  * **Fault Tolerance:**
      * Automatic Leader Election if the master fails.
      * Worker health checks and automatic status replication.
      * Log-based recovery and snapshotting.
  * **gRPC Communication:** High-performance RPC framework for internal and external communication.
  * **Concurrency:** Utilizes `ThreadPoolExecutor` for handling concurrent client requests.
  * **Smart Client:** The client automatically detects the current Leader and handles redirects transparently.
  * **Flexible Querying:** Supports Python-lambda style string queries for filtering documents.

-----

## 📂 Project Structure

```text
PSDB/
├── client/
│   └── main.py           # Interactive CLI Client
├── master/
│   ├── main.py           # Entry point for Master Node
│   ├── master_node.py    # Master logic & State management
│   ├── master_service.py # gRPC Service Implementation
│   ├── raft_node.py      # Raft Consensus Algorithm
│   └── distributed_lock.py
├── worker/
│   ├── main.py           # Entry point for Worker Node
│   ├── database_service.py
│   └── simpleJSON.py     # Storage Engine
├── protos/
│   └── database.proto    # gRPC Protocol Definitions
├── logs/                 # Transaction logs
└── requirements.txt
```

-----

## 🛠️ Installation & Setup

### Prerequisites

  * Python 3.8+
  * Redis (Required for distributed locking in Raft)

### 1\. Install Dependencies

```bash
pip install grpcio grpcio-tools redis
```

### 2\. Generate Protobuf Files

Ensure you compile the `.proto` file before running the system.

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. database.proto
```

-----

## ⚡ Usage Guide

To run a fully distributed cluster locally, you need to start Redis, multiple Master nodes (for Raft), Worker nodes, and finally the Client.

### Step 1: Start Redis

```bash
redis-server
```

### Step 2: Start the Master Cluster (Raft)

Open 3 terminal tabs to create a 3-node Raft cluster.

**Node 1 (Leader Candidate):**

```bash
python -m master.main --node-id node1 --raft-port 50060 --service-port 50050 --peers node2:localhost:50051 node3:localhost:50052
```

**Node 2:**

```bash
python -m master.main --node-id node2 --raft-port 50061 --service-port 50051 --peers node1:localhost:50050 node3:localhost:50052
```

**Node 3:**

```bash
python -m master.main --node-id node3 --raft-port 50062 --service-port 50052 --peers node1:localhost:50050 node2:localhost:50051
```

### Step 3: Start Worker Nodes

Open new terminal tabs for storage workers.

**Worker 1:**

```bash
# Port 6001, Connects to Master at 50050
python -m worker.main 6001 localhost:50050
```

**Worker 2:**

```bash
python -m worker.main 6002 localhost:50050
```

### Step 4: Run the Client

```bash
python -m client.main --masters localhost:50050 localhost:50051 localhost:50052
```

-----

## 🎮 Client Commands

Once inside the client CLI, you can use the following commands:

| Command | Description | Example |
| :--- | :--- | :--- |
| **create** | Create a new database | `create users` |
| **use** | Switch context to a database | `use users` |
| **insert** | Insert a JSON document | `insert {"name": "Tejal", "role": "Dev"}` |
| **read** | Read a document by ID | `read <uuid>` |
| **query** | Filter documents using Python syntax | `query 'lambda d: d.get("role") == "Dev"'` |
| **update** | Update a document | `update <uuid> {"role": "Lead"}` |
| **delete\_doc** | Delete a specific document | `delete_doc <uuid>` |
| **workers** | List active worker nodes | `workers` |
| **list** | List all databases | `list` |
| **clear** | Wipe current database | `clear` |

-----

## 🧠 Technical Details

### Raft Consensus

The `master/raft_node.py` implements the core Raft logic:

1.  **Leader Election:** Uses randomized timeouts (3.0s - 5.0s) to prevent split votes.
2.  **Log Replication:** Uses `AppendEntries` RPC to replicate state changes (Db creation, worker assignment) to followers.
3.  **Persistence:** State is saved to `raft_state_{node_id}.json` and logs are compacted via snapshots.

### Distributed Locking

To ensure thread safety and atomic state transitions within the Raft loop, a Redis-based `DistributedLock` is utilized, preventing race conditions during the election phase.

### Recovery

  * **Master Recovery:** On startup, masters load the latest snapshot and replay the log to restore state.
  * **Worker Recovery:** Workers process `worker_{port}_request_log.txt` to rebuild their local B+ Trees and JSON stores in case of a crash.

-----

## 📝 License

This project is open source and available under the [MIT License](https://www.google.com/search?q=LICENSE).
