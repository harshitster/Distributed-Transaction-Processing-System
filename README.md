# Distributed Transaction Processing System

A fault-tolerant distributed transaction processing system implementing the Two-Phase Commit (2PC) protocol. This system ensures ACID properties across multiple distributed key-value stores with comprehensive crash recovery and fault tolerance mechanisms.

## System Architecture

```mermaid
flowchart TB
 subgraph subGraph0["Client Layer"]
        C["Client<br>CLI Interface"]
  end
 subgraph subGraph1["Coordinator Layer (Go)"]
        COORD["Coordinator<br>2PC Orchestrator"]
        LOGS["Persistent Storage<br>txn.log + queue.json"]
  end
 subgraph subGraph2["Data Management"]
        HASH["Consistent Hashing<br>Key → Bin → Server"]
  end
 subgraph subGraph3["Backend Logs"]
        LOG1[("kv_backend_1.log")]
        LOG2[("kv_backend_2.log")]
        LOG3[("kv_backend_3.log")]
  end
 subgraph subGraph4["Backend Layer (Rust)"]
        KV1["KV Server 1<br>Port 8081"]
        KV2["KV Server 2<br>Port 8082"]
        KV3["KV Server 3<br>Port 8083"]
        subGraph3
  end
    C -- gRPC: Txn, AckTxn, GetStatus --> COORD
    COORD --- LOGS
    COORD -- Prepare/Commit/Abort --> KV1 & KV2 & KV3
    HASH -- Route Keys --> KV1 & KV2 & KV3
    KV1 --- LOG1
    KV2 --- LOG2
    KV3 --- LOG3

     C:::client
     COORD:::coordinator
     LOGS:::storage
     HASH:::routing
     LOG1:::storage
     LOG2:::storage
     LOG3:::storage
     KV1:::backend
     KV2:::backend
     KV3:::backend
    classDef client fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef coordinator fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef backend fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef storage fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef routing fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```

## Transaction Workflow

```mermaid
sequenceDiagram
    participant C as Client
    participant COORD as Coordinator
    participant KV1 as KV Server 1
    participant KV2 as KV Server 2
    participant KV3 as KV Server 3
    
    Note over C,KV3: Phase 1: Prepare Phase
    C->>COORD: Submit Transaction
    COORD->>COORD: Log PENDING
    
    par Parallel Prepare
        COORD->>KV1: Prepare(txn_id)
        COORD->>KV2: Prepare(txn_id)
        COORD->>KV3: Prepare(txn_id)
    end
    
    par Validate & Respond
        KV1->>KV1: Validate & Log PREPARE
        KV2->>KV2: Validate & Log PREPARE
        KV3->>KV3: Validate & Log PREPARE
    end
    
    par Vote Results
        KV1-->>COORD: YES (PREPARED)
        KV2-->>COORD: YES (PREPARED)
        KV3-->>COORD: YES (PREPARED)
    end
    
    Note over C,KV3: Phase 2: Commit Phase
    COORD->>COORD: Log TxnPrepared
    
    par Parallel Commit
        COORD->>KV1: Commit(txn_id)
        COORD->>KV2: Commit(txn_id)
        COORD->>KV3: Commit(txn_id)
    end
    
    par Apply Changes
        KV1->>KV1: Apply & Log COMMIT
        KV2->>KV2: Apply & Log COMMIT
        KV3->>KV3: Apply & Log COMMIT
    end
    
    par Commit Confirmation
        KV1-->>COORD: COMMITTED
        KV2-->>COORD: COMMITTED
        KV3-->>COORD: COMMITTED
    end
    
    COORD->>COORD: Log COMMITTED
    COORD-->>C: Transaction SUCCESS
    C->>COORD: AckTxn
    
    Note over KV1,KV3: PostPrepare Recovery (if needed)
    KV1-.->KV2: Query transaction status
    KV2-.->KV3: Query transaction status
```

## Features
- Fault Tolerance: Handles coordinator and backend crashes at any protocol phase
- Persistent Logging: Durable transaction logs for crash recovery
- PostPrepare Protocol: Termination protocol for uncertain transaction resolution
- Idempotent Operations: Safe handling of duplicate messages and retries
- Comprehensive Testing: Automated test suite covering 13 failure scenarios
- Data Partitioning: Consistent hashing for scalable data distribution
- ACID Guarantees: Full transactional semantics across distributed stores
