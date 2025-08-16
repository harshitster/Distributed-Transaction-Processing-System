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

## Key Features

- **Fault Tolerance**: Handles coordinator and backend crashes at any protocol phase
- **Persistent Logging**: Durable transaction logs for crash recovery
- **PostPrepare Protocol**: Termination protocol for uncertain transaction resolution
- **Idempotent Operations**: Safe handling of duplicate messages and retries
- **Comprehensive Testing**: Automated test suite covering 13 failure scenarios
- **Data Partitioning**: Consistent hashing for scalable data distribution
- **ACID Guarantees**: Full transactional semantics across distributed stores

## Test Suite

The system includes extensive testing covering various failure scenarios to ensure fault tolerance and correctness across all phases of the 2PC protocol.

#### Coordinator Crash Tests (C1-C7)

**Test Scenarios:**
- **C1 - Normal Transaction Execution**: Baseline correctness under failure-free conditions
- **C2 - Crash after Sending Prepare**: Coordinator crashes after sending Prepare messages but before receiving responses
- **C3 - Crash after Prepare ACKs Received**: Coordinator crashes after receiving all Prepared acknowledgments but before sending Commit
- **C4 - Crash after TxnPrepared Log Written**: Coordinator crashes after logging TxnPrepared state but before sending Commit messages
- **C5 - Crash after Some Commit Sent**: Coordinator crashes after partially completing the commit phase
- **C6 - Crash after Full Commit but Before Client ACK**: Coordinator crashes after all participants commit but before client notification
- **C7 - Normal Commit with Delayed Client ACK**: System tolerance for delayed client acknowledgments

#### Backend Crash Tests (S1-S6)

**Test Scenarios:**
- **S1 - Backend Crash Before Prepare**: Backend crashes before receiving any Prepare message
- **S2 - Crash after Prepare Written but Before ACK Sent**: Backend crashes after persisting PREPARE log but before sending acknowledgment
- **S3 - Crash after Prepare ACK, Before Commit**: Backend crashes after acknowledging Prepare but before receiving Commit
- **S4 - Crash after Partial Commit Applied**: Backend crashes mid-way through applying the Commit phase
- **S5 - Crash after Commit Fully Done**: Backend crashes after successfully completing the commit operation
- **S6 - Crash after Abort Written**: Backend crashes after logging ABORT but before completing cleanup

#### Extended Failure Scenarios

**Network Partition Tests:**
- **N1 - Partition During Prepare**: Coordinator partitioned from subset of servers during prepare phase
- **N2 - Partition During Commit**: Network partitions occurring after prepare but before commit
- **N3 - Partition Between Backends**: Backend servers unable to communicate during PostPrepare resolution

**Client Fault Tolerance:**
- **CL1 - Client Crash Before ACK**: Client crashes after submitting transaction but before acknowledgment
- **CL2 - Client Crash After Seeing Commit**: Client crashes after receiving COMMITTED response
- **CL3 - Restarted Client GetStatus Flow**: Client restart and transaction status recovery

**Timing and Race Conditions:**
- **T1 - Late Prepare ACK**: Late-arriving Prepare responses after coordinator timeout
- **T2 - Late Commit ACK After Coordinator Crash**: Delayed commit acknowledgments after coordinator restart
- **T3 - DISCARD Despite Commit Observed**: Conflicting PostPrepare outcomes between backends

## Future Work
- **Concurrent Transaction Support**: Key-level conflict detection
- **Dynamic Node Management**: Automatic backend discovery
- **Performance Optimizations**: Batching and pipelining
- **Enhanced Monitoring**: Metrics and health checks
- **Byzantine Fault Tolerance**: Advanced failure handling
