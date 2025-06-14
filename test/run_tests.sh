#!/bin/bash

# Paths
TEST_COORD_BIN="./test/coordinator/main.go"
TEST_SERVER_BIN="./test/server/main.go"
CLIENT_BIN="./code/cmd/client/main.go"
CONFIG_FILE="./test/test_config.json"
RESULT_LOG="./test/results_c1_c7.log"
LOG_DIR="./test/logs"

mkdir -p "$LOG_DIR"

# Load config
COORDINATOR_ADDR=$(jq -r '.coordinator' "$CONFIG_FILE")
CLIENT_ADDR=$(jq -r '.client_address' "$CONFIG_FILE"
)
BACKENDS=$(jq -r '.backend_map | keys[]' "$CONFIG_FILE")

# Helper to start coordinator
# start_coordinator() {
#     echo "Starting Coordinator..." | tee -a "$RESULT_LOG"
#     TEST_MODE=true TEST_SLEEP_MS=$2 $1 go run $TEST_COORD_BIN --config "$CONFIG_FILE" > "$LOG_DIR/coordinator.log" 2>&1 &
#     COORD_PID=$!
#     echo "Coordinator PID: $COORD_PID" | tee -a "$RESULT_LOG"
#     sleep 2
# }
start_coordinator() {
    local sleep_ms=$1
    shift
    echo "Starting Coordinator with TEST_SLEEP_MS=${sleep_ms} and extra env vars: $@"
    env TEST_MODE=true TEST_SLEEP_MS=${sleep_ms} "$@" go run $TEST_COORD_BIN --config "$CONFIG_FILE" > "$LOG_DIR/coordinator.log" 2>&1 &
    COORD_PID=$!
}
# Helper to stop coordinator
stop_coordinator() {
    echo "Stopping Coordinator PID $COORD_PID" | tee -a "$RESULT_LOG"
    kill $COORD_PID
    sleep 2
}

# Helper to start all backends
start_backends() {
    BACKEND_PIDS=()
    for BACKEND_ID in $BACKENDS; do
        echo "Starting backend $BACKEND_ID..." | tee -a "$RESULT_LOG"
        go run $TEST_SERVER_BIN --config "$CONFIG_FILE" --backend "$BACKEND_ID" > "$LOG_DIR/${BACKEND_ID}.log" 2>&1 &
        BACKEND_PIDS+=($!)
        sleep 1
    done
}

# Helper to stop all backends
stop_backends() {
    echo "Stopping backends..." | tee -a "$RESULT_LOG"
    for pid in "${BACKEND_PIDS[@]}"; do
        kill $pid
    done
    sleep 2
}

# Helper to run client transaction
run_client_txn() {
    local txn_desc=$1
    local txn_cmd=$2
    echo ">>> TEST: $txn_desc" | tee -a "$RESULT_LOG"
    echo "Command: $txn_cmd" | tee -a "$RESULT_LOG"
    eval "$txn_cmd" >> "$RESULT_LOG" 2>&1
    echo "<<< END TEST" | tee -a "$RESULT_LOG"
    echo "" | tee -a "$RESULT_LOG"
}

# START TEST RUN
echo "2PC C1-C7 Test Run - $(date)" > "$RESULT_LOG"
echo "==========================" >> "$RESULT_LOG"
echo "" >> "$RESULT_LOG"

# Start backends
start_backends

#####################################
C1 - Normal Transaction
#####################################
start_coordinator 5000
start_backends
run_client_txn "C1 - Normal Transaction" \
    "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR credit A 10"
stop_coordinator
stop_backends
sudo lsof -t -iTCP:8000 -sTCP:LISTEN | xargs -r sudo kill -9
sudo lsof -t -iTCP:8001 -sTCP:LISTEN | xargs -r sudo kill -9
sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
sudo lsof -t -iTCP:9090 -sTCP:LISTEN | xargs -r sudo kill -9


# #####################################
# C2 - Crash after sending Prepare
# #####################################
# start_coordinator 8000 PAUSE_AT_C2=true
# start_backends
# sleep 2
# run_client_txn "C2 - Crash after sending Prepare" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer A B 10 &"
# CLIENT_PID=$!
# sleep 3
# stop_coordinator
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
# wait $CLIENT_PID
# start_coordinator 5000
# sleep 8
# stop_coordinator
# stop_backends
# sudo lsof -t -iTCP:8000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:8001 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9090 -sTCP:LISTEN | xargs -r sudo kill -9
# ######################################
# # C3 - Crash after Prepare ACKs received but before Commit
# ######################################
# start_coordinator 8000 PAUSE_AT_C3=true
# start_backends
# sleep 2
# run_client_txn "C3 - Crash after Prepare ACKs but before Commit" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer B A 10 &"
# CLIENT_PID=$!
# sleep 3
# stop_coordinator
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9

# wait $CLIENT_PID
# start_coordinator 5000
# sleep 5
# stop_coordinator
# stop_backends
# sudo lsof -t -iTCP:8000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:8001 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9090 -sTCP:LISTEN | xargs -r sudo kill -9
# ######################################
# # C4 - Crash after TxnPrepared log written
# ######################################
# start_coordinator 5000 PAUSE_AT_C4=true
# start_backends
# sleep 2
# run_client_txn "C4 - Crash after writing TxnPrepared" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer A B 5 &"
# CLIENT_PID=$!
# sleep 3
# stop_coordinator
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9

# wait $CLIENT_PID
# start_coordinator 5000
# sleep 5
# stop_coordinator
# stop_backends
# sudo lsof -t -iTCP:8000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:8001 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9090 -sTCP:LISTEN | xargs -r sudo kill -9

# ######################################
# # C5 - Crash after some Commit sent
# ######################################
# start_coordinator 5000 PAUSE_AT_C5=true
# start_backends
# sleep 2
# run_client_txn "C5 - Crash after some Commit sent" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer A B 4 &"
# CLIENT_PID=$!
# sleep 3
# stop_coordinator
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9

# wait $CLIENT_PID
# start_coordinator 5000
# sleep 5
# stop_coordinator
# stop_backends
# sudo lsof -t -iTCP:8000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:8001 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9090 -sTCP:LISTEN | xargs -r sudo kill -9

# ######################################
# # C6 - Crash after full Commit but before client ACK
# ######################################
# start_coordinator 5000 PAUSE_AT_C6=true
# start_backends
# sleep 2
# run_client_txn "C6 - Crash after full Commit but before client ACK" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR credit L 35 &"
# CLIENT_PID=$!
# sleep 3
# stop_coordinator
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9

# wait $CLIENT_PID
# start_coordinator 5000
# sleep 5
# stop_coordinator
# stop_backends
# sudo lsof -t -iTCP:8000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:8001 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9090 -sTCP:LISTEN | xargs -r sudo kill -9

# ######################################
# # C7 - Normal Commit with delayed client ACK
# ######################################
# start_coordinator 5000
# start_backends
# sleep 2
# run_client_txn "C7 - Normal Commit with delayed client ACK" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer M N 40"
# echo "For C7: Simulate delayed ACK by not sending AckTxn immediately (manual if needed)" | tee -a "$RESULT_LOG"
# sleep 5
# stop_coordinator
# stop_backends
# sudo lsof -t -iTCP:8000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:8001 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
# sudo lsof -t -iTCP:9090 -sTCP:LISTEN | xargs -r sudo kill -9

# Done
echo "All C1-C7 tests completed. Results saved to $RESULT_LOG."
