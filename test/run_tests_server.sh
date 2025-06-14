#!/bin/bash

# Paths
TEST_COORD_BIN="./test/coordinator/main.go"
TEST_SERVER_BIN="./test/server/main.go"
CLIENT_BIN="./code/cmd/client/main.go"
CONFIG_FILE="./test/test_config.json"
RESULT_LOG="./test/results_S1_S6.log"
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
# start_backends() {
#     BACKEND_PIDS=()
#     for BACKEND_ID in $BACKENDS; do
#         echo "Starting backend $BACKEND_ID..." | tee -a "$RESULT_LOG"
#         go run $TEST_SERVER_BIN --config "$CONFIG_FILE" --backend "$BACKEND_ID" > "$LOG_DIR/${BACKEND_ID}.log" 2>&1 &
#         BACKEND_PIDS+=($!)
#         sleep 1
#     done
# }
start_backends() {
    local sleep_ms=$1
    shift
    BACKEND_PIDS=()
    for BACKEND_ID in $BACKENDS; do
        echo "Starting backend $BACKEND_ID with TEST_SLEEP_MS=${sleep_ms} and extra env vars: $@" | tee -a "$RESULT_LOG"
        # env TEST_MODE=true TEST_SLEEP_MS=${sleep_ms} "$@" go run $TEST_SERVER_BIN --config "$CONFIG_FILE" --backend "$BACKEND_ID" > "$LOG_DIR/${BACKEND_ID}.log" 2>&1 &
        env TEST_MODE=true TEST_SLEEP_MS=${sleep_ms} "$@" stdbuf -oL -eL go run $TEST_SERVER_BIN --config "$CONFIG_FILE" --backend "$BACKEND_ID" > "$LOG_DIR/${BACKEND_ID}.log" 2>&1 &

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
echo "2PC S1-S6 Test Run - $(date)" > "$RESULT_LOG"
echo "==========================" >> "$RESULT_LOG"
echo "" >> "$RESULT_LOG"


cleanup_ports() {
    echo "Cleaning up ports..."
    sudo lsof -t -iTCP:8000 -sTCP:LISTEN | xargs -r sudo kill -9
    sudo lsof -t -iTCP:8001 -sTCP:LISTEN | xargs -r sudo kill -9

}

sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
sudo lsof -t -iTCP:9090 -sTCP:LISTEN | xargs -r sudo kill -9
start_coordinator 5000
# ######################################
# # S1 - Backend crash before Prepare
# ######################################
# start_backends
# sleep 2
# stop_backends
# cleanup_ports
# sleep 1
# start_backends
# sleep 2
# run_client_txn "S1 - Backend crash before Prepare" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR credit A 10"
# stop_backends
# cleanup_ports

# ######################################
# # S2 - Backend crash after Prepare written but before ACK sent
# ######################################
# start_backends 11000 PAUSE_AT_S2=true
# sleep 2
# run_client_txn "S2 - Backend crash after Prepare written but before ACK" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer B A 10 &"
# CLIENT_PID=$!
# sleep 3
# stop_backends
# cleanup_ports
# wait $CLIENT_PID
# start_backends
# sleep 5
# stop_backends
# cleanup_ports

# ######################################
# # S3 - Backend crash after Prepare ACK, before Commit
# ######################################
# start_backends
# sleep 2
# run_client_txn "S3 - Backend crash after Prepare ACK before Commit" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer A B 10 &"
# CLIENT_PID=$!
# sleep 3
# stop_backends
# cleanup_ports
# wait $CLIENT_PID
# start_backends
# sleep 5
# stop_backends
# cleanup_ports

# ######################################
# # S4 - Backend crash after partial Commit applied
# ######################################
# start_backends 11000 PAUSE_AT_S4=true
# sleep 2
# run_client_txn "S4 - Backend crash after partial Commit applied" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer B A 10 &"
# CLIENT_PID=$!
# sleep 3
# stop_backends
# cleanup_ports
# wait $CLIENT_PID
# start_backends
# sleep 5
# stop_backends
# cleanup_ports

# ######################################
# # S5 - Backend crash after Commit fully done
# ######################################
start_backends 5000 PAUSE_AT_S5=true
sleep 20    
run_client_txn "S5 - Backend crash after Commit fully done" \
    "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer B A 10 &"
CLIENT_PID=$!
sleep 3
stop_backends
cleanup_ports
wait $CLIENT_PID
start_backends
sleep 5
stop_backends
cleanup_ports

# ######################################
# # S6 - Backend crash after Abort written
# ######################################
# start_backends 5000 PAUSE_AT_S6=true
# sleep 2
# run_client_txn "S6 - Backend crash after Abort written" \
#     "go run $CLIENT_BIN -coordinator=$COORDINATOR_ADDR -client=$CLIENT_ADDR transfer A B 999999 &"
# CLIENT_PID=$!
# sleep 3
# stop_backends
# cleanup_ports
# wait $CLIENT_PID
# start_backends
# sleep 5
# stop_backends
# cleanup_ports

# ######################################
# # Final cleanup of ports after all tests
# ######################################
# echo "Final cleanup of ports after all tests..."
# cleanup_ports

# Done
echo "All S1-S6 tests completed. Results saved to $RESULT_LOG."
sudo lsof -t -iTCP:9000 -sTCP:LISTEN | xargs -r sudo kill -9
sudo lsof -t -iTCP:9090 -sTCP:LISTEN | xargs -r sudo kill -9