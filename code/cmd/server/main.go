package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/harshitster/223B-Project/code/proto"
	"github.com/harshitster/223B-Project/code/server"
	"google.golang.org/grpc"
)

type BinConfig struct {
	Coordinator  string            `json:"coordinator"`
	BinToBackend map[string]string `json:"bin_to_backend"`
	BackendMap   map[string]string `json:"backend_map"`
	NumBins      int               `json:"num_bins"`
}

func main() {
	var (
		configPath string
		backendID  string
	)

	flag.StringVar(&configPath, "config", "", "Path to config.json")
	flag.StringVar(&backendID, "backend", "", "Backend ID to launch (e.g., backend0)")
	flag.Parse()

	if configPath == "" || backendID == "" {
		log.Fatal("Usage: go run main.go --config path/to/config.json --backend backend0")
	}

	file, err := os.Open(configPath)
	if err != nil {
		log.Fatalf("Failed to open config file: %v", err)
	}
	defer file.Close()

	var cfg BinConfig
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		log.Fatalf("Failed to decode config.json: %v", err)
	}

	selfAddress, ok := cfg.BackendMap[backendID]
	if !ok {
		log.Fatalf("Backend %s not found in config", backendID)
	}

	var peerAddresses []string
	for _, addr := range cfg.BackendMap {
		peerAddresses = append(peerAddresses, addr)
	}

	logFile := fmt.Sprintf("kvserver_%s.log", backendID)
	kv := server.NewKVServer(logFile, peerAddresses, selfAddress, cfg.Coordinator)

	lis, err := net.Listen("tcp", selfAddress)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", selfAddress, err)
	}

	log.Printf("[KVServer] %s starting on %s", backendID, selfAddress)

	grpcServer := grpc.NewServer()
	proto.RegisterKVServiceServer(grpcServer, kv)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
