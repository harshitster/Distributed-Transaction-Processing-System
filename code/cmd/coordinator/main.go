package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"os"
	"time"

	"github.com/harshitster/223B-Project/code/coordinator"
	"github.com/harshitster/223B-Project/code/proto"
	"google.golang.org/grpc"
)

type BinConfig struct {
	Coordinator  string            `json:"coordinator"`
	BinToBackend map[string]string `json:"bin_to_backend"`
	BackendMap   map[string]string `json:"backend_map"`
	NumBins      int               `json:"num_bins"`
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to config.json")
	flag.Parse()

	if configPath == "" {
		log.Fatal("Please provide the path to config.json using --config")
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

	lis, err := net.Listen("tcp", cfg.Coordinator)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", cfg.Coordinator, err)
	}
	log.Printf("[Coordinator] Listening on %s", cfg.Coordinator)

	grpcServer := grpc.NewServer()

	coord, err := coordinator.NewCoordinator(
		configPath,
		"coordinator.log",
		5*time.Second,
		100,
	)
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}

	proto.RegisterCoordinatorServiceServer(grpcServer, coord)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve coordinator: %v", err)
	}
}
