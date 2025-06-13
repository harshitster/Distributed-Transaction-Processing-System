package coordinator

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sync"
	"time"

	"github.com/harshitster/223B-Project/code/proto"
)

type Coordinator struct {
	proto.UnimplementedCoordinatorServiceServer

	TxnQueue       []*Transaction
	TxnMap         map[string]*Transaction
	QueueMu        sync.Mutex
	TxnMu          sync.Mutex
	Timeout        time.Duration
	ProcessChannel chan *Transaction
	NumBins        int
	BinsToBackend  map[string]string
	logPath        string
	logMu          sync.RWMutex
}

func NewCoordinator(BinsJSON string, logPath string, timeout time.Duration, maxNumTransactions int) (*Coordinator, error) {
	log.Printf("NewCoordinator: Creating new coordinator with config - BinsJSON: %s, logPath: %s, timeout: %v, maxTransactions: %d",
		BinsJSON, logPath, timeout, maxNumTransactions)

	coordinator := &Coordinator{
		TxnMap:         make(map[string]*Transaction),
		Timeout:        timeout,
		ProcessChannel: make(chan *Transaction, maxNumTransactions),
		logPath:        logPath,
	}
	log.Printf("NewCoordinator: Initialized coordinator struct with process channel capacity: %d", maxNumTransactions)

	log.Printf("NewCoordinator: Loading bin mapping configuration from %s", BinsJSON)
	if err := coordinator.LoadBinMappingConfig(BinsJSON); err != nil {
		log.Printf("NewCoordinator: Failed to load bin mapping config from %s: %v", BinsJSON, err)
		return nil, fmt.Errorf("could not load %s", BinsJSON)
	}
	log.Printf("NewCoordinator: Successfully loaded bin mapping configuration")

	log.Printf("NewCoordinator: Attempting to recover queue from file: queue.json")
	if err := coordinator.RecoverQueueFromFile("queue.json"); err != nil {
		log.Printf("NewCoordinator: Failed to recover queue from file (continuing anyway): %v", err)
	} else {
		log.Printf("NewCoordinator: Successfully recovered queue from file")
	}

	log.Printf("NewCoordinator: Starting background workers")
	go coordinator.ChannelWorker()
	log.Printf("NewCoordinator: Started ChannelWorker goroutine")

	go coordinator.QueueWorker()
	log.Printf("NewCoordinator: Started QueueWorker goroutine")

	log.Printf("NewCoordinator: Coordinator initialization completed successfully")
	return coordinator, nil
}

func (c *Coordinator) LoadBinMappingConfig(path string) error {
	log.Printf("LoadBinMappingConfig: Starting to load configuration from file: %s", path)

	file, err := os.ReadFile(path)
	if err != nil {
		log.Printf("LoadBinMappingConfig: Failed to read config file %s: %v", path, err)
		return err
	}
	log.Printf("LoadBinMappingConfig: Successfully read config file, size: %d bytes", len(file))

	var config struct {
		BinMap  map[string]string `json:"bin_to_backend"`
		NumBins int               `json:"num_bins"`
	}

	if err := json.Unmarshal(file, &config); err != nil {
		log.Printf("LoadBinMappingConfig: Failed to unmarshal JSON from file %s: %v", path, err)
		return err
	}
	log.Printf("LoadBinMappingConfig: Successfully parsed JSON configuration")

	c.BinsToBackend = config.BinMap
	c.NumBins = config.NumBins

	log.Printf("LoadBinMappingConfig: Configuration loaded successfully from %s", path)
	log.Printf("LoadBinMappingConfig: Number of bins configured: %d", c.NumBins)
	log.Printf("LoadBinMappingConfig: Bin mappings loaded: %d entries", len(c.BinsToBackend))

	for bin, addr := range c.BinsToBackend {
		log.Printf("LoadBinMappingConfig: Bin mapping - %s → %s", bin, addr)
	}

	return nil
}

func (c *Coordinator) hashToBin(account string) string {
	log.Printf("hashToBin: Computing bin for account: %s", account)

	h := fnv.New32a()
	h.Write([]byte(account))
	hashValue := h.Sum32()
	binIndex := int(hashValue) % c.NumBins
	binName := fmt.Sprintf("bin%d", binIndex)

	log.Printf("hashToBin: Account %s -> hash: %d -> bin index: %d -> bin name: %s",
		account, hashValue, binIndex, binName)

	return binName
}
