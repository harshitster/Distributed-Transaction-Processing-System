package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/harshitster/223B-Project/code/client"
)

type Config struct {
	Coordinator   string `json:"coordinator"`
	ClientAddress string `json:"client_address"`
}

func main() {
	// Default values
	defaultCoordinator := "localhost:9000"
	defaultClientAddr := "localhost:9090"

	// Try to load from config.json
	conf, err := loadConfig()
	if err == nil {
		defaultCoordinator = conf.Coordinator
		defaultClientAddr = conf.ClientAddress
	} else {
		log.Printf("Using default addresses (config error: %v)", err)
	}

	// Define command-line flags
	coordinatorAddr := flag.String("coordinator", defaultCoordinator, "Coordinator address")
	clientAddr := flag.String("client", defaultClientAddr, "Client listener address")
	flag.Parse()

	// Initialize client
	c := client.NewClient(*coordinatorAddr, *clientAddr)
	err = c.StartServer()
	if err != nil {
		log.Fatalf("Failed to start client server: %v", err)
	}
	defer c.Stop()

	time.Sleep(1 * time.Second) // Allow server to start

	// Handle subcommands
	if len(flag.Args()) < 1 {
		printUsage()
		return
	}

	cmd := flag.Arg(0)
	switch cmd {
	case "transfer":
		handleTransfer(c, flag.Args()[1:])
	case "credit":
		handleCredit(c, flag.Args()[1:])
	case "debit":
		handleDebit(c, flag.Args()[1:])
	case "manual":
		handleManual(c, flag.Args()[1:])
	case "batch":
		handleBatch(c, flag.Args()[1:])
	case "perf":
		handlePerformance(c, flag.Args()[1:])
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
	}
}

func loadConfig() (Config, error) {
	var conf Config

	// Get current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return conf, err
	}

	// Check if we're in the project root
	configPath := filepath.Join("./code/config.json")
	if _, err := os.Stat(configPath); err == nil {
		return parseConfig(configPath)
	}

	// If not found, try parent directory (cmd directory case)
	parentDir := filepath.Dir(cwd)
	configPath = filepath.Join(parentDir, "config.json")
	if _, err := os.Stat(configPath); err == nil {
		return parseConfig(configPath)
	}

	return conf, fmt.Errorf("config.json not found")
}

func parseConfig(path string) (Config, error) {
	var conf Config
	file, err := os.Open(path)
	if err != nil {
		return conf, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&conf)
	return conf, err
}

func printUsage() {
	fmt.Println("Usage: client [flags] <command> [arguments]")
	fmt.Println("\nFlags:")
	fmt.Println("  -coordinator=addr Coordinator address (default from config.json)")
	fmt.Println("  -client=addr      Client listener address (default from config.json)")
	fmt.Println("\nCommands:")
	fmt.Println("  transfer <from> <to> <amount>")
	fmt.Println("  credit <account> <amount>")
	fmt.Println("  debit <account> <amount>")
	fmt.Println("  manual <from> <to> <amount>")
	fmt.Println("  batch <count> <from> <to> <amount>")
	fmt.Println("  perf <count> <account>")
}

func handleTransfer(c *client.Client, args []string) {
	if len(args) != 3 {
		log.Fatal("Usage: transfer <from> <to> <amount>")
	}
	amount, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatalf("Invalid amount: %v", err)
	}

	txnID := fmt.Sprintf("transfer_%d", time.Now().UnixNano())
	status, err := c.ExecuteTransactionWithWait(
		txnID,
		"transfer",
		[]string{args[0], args[1]},
		int32(amount), // Convert to int32
		30*time.Second,
	)

	if err != nil {
		log.Printf("Transfer failed: %v", err)
	} else {
		fmt.Printf("Transfer completed. Status: %s\n", status)
	}
}

func handleCredit(c *client.Client, args []string) {
	if len(args) != 2 {
		log.Fatal("Usage: credit <account> <amount>")
	}
	amount, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatalf("Invalid amount: %v", err)
	}

	txnID := fmt.Sprintf("credit_%d", time.Now().UnixNano())
	status, err := c.ExecuteTransactionWithWait(
		txnID,
		"credit",
		[]string{args[0]},
		int32(amount), // Convert to int32
		30*time.Second,
	)

	if err != nil {
		log.Printf("Credit failed: %v", err)
	} else {
		fmt.Printf("Credit completed. Status: %s\n", status)
	}
}

func handleDebit(c *client.Client, args []string) {
	if len(args) != 2 {
		log.Fatal("Usage: debit <account> <amount>")
	}
	amount, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatalf("Invalid amount: %v", err)
	}

	txnID := fmt.Sprintf("debit_%d", time.Now().UnixNano())
	status, err := c.ExecuteTransactionWithWait(
		txnID,
		"debit",
		[]string{args[0]},
		int32(amount), // Convert to int32
		30*time.Second,
	)

	if err != nil {
		log.Printf("Debit failed: %v", err)
	} else {
		fmt.Printf("Debit completed. Status: %s\n", status)
	}
}

func handleManual(c *client.Client, args []string) {
	if len(args) != 3 {
		log.Fatal("Usage: manual <from> <to> <amount>")
	}
	amount, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatalf("Invalid amount: %v", err)
	}

	txnID := fmt.Sprintf("manual_%d", time.Now().UnixNano())
	fmt.Printf("Submitting transaction %s\n", txnID)
	err = c.SubmitTransaction(
		txnID,
		"transfer",
		[]string{args[0], args[1]},
		int32(amount), // Convert to int32
	)
	if err != nil {
		log.Printf("Submission failed: %v", err)
		return
	}

	fmt.Println("Polling for status...")
	for i := 0; i < 10; i++ {
		status, err := c.GetTransactionStatus(txnID)
		if err != nil {
			log.Printf("Status check failed: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		fmt.Printf("Status[%d]: %s\n", i+1, status)
		switch status {
		case "COMMITTED":
			if err := c.AcknowledgeTransaction(txnID); err != nil {
				log.Printf("Acknowledgement failed: %v", err)
			} else {
				fmt.Println("Transaction acknowledged")
			}
			return
		case "ABORTED":
			fmt.Println("Transaction aborted")
			return
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Println("Status polling timed out")
}

func handleBatch(c *client.Client, args []string) {
	if len(args) != 4 {
		log.Fatal("Usage: batch <count> <from> <to> <amount>")
	}
	count, err := strconv.Atoi(args[0])
	if err != nil {
		log.Fatalf("Invalid count: %v", err)
	}
	amount, err := strconv.Atoi(args[3])
	if err != nil {
		log.Fatalf("Invalid amount: %v", err)
	}

	results := make(chan string, count)
	for i := 0; i < count; i++ {
		go func(idx int) {
			txnID := fmt.Sprintf("batch_%d_%d", time.Now().UnixNano(), idx)
			status, err := c.ExecuteTransactionWithWait(
				txnID,
				"transfer",
				[]string{args[1], args[2]},
				int32(amount), // Convert to int32
				30*time.Second,
			)

			if err != nil {
				results <- fmt.Sprintf("Transaction %d failed: %v", idx, err)
			} else {
				results <- fmt.Sprintf("Transaction %d completed: %s", idx, status)
			}
		}(i)
	}

	for i := 0; i < count; i++ {
		fmt.Println(<-results)
	}
}

func handlePerformance(c *client.Client, args []string) {
	if len(args) != 2 {
		log.Fatal("Usage: perf <count> <account>")
	}
	count, err := strconv.Atoi(args[0])
	if err != nil {
		log.Fatalf("Invalid count: %v", err)
	}

	start := time.Now()
	for i := 0; i < count; i++ {
		txnID := fmt.Sprintf("perf_%d_%d", start.UnixNano(), i)
		err := c.SubmitTransaction(
			txnID,
			"credit",
			[]string{args[1]},
			int32(1), // Convert to int32
		)
		if err != nil {
			log.Printf("Submission %d failed: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("Submitted %d transactions in %s\n", count, elapsed)
	fmt.Printf("Throughput: %.2f txns/sec\n", float64(count)/elapsed.Seconds())
}
