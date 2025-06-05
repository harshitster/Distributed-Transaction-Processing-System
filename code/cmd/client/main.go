package main

import (
	"fmt"
	"log"
	"time"

	"github.com/harshitster/223B-Project/code/client"
)

func main() {
	coordinatorAddr := "localhost:9000"
	clientAddr := "localhost:9090"

	c := client.NewClient(coordinatorAddr, clientAddr)

	err := c.StartServer()
	if err != nil {
		log.Fatalf("Failed to start client server: %v", err)
	}
	defer c.Stop()

	time.Sleep(1 * time.Second)

	fmt.Println("=== Example 1: Transfer Transaction ===")
	transferExample(c)

	fmt.Println("\n=== Example 2: Credit Transaction ===")
	creditExample(c)

	fmt.Println("\n=== Example 3: Debit Transaction ===")
	debitExample(c)

	fmt.Println("\n=== Example 4: Manual Transaction Handling ===")
	manualTransactionExample(c)

	fmt.Println("\nClient is running. Press Ctrl+C to exit.")
	select {}
}

func transferExample(c *client.Client) {
	txnID := fmt.Sprintf("transfer_%d", time.Now().UnixNano())

	status, err := c.ExecuteTransactionWithWait(
		txnID,
		"transfer",
		[]string{"alice", "bob"},
		100,
		30*time.Second,
	)

	if err != nil {
		log.Printf("Transfer failed: %v", err)
		return
	}

	fmt.Printf("Transfer completed with status: %s\n", status)
}

func creditExample(c *client.Client) {
	txnID := fmt.Sprintf("credit_%d", time.Now().UnixNano())

	status, err := c.ExecuteTransactionWithWait(
		txnID,
		"credit",
		[]string{"alice"},
		500,
		30*time.Second,
	)

	if err != nil {
		log.Printf("Credit failed: %v", err)
		return
	}

	fmt.Printf("Credit completed with status: %s\n", status)
}

func debitExample(c *client.Client) {
	txnID := fmt.Sprintf("debit_%d", time.Now().UnixNano())

	status, err := c.ExecuteTransactionWithWait(
		txnID,
		"debit",
		[]string{"bob"},
		200,
		30*time.Second,
	)

	if err != nil {
		log.Printf("Debit failed: %v", err)
		return
	}

	fmt.Printf("Debit completed with status: %s\n", status)
}

func manualTransactionExample(c *client.Client) {
	txnID := fmt.Sprintf("manual_%d", time.Now().UnixNano())

	// Step 1: Submit transaction
	fmt.Printf("Submitting transaction %s\n", txnID)
	err := c.SubmitTransaction(txnID, "transfer", []string{"alice", "charlie"}, 75)
	if err != nil {
		log.Printf("Failed to submit transaction: %v", err)
		return
	}

	// Step 2: Poll for status
	fmt.Printf("Polling for transaction status...\n")
	for i := 0; i < 10; i++ {
		status, err := c.GetTransactionStatus(txnID)
		if err != nil {
			log.Printf("Failed to get status: %v", err)
			continue
		}

		fmt.Printf("Status check %d: %s\n", i+1, status)

		if status == "COMMITTED" {
			fmt.Printf("Transaction committed! Acknowledging...\n")
			err = c.AcknowledgeTransaction(txnID)
			if err != nil {
				log.Printf("Failed to acknowledge: %v", err)
			} else {
				fmt.Printf("Transaction acknowledged successfully\n")
			}
			break
		} else if status == "ABORTED" {
			fmt.Printf("Transaction was aborted\n")
			break
		}

		time.Sleep(1 * time.Second)
	}
}

// Additional utility functions for testing

func batchTransactionTest(c *client.Client) {
	fmt.Println("\n=== Batch Transaction Test ===")

	numTransactions := 5
	results := make(chan string, numTransactions)

	for i := 0; i < numTransactions; i++ {
		go func(idx int) {
			txnID := fmt.Sprintf("batch_txn_%d_%d", time.Now().UnixNano(), idx)

			status, err := c.ExecuteTransactionWithWait(
				txnID,
				"transfer",
				[]string{"alice", "bob"},
				10,
				30*time.Second,
			)

			if err != nil {
				results <- fmt.Sprintf("Transaction %d failed: %v", idx, err)
			} else {
				results <- fmt.Sprintf("Transaction %d completed: %s", idx, status)
			}
		}(i)
	}

	// Collect results
	for i := 0; i < numTransactions; i++ {
		result := <-results
		fmt.Println(result)
	}
}

func performanceTest(c *client.Client) {
	fmt.Println("\n=== Performance Test ===")

	start := time.Now()
	numTransactions := 10

	for i := 0; i < numTransactions; i++ {
		txnID := fmt.Sprintf("perf_txn_%d", i)

		err := c.SubmitTransaction(txnID, "credit", []string{"test_account"}, 1)
		if err != nil {
			log.Printf("Failed to submit transaction %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("Submitted %d transactions in %v (%.2f txn/sec)\n",
		numTransactions, elapsed, float64(numTransactions)/elapsed.Seconds())
}
