package main

import (
	"bufio"
	"flag"
	"fmt"
	"go-mil/internal/replica"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Console client uses an in-process transaction client (no gRPC).
// Commands: start, read <key>, write <key> <value>, commit, abort, exit
func main() {
	_ = flag.String("addr", "unused", "deprecated: no rpc in console")
	flag.Parse()

	r := replica.NewReplica("console-replica")
	txn := replica.NewLocalClient(r)

	runConsole(txn)
}

func runConsole(txn replica.TxnClient) {
	scanner := bufio.NewScanner(os.Stdin)
	var txID string

	fmt.Println("Console connected locally. Commands: start | read <key> | write <key> <value> | commit | abort | exit")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "exit", "quit":
			fmt.Println("bye")
			return
		case "start":
			txID = handleStart(txn)
		case "read":
			handleRead(txn, txID, parts)
		case "write":
			txID = handleWrite(txn, txID, parts)
		case "commit":
			txID = handleCommit(txn, txID)
		case "abort":
			txID = handleAbort(txn, txID)
		default:
			fmt.Println("unknown command; use start | read <key> | write <key> <value> | commit | abort | exit")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("input error: %v", err)
	}
}

func handleStart(txn replica.TxnClient) string {
	txID, sts, err := txn.Start(nil)
	if err != nil {
		fmt.Printf("start failed: %v\n", err)
		return ""
	}
	fmt.Printf("started tx=%s sts=%d\n", txID, sts)
	return txID
}

func handleRead(txn replica.TxnClient, txID string, parts []string) {
	if len(parts) != 2 {
		fmt.Println("usage: read <key>")
		return
	}
	if txID == "" {
		fmt.Println("no active tx; run start first")
		return
	}
	key := parts[1]
	_, found, err := txn.Read(nil, txID, key)
	if err != nil {
		fmt.Printf("read failed: %v\n", err)
		return
	}
	if found {
		fmt.Printf("%s found (value omitted in stub)\n", key)
	} else {
		fmt.Printf("%s not found\n", key)
	}
}

func handleWrite(txn replica.TxnClient, txID string, parts []string) string {
	if len(parts) != 3 {
		fmt.Println("usage: write <key> <value>")
		return txID
	}
	if txID == "" {
		fmt.Println("no active tx; run start first")
		return txID
	}
	key := parts[1]
	if _, err := strconv.ParseInt(parts[2], 10, 64); err != nil {
		fmt.Printf("invalid value: %v\n", err)
		return txID
	}
	if err := txn.Write(nil, txID, key, 0); err != nil {
		fmt.Printf("write failed: %v\n", err)
		return txID
	}
	fmt.Println("write ok (stub; value ignored)")
	return txID
}

func handleCommit(txn replica.TxnClient, txID string) string {
	if txID == "" {
		fmt.Println("no active tx; run start first")
		return txID
	}
	cts, err := txn.Commit(nil, txID)
	if err != nil {
		fmt.Printf("commit failed: %v\n", err)
		return txID
	}
	fmt.Printf("commit ok cts=%d (stub)\n", cts)
	return ""
}

func handleAbort(txn replica.TxnClient, txID string) string {
	if txID == "" {
		fmt.Println("no active tx; run start first")
		return txID
	}
	if err := txn.Abort(nil, txID); err != nil {
		fmt.Printf("abort failed: %v\n", err)
		return txID
	}
	fmt.Println("abort ok (stub)")
	return ""
}

// Optional: set a reasonable default timeout for any future context usage.
func defaultTimeout() time.Duration { return 3 * time.Second }
