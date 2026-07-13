// Command cc-dns-worker resolves DNS for corpscout domains directly from authoritative
// nameservers, stages results in SQLite (resumable), and loads them into ClickHouse.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func usage() {
	fmt.Fprint(os.Stderr, `cc-dns-worker — resolve corpscout domains directly from authoritative DNS

Usage:
  cc-dns-worker <command> [flags]

Commands:
  scan   run one DNS cycle
  run    continuously supervise DNS cycles

Run "cc-dns-worker <command> -h" for that command's flags.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	command := os.Args[1]
	var err error
	switch command {
	case "scan":
		err = runScan(ctx, os.Args[2:])
	case "run":
		err = runSupervisor(ctx, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}

	if err == nil || errors.Is(err, flag.ErrHelp) || errors.Is(err, context.Canceled) {
		return
	}
	slog.Error("DNS worker command failed", "command", command, "error", err)
	os.Exit(1)
}
