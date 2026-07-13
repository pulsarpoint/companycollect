// Command cc-axfr-worker probes authoritative nameservers for AXFR exposure,
// stages resumable work in SQLite, and loads observations into ClickHouse.
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
	fmt.Fprint(os.Stderr, `cc-axfr-worker — scan authoritative nameservers for AXFR exposure

Usage:
  cc-axfr-worker <command> [flags]

Commands:
  scan   run one AXFR scan cycle
  run    continuously supervise resumable AXFR scan cycles

Run "cc-axfr-worker <command> -h" for that command's flags.
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
	slog.Error("AXFR worker command failed", "command", command, "error", err)
	os.Exit(1)
}
