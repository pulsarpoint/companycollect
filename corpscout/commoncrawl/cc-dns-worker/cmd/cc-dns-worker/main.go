// Command cc-dns-worker resolves DNS for corpscout domains directly from authoritative
// nameservers, stages results in SQLite (resumable), and loads them into ClickHouse.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func usage() {
	fmt.Fprint(os.Stderr, `cc-dns-worker — resolve corpscout domains directly from authoritative DNS

Usage:
  cc-dns-worker <command> [flags]

Commands:
  scan   run one DNS cycle and one AXFR cycle concurrently
  run    continuously supervise independent DNS and AXFR cycles

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
	switch os.Args[1] {
	case "scan":
		if err := runScan(ctx, os.Args[2:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return
			}
			fmt.Fprintln(os.Stderr, "scan:", err)
			os.Exit(1)
		}
	case "run":
		if err := runOrchestrator(ctx, os.Args[2:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return
			}
			fmt.Fprintln(os.Stderr, "run:", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}
