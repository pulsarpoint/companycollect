// Command cc-dns-worker resolves DNS for corpscout domains directly from authoritative
// nameservers, stages results in SQLite (resumable), and loads them into ClickHouse.
package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Fprint(os.Stderr, `cc-dns-worker — resolve corpscout domains directly from authoritative DNS

Usage:
  cc-dns-worker <command> [flags]

Commands:
  scan   resolve domains from ClickHouse into a durable SQLite stage (resumable)
  load   bulk-copy the SQLite stage into corpscout ClickHouse tables

Run "cc-dns-worker <command> -h" for that command's flags.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "scan":
		if err := runScan(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "scan:", err)
			os.Exit(1)
		}
	case "load":
		if err := runLoad(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "load:", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}
