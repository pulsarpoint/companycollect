package main

import "flag"

func runScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	_ = fs.Parse(args)
	return nil // wired up in Task 8
}
