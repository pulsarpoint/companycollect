package main

import "flag"

func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	_ = fs.Parse(args)
	return nil // wired up in Task 9
}
