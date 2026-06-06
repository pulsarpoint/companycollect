package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/config"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/runner"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "translation e2e config error: %v\n", err)
		os.Exit(2)
	}
	result, err := runner.Run(context.Background(), cfg)
	if result != nil {
		result.Print(os.Stdout)
		if cfg.ReportJSONPath != "" {
			if writeErr := result.WriteJSON(cfg.ReportJSONPath); writeErr != nil {
				_, _ = fmt.Fprintf(os.Stderr, "write translation e2e report: %v\n", writeErr)
				os.Exit(1)
			}
		}
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "translation e2e failed: %v\n", err)
		os.Exit(1)
	}
}
