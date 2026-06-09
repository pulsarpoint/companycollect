package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(os.Args[1:], os.Stdout); err != nil {
		logger.Error("corpscout source command failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("command is required")
	}
	registry := defaultRegistry()
	switch args[0] {
	case "list-sources":
		for _, key := range registry.Keys() {
			if _, err := fmt.Fprintln(output, key); err != nil {
				return errors.Wrap(err, "write source list")
			}
		}
		return nil
	default:
		return errors.Errorf("unknown command %s", args[0])
	}
}

func defaultRegistry() companysources.Registry {
	return companysources.NewRegistry(finlandPRHYTJTemporary{})
}

type finlandPRHYTJTemporary struct{}

func (finlandPRHYTJTemporary) Key() companysources.Key {
	return companysources.Key{Country: "finland", Source: "prhytj"}
}

func (finlandPRHYTJTemporary) DisplayName() string {
	return "PRH Open Data YTJ API v3 companies"
}

func (finlandPRHYTJTemporary) Import(context.Context, companysources.ImportOptions) (companysources.ImportResult, error) {
	return companysources.ImportResult{}, errors.New("finland/prhytj import is not wired yet")
}
