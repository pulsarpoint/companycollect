package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
	"github.com/pulsarpoint/companycollect/companies/companysource/internal/cli"
)

func main() {
	result, err := cli.Run(context.Background(), os.Args[1:])
	if err != nil {
		slog.Error("run companysource command", "error_kind", countryimport.Classify(err), "error", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		slog.Error("write companysource result", "error", err)
		os.Exit(1)
	}
}
