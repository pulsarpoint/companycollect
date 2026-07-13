package main

import (
	"fmt"
	"strconv"
	"strings"
)

type partsRange struct {
	lo uint32
	hi uint32
}

// parsePartsRange parses a string in the format "A-B" (inclusive range) or single "N".
// A and B must be valid uint32 values, and A <= B.
func parsePartsRange(s string) (partsRange, error) {
	if s == "" {
		return partsRange{}, fmt.Errorf("empty string")
	}

	// Try to parse as a single number first
	lo, errLo := strconv.ParseUint(s, 10, 32)
	if errLo == nil {
		// It's a single number
		return partsRange{lo: uint32(lo), hi: uint32(lo)}, nil
	}

	// Try to parse as a range "A-B"
	loStr, hiStr, ok := strings.Cut(s, "-")
	if !ok {
		// No hyphen found and it's not a valid single number
		return partsRange{}, fmt.Errorf("invalid parts range format: %q", s)
	}

	// Check if lo starts with "-" (negative number)
	if loStr == "" {
		return partsRange{}, fmt.Errorf("invalid parts range: lo is empty")
	}

	lo, errLo = strconv.ParseUint(loStr, 10, 32)
	if errLo != nil {
		return partsRange{}, fmt.Errorf("invalid lo: %w", errLo)
	}

	hi, errHi := strconv.ParseUint(hiStr, 10, 32)
	if errHi != nil {
		return partsRange{}, fmt.Errorf("invalid hi: %w", errHi)
	}

	if lo > hi {
		return partsRange{}, fmt.Errorf("invalid range: lo (%d) > hi (%d)", lo, hi)
	}

	return partsRange{lo: uint32(lo), hi: uint32(hi)}, nil
}

type runnerOpts struct {
	parts            partsRange
	mode             string // "local" or "remote"
	remoteMaxPages   int64  // required > 0 for tech/both; industry/embed: used only to warn
	warcParallel     int    // remote lane; default 4, >=1
	downloadParallel int    // local lane; default 2, >=1
	processParallel  int    // local lane; default 2, >=1
	maxWARCFiles     int    // local lane; REQUIRED >=1 (recommend 5); downloadParallel <= maxWARCFiles
}

// validateRunnerOpts validates the runner options.
// Rules:
// - mode must be "local" or "remote"
// - cmd "industry"/"embed" + mode "local" → error "industry/embed selections are sparse; only --mode remote is supported"
// - mode local requires maxWARCFiles >= 1 and downloadParallel <= maxWARCFiles
// - tech/both require remoteMaxPages >= 1
func validateRunnerOpts(cmd string, o runnerOpts) error {
	// Check mode is valid
	if o.mode != "local" && o.mode != "remote" {
		return fmt.Errorf("invalid mode: %q", o.mode)
	}

	// Check industry/embed + local mode
	if (cmd == "industry" || cmd == "embed") && o.mode == "local" {
		return fmt.Errorf("industry/embed selections are sparse; only --mode remote is supported")
	}

	// Check mode local requirements
	if o.mode == "local" {
		if o.maxWARCFiles < 1 {
			return fmt.Errorf("maxWARCFiles must be >= 1 for local mode")
		}
		if o.downloadParallel > o.maxWARCFiles {
			return fmt.Errorf("downloadParallel (%d) must be <= maxWARCFiles (%d)", o.downloadParallel, o.maxWARCFiles)
		}
	}

	// Check tech/both require remoteMaxPages >= 1
	if cmd == "tech" || cmd == "both" {
		if o.remoteMaxPages < 1 {
			return fmt.Errorf("remoteMaxPages must be >= 1 for %s", cmd)
		}
	}

	return nil
}
