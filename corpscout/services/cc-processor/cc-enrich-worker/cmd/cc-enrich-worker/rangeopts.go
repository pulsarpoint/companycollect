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
	parts        partsRange
	warcParallel int // parts produced concurrently; default 4, >=1
}

// validateRunnerOpts validates the runner options. Range reads are the only fetch strategy, so the
// single remaining rule is that the parts-parallelism is at least one.
func validateRunnerOpts(o runnerOpts) error {
	if o.warcParallel < 1 {
		return fmt.Errorf("warcParallel must be >= 1")
	}
	return nil
}
