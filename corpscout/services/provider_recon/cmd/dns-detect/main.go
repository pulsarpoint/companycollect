// Command dns-detect resolves DNS records into the services they prove.
//
//	dns-detect resolve -knowledge DIR < records.ndjson > results.ndjson
//
// stdin holds record objects ({"record_id", "root_domain", "name", "type",
// "value", "first_seen", "last_seen"}), concatenated or one per line. stdout
// gets one line per record, in input order: its results and findings plus the
// knowledge version that produced them. DIR holds provider-recon documents
// (<slug>.json). The ClickHouse worker (spec slice 4) wraps the same resolver.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"provider_recon/internal/detect/knowledge"
	"provider_recon/internal/detect/resolve"
)

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

// line is one output line: a record's output and the knowledge version.
type line struct {
	KnowledgeVersion string `json:"knowledge_version"`
	resolve.Output
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "resolve" {
		fmt.Fprintln(stderr, "usage: dns-detect resolve -knowledge DIR < records.ndjson")
		return 2
	}
	fs := flag.NewFlagSet("resolve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("knowledge", "", "directory of provider-recon documents (<slug>.json)")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *dir == "" {
		fmt.Fprintln(stderr, "resolve: -knowledge is required")
		return 2
	}
	kb, err := knowledge.LoadDir(*dir)
	if err != nil {
		fmt.Fprintf(stderr, "resolve: load knowledge: %v\n", err)
		return 1
	}
	dec := json.NewDecoder(bufio.NewReader(stdin))
	out := bufio.NewWriter(stdout)
	defer out.Flush()
	enc := json.NewEncoder(out)
	for n := 1; ; n++ {
		var rec resolve.Record
		if err := dec.Decode(&rec); errors.Is(err, io.EOF) {
			return 0
		} else if err != nil {
			fmt.Fprintf(stderr, "resolve: record %d: %v\n", n, err)
			return 1
		}
		if err := rec.Validate(); err != nil {
			fmt.Fprintf(stderr, "resolve: record %d: %v\n", n, err)
			return 1
		}
		if err := enc.Encode(line{KnowledgeVersion: kb.Version(), Output: resolve.Resolve(rec, kb)}); err != nil {
			fmt.Fprintf(stderr, "resolve: write: %v\n", err)
			return 1
		}
	}
}
