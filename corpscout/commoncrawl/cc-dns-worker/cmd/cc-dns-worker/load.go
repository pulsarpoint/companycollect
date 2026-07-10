package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"cc-dns-worker/internal/load"
	"cc-dns-worker/internal/store"
)

func runLoad(args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	dbPath := fs.String("db", "scan.db", "SQLite stage path")
	scanID := fs.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan id to load")
	_ = fs.Parse(args)

	ctx := context.Background()
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	conn, err := chConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	nr, nd, err := load.FromStore(ctx, conn, st, *scanID)
	if err != nil {
		return err
	}
	fmt.Printf("loaded %d records and %d domain summaries for scan_id=%s\n", nr, nd, *scanID)

	nh, err := load.WriteHostnameRegistry(ctx, conn, st, *scanID, time.Now())
	if err != nil {
		return err
	}
	fmt.Printf("registered %d discovered hostnames for scan_id=%s\n", nh, *scanID)

	// The staged AXFR load pass is a no-op — no ClickHouse round trip at all — unless this scan-id ever
	// staged AXFR work (SeedAXFRDomains added at least one axfr_domains row, i.e. `scan --axfr` or `run`
	// ran with --axfr on for this scan-id). load.LoadAXFR is the SAME function axfrCycle (axfr.go) uses.
	hasAXFR, err := st.HasAXFRWork(ctx, *scanID)
	if err != nil {
		return err
	}
	if !hasAXFR {
		return nil
	}
	if err := load.LoadAXFR(ctx, conn, st, *scanID, time.Now()); err != nil {
		return err
	}
	fmt.Printf("loaded AXFR results for scan_id=%s\n", *scanID)
	return nil
}
