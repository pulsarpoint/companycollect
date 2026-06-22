// wappalyzer-service: a thin HTTP sidecar wrapping projectdiscovery/wappalyzergo.
//
// The Python CommonCrawl pipeline POSTs batches of {id, headers, body} extracted from
// WARC response records; the service runs wappalyzergo (headers+body matching, no JS)
// and returns detected technologies with categories and version. Using wappalyzergo
// keeps the fingerprint set identical to pulsarprotectrunner2 and updatable in one place
// (bump the module version, or point WAPPALYZER_FINGERPRINTS_FILE at a fresher file).
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	wappalyzer "github.com/projectdiscovery/wappalyzergo"
)

type item struct {
	ID      string              `json:"id"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
}

type analyzeRequest struct {
	Items []item `json:"items"`
}

type technology struct {
	Name       string   `json:"name"`
	Categories []string `json:"categories"`
	Version    string   `json:"version"`
	Confidence int      `json:"confidence"`
}

type result struct {
	ID           string       `json:"id"`
	Technologies []technology `json:"technologies"`
}

type analyzeResponse struct {
	Results []result `json:"results"`
}

var client *wappalyzer.Wappalyze

func fingerprint(it item) []technology {
	info := client.FingerprintWithInfo(it.Headers, []byte(it.Body))
	techs := make([]technology, 0, len(info))
	for key, app := range info {
		// wappalyzergo encodes a detected version as "Name:version".
		name, version := key, ""
		if i := strings.Index(key, ":"); i >= 0 {
			name, version = key[:i], key[i+1:]
		}
		techs = append(techs, technology{
			Name: name, Categories: app.Categories, Version: version, Confidence: 100,
		})
	}
	return techs
}

func handleAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req analyzeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := analyzeResponse{Results: make([]result, 0, len(req.Items))}
	for _, it := range req.Items {
		resp.Results = append(resp.Results, result{ID: it.ID, Technologies: fingerprint(it)})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func main() {
	var err error
	// Optional external fingerprints file (fetched from GitHub) supersedes the embedded set.
	if path := os.Getenv("WAPPALYZER_FINGERPRINTS_FILE"); path != "" {
		client, err = wappalyzer.NewFromFile(path, true, true)
	} else {
		client, err = wappalyzer.New()
	}
	if err != nil {
		log.Fatalf("init wappalyzer: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/analyze", handleAnalyze)
	mux.HandleFunc("/health", handleHealth)

	addr := os.Getenv("WAPPALYZER_ADDR")
	if addr == "" {
		addr = ":9876"
	}
	log.Printf("wappalyzer-service listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
