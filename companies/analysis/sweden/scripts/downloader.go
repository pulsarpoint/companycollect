// Sweden company-data collector for the Bolagsverket "Värdefulla datamängder" API.
//
// Unlike the generic open-data downloader, Sweden's primary source is OAuth2-gated
// (WSO2 gateway, grant_type=client_credentials, scope vardefulla-datamangder:read).
// Register for free credentials via the Kundanmälan form:
//   https://bolagsverket.se/apierochoppnadata/vardefulladatamangder/kundanmalantillapiforvardefulladatamangder.5528.html
//
// Then export:
//   BV_CLIENT_ID, BV_CLIENT_SECRET
//   BV_TOKEN_URL   (provided at registration; gateway base is https://gw.api.bolagsverket.se)
//   BV_DATA_ROOT   (e.g. /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/sweden)
//
// Usage:
//   go run downloader.go 5560125790 [more orgnr...]
//
// For each organisationsnummer it pulls:
//   POST /organisationer   -> base data        -> raw/api/bolagsverket_vdm_org_{orgnr}.json
//   POST /dokumentlista    -> annual-report list-> raw/api/bolagsverket_vdm_doclist_{orgnr}.json
//   GET  /dokument/{id}    -> iXBRL ZIP         -> raw/bulk/bolagsverket_vdm_doc_{id}.zip
//
// SCB's free Företagsregistret API is certificate-based and is intentionally NOT wired here;
// add a tls.Config with the SCB-issued client cert once obtained (scbforetag@scb.se), or wait
// for the September 2026 API-key model.

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const apiBase = "https://gw.api.bolagsverket.se/vardefulla-datamangder/v1"

type tokenResp struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

func getToken(ctx context.Context, c *http.Client) (string, error) {
	tokenURL := os.Getenv("BV_TOKEN_URL")
	if tokenURL == "" {
		return "", fmt.Errorf("BV_TOKEN_URL not set (provided at registration)")
	}
	id, secret := os.Getenv("BV_CLIENT_ID"), os.Getenv("BV_CLIENT_SECRET")
	if id == "" || secret == "" {
		return "", fmt.Errorf("BV_CLIENT_ID / BV_CLIENT_SECRET not set")
	}
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("scope", "vardefulla-datamangder:read")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(id, secret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint HTTP %d: %s", resp.StatusCode, string(body))
	}
	var tr tokenResp
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", err
	}
	if tr.AccessToken == "" {
		return "", fmt.Errorf("no access_token in response: %s", string(body))
	}
	return tr.AccessToken, nil
}

func saveWithMeta(path, sourceURL string, status int, contentType string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	h := sha256.Sum256(data)
	meta := map[string]any{
		"source_url":     sourceURL,
		"retrieved_at":   time.Now().UTC().Format(time.RFC3339),
		"http_status":    status,
		"content_type":   contentType,
		"content_length": len(data),
		"saved_as":       path,
		"sha256":         hex.EncodeToString(h[:]),
	}
	mb, _ := json.MarshalIndent(meta, "", "  ")
	return os.WriteFile(path+".metadata.json", mb, 0o644)
}

func doJSON(ctx context.Context, c *http.Client, token, method, u string, body any) ([]byte, int, string, error) {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return nil, 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, 0, "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return data, resp.StatusCode, resp.Header.Get("Content-Type"), err
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run downloader.go <organisationsnummer> [more...]")
		os.Exit(2)
	}
	dataRoot := os.Getenv("BV_DATA_ROOT")
	if dataRoot == "" {
		dataRoot = "/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/sweden"
	}
	ctx := context.Background()
	c := &http.Client{Timeout: 60 * time.Second}

	token, err := getToken(ctx, c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "token error:", err)
		os.Exit(1)
	}
	fmt.Println("got OAuth2 token")

	// NOTE: confirm the exact request-body field name at registration; "identitetsbeteckning"
	// (the standard Swedish term for an org/identity number) is the expected key.
	for _, orgnr := range os.Args[1:] {
		reqBody := map[string]string{"identitetsbeteckning": orgnr}

		// 1) base data
		data, status, ct, err := doJSON(ctx, c, token, http.MethodPost, apiBase+"/organisationer", reqBody)
		if err != nil {
			fmt.Fprintln(os.Stderr, orgnr, "organisationer error:", err)
			continue
		}
		out := filepath.Join(dataRoot, "raw", "api", "bolagsverket_vdm_org_"+orgnr+".json")
		_ = saveWithMeta(out, apiBase+"/organisationer", status, ct, data)
		fmt.Printf("%s organisationer -> HTTP %d (%s)\n", orgnr, status, out)

		// 2) document list (annual reports)
		dl, status, ct, err := doJSON(ctx, c, token, http.MethodPost, apiBase+"/dokumentlista", reqBody)
		if err != nil {
			fmt.Fprintln(os.Stderr, orgnr, "dokumentlista error:", err)
			continue
		}
		out = filepath.Join(dataRoot, "raw", "api", "bolagsverket_vdm_doclist_"+orgnr+".json")
		_ = saveWithMeta(out, apiBase+"/dokumentlista", status, ct, dl)
		fmt.Printf("%s dokumentlista -> HTTP %d (%s)\n", orgnr, status, out)

		// 3) download each document (iXBRL ZIP). Parse ids from dl per the documented schema.
		var listed struct {
			Dokument []struct {
				DokumentId string `json:"dokumentId"`
			} `json:"dokument"`
		}
		_ = json.Unmarshal(dl, &listed)
		for _, d := range listed.Dokument {
			if d.DokumentId == "" {
				continue
			}
			docURL := apiBase + "/dokument/" + url.PathEscape(d.DokumentId)
			body, st, ct, err := doJSON(ctx, c, token, http.MethodGet, docURL, nil)
			if err != nil {
				fmt.Fprintln(os.Stderr, orgnr, "dokument error:", err)
				continue
			}
			out = filepath.Join(dataRoot, "raw", "bulk", "bolagsverket_vdm_doc_"+d.DokumentId+".zip")
			_ = saveWithMeta(out, docURL, st, ct, body)
			fmt.Printf("%s dokument %s -> HTTP %d (%s)\n", orgnr, d.DokumentId, st, out)
			time.Sleep(500 * time.Millisecond)
		}
	}
}
