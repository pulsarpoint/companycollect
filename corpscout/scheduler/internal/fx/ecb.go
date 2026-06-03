package fx

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	DefaultProvider           = "ecb"
	DefaultDailySourceURL     = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
	DefaultMaxSourceFileBytes = int64(10 << 20)
)

type DownloadedRateFile struct {
	Body               []byte
	ContentSHA256      string
	ContentLengthBytes int64
	ContentType        string
	ETag               string
	LastModified       string
	SourceURL          string
}

type RateSheet struct {
	Provider      string
	RateDate      string
	BaseCurrency  string
	ContentSHA256 string
	SourceURL     string
	Rates         map[string]string
}

func (s RateSheet) Currencies() []string {
	currencies := make([]string, 0, len(s.Rates))
	for currency := range s.Rates {
		currencies = append(currencies, currency)
	}
	sort.Strings(currencies)
	return currencies
}

func DownloadRateFile(ctx context.Context, httpClient *http.Client, sourceURL string, maxBytes int64) (DownloadedRateFile, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxSourceFileBytes
	}
	sourceURL = strings.TrimSpace(sourceURL)
	if sourceURL == "" {
		return DownloadedRateFile{}, errors.New("exchange rate source url is required")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return DownloadedRateFile{}, errors.Wrap(err, "build exchange rate source request")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return DownloadedRateFile{}, errors.Wrap(err, "download exchange rate source")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return DownloadedRateFile{}, errors.Newf("download exchange rate source returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return DownloadedRateFile{}, errors.Wrap(err, "read exchange rate source")
	}
	if int64(len(body)) > maxBytes {
		return DownloadedRateFile{}, errors.Newf("exchange rate source exceeds %d bytes", maxBytes)
	}
	sum := sha256.Sum256(body)
	return DownloadedRateFile{
		Body:               body,
		ContentSHA256:      hex.EncodeToString(sum[:]),
		ContentLengthBytes: int64(len(body)),
		ContentType:        resp.Header.Get("Content-Type"),
		ETag:               resp.Header.Get("ETag"),
		LastModified:       resp.Header.Get("Last-Modified"),
		SourceURL:          sourceURL,
	}, nil
}

type ecbEnvelope struct {
	Cube ecbOuterCube `xml:"Cube"`
}

type ecbOuterCube struct {
	Days []ecbDayCube `xml:"Cube"`
}

type ecbDayCube struct {
	Time  string        `xml:"time,attr"`
	Rates []ecbRateCube `xml:"Cube"`
}

type ecbRateCube struct {
	Currency string `xml:"currency,attr"`
	Rate     string `xml:"rate,attr"`
}

func ParseECBDailyRateSheet(downloaded DownloadedRateFile) (RateSheet, error) {
	var envelope ecbEnvelope
	if err := xml.NewDecoder(bytes.NewReader(downloaded.Body)).Decode(&envelope); err != nil {
		return RateSheet{}, errors.Wrap(err, "parse ECB daily XML")
	}

	var selected *ecbDayCube
	for i := range envelope.Cube.Days {
		if strings.TrimSpace(envelope.Cube.Days[i].Time) != "" {
			selected = &envelope.Cube.Days[i]
			break
		}
	}
	if selected == nil {
		return RateSheet{}, errors.New("ecb daily feed does not contain a dated rate cube")
	}

	rateDate := strings.TrimSpace(selected.Time)
	if _, err := time.Parse("2006-01-02", rateDate); err != nil {
		return RateSheet{}, errors.Wrap(err, "parse ECB rate date")
	}
	if len(selected.Rates) == 0 {
		return RateSheet{}, errors.New("ecb daily feed contained no exchange rates")
	}

	rates := map[string]string{"EUR": "1.000000000000"}
	for _, rawRate := range selected.Rates {
		currency := strings.ToUpper(strings.TrimSpace(rawRate.Currency))
		if len(currency) != 3 {
			return RateSheet{}, errors.Newf("invalid ECB currency %q", rawRate.Currency)
		}
		normalized, err := normalizeDecimalRate(rawRate.Rate)
		if err != nil {
			return RateSheet{}, errors.Wrapf(err, "parse ECB rate for %s", currency)
		}
		rates[currency] = normalized
	}

	return RateSheet{
		Provider:      DefaultProvider,
		RateDate:      rateDate,
		BaseCurrency:  "EUR",
		ContentSHA256: downloaded.ContentSHA256,
		SourceURL:     downloaded.SourceURL,
		Rates:         rates,
	}, nil
}

func normalizeDecimalRate(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("rate is empty")
	}
	rat, ok := new(big.Rat).SetString(value)
	if !ok || rat.Sign() <= 0 {
		return "", errors.New("rate must be a positive decimal")
	}
	return rat.FloatString(12), nil
}
