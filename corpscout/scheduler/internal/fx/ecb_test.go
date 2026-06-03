package fx

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const validECBXML = `<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01"
                 xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
  <Cube>
    <Cube time="2026-06-03">
      <Cube currency="USD" rate="1.1599"/>
      <Cube currency="NOK" rate="10.7075"/>
      <Cube currency="GBP" rate="0.84210"/>
    </Cube>
  </Cube>
</gesmes:Envelope>`

func TestParseECBDailyRates(t *testing.T) {
	downloaded := DownloadedRateFile{
		Body:               []byte(validECBXML),
		ContentSHA256:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ContentLengthBytes: int64(len(validECBXML)),
		SourceURL:          "https://example.test/ecb.xml",
	}

	sheet, err := ParseECBDailyRateSheet(downloaded)

	require.NoError(t, err)
	require.Equal(t, DefaultProvider, sheet.Provider)
	require.Equal(t, "2026-06-03", sheet.RateDate)
	require.Equal(t, "EUR", sheet.BaseCurrency)
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", sheet.ContentSHA256)
	require.Equal(t, "1.000000000000", sheet.Rates["EUR"])
	require.Equal(t, "1.159900000000", sheet.Rates["USD"])
	require.Equal(t, "10.707500000000", sheet.Rates["NOK"])
	require.Equal(t, []string{"EUR", "GBP", "NOK", "USD"}, sheet.Currencies())
}

func TestParseECBDailyRatesRejectsMissingDay(t *testing.T) {
	_, err := ParseECBDailyRateSheet(DownloadedRateFile{Body: []byte(`<Envelope><Cube><Cube></Cube></Cube></Envelope>`)})
	require.ErrorContains(t, err, "ecb daily feed does not contain a dated rate cube")
}

func TestParseECBDailyRatesRejectsInvalidRate(t *testing.T) {
	xml := `<Envelope><Cube><Cube time="2026-06-03"><Cube currency="USD" rate="abc"/></Cube></Cube></Envelope>`
	_, err := ParseECBDailyRateSheet(DownloadedRateFile{Body: []byte(xml)})
	require.ErrorContains(t, err, "parse ECB rate for USD")
}

func TestParseECBDailyRatesRejectsEmptyRates(t *testing.T) {
	xml := `<Envelope><Cube><Cube time="2026-06-03"></Cube></Cube></Envelope>`
	_, err := ParseECBDailyRateSheet(DownloadedRateFile{Body: []byte(xml)})
	require.ErrorContains(t, err, "ecb daily feed contained no exchange rates")
}

func TestDownloadRateFileCapturesHashAndHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("ETag", `"fx-test"`)
		w.Header().Set("Last-Modified", "Wed, 03 Jun 2026 12:00:00 GMT")
		_, _ = w.Write([]byte(validECBXML))
	}))
	t.Cleanup(server.Close)

	downloaded, err := DownloadRateFile(t.Context(), server.Client(), server.URL, DefaultMaxSourceFileBytes)

	require.NoError(t, err)
	require.Equal(t, server.URL, downloaded.SourceURL)
	require.Equal(t, "application/xml", downloaded.ContentType)
	require.Equal(t, `"fx-test"`, downloaded.ETag)
	require.Equal(t, "Wed, 03 Jun 2026 12:00:00 GMT", downloaded.LastModified)
	require.Len(t, downloaded.ContentSHA256, 64)
	require.Equal(t, int64(len(validECBXML)), downloaded.ContentLengthBytes)
}

func TestDownloadRateFileRejectsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	_, err := DownloadRateFile(t.Context(), server.Client(), server.URL, DefaultMaxSourceFileBytes)

	require.ErrorContains(t, err, "download exchange rate source returned status 503")
}
