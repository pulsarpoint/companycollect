package elastic_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/cvr/elastic"
)

func TestClientStreamRecordsUsesInitialSearchThenScroll(t *testing.T) {
	var initialSearchSeen bool
	var scrollSeen bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		username, password, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "cvr-user", username)
		require.Equal(t, "cvr-pass", password)

		switch r.URL.Path {
		case "/cvr-permanent/virksomhed/_search":
			initialSearchSeen = true
			require.Equal(t, "1m", r.URL.Query().Get("scroll"))
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, float64(1), body["size"])
			writeSearchResponse(t, w, "scroll-1", "12345678", "First ApS")
		case "/_search/scroll":
			scrollSeen = true
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "1m", body["scroll"])
			require.Equal(t, "scroll-1", body["scroll_id"])
			writeSearchResponse(t, w, "", "87654321", "Second ApS")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := elastic.NewClient(server.Client())
	var seen []string
	result, err := client.StreamRecords(context.Background(), elastic.StreamInput{
		SourceURL: server.URL + "/cvr-permanent/virksomhed/_search",
		ScrollURL: server.URL + "/_search/scroll",
		Scroll:    "1m",
		PageSize:  1,
		Limit:     2,
		Username:  "cvr-user",
		Password:  "cvr-pass",
	}, func(record elastic.Record) error {
		seen = append(seen, record.CVRNumber)
		return nil
	})

	require.NoError(t, err)
	require.True(t, initialSearchSeen)
	require.True(t, scrollSeen)
	require.Equal(t, int32(2), result.RowsSeen)
	require.Equal(t, []string{"12345678", "87654321"}, seen)
}

func TestClientStreamRecordsRequiresCredentials(t *testing.T) {
	client := elastic.NewClient(http.DefaultClient)

	_, err := client.StreamRecords(context.Background(), elastic.StreamInput{
		SourceURL: "https://distribution.virk.dk/cvr-permanent/virksomhed/_search",
	}, func(record elastic.Record) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "cvr distribution credentials are not configured")
}

func writeSearchResponse(t *testing.T, w http.ResponseWriter, scrollID, cvrNumber, name string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]any{
		"_scroll_id": scrollID,
		"hits": map[string]any{
			"hits": []map[string]any{
				{
					"_id": cvrNumber,
					"_source": map[string]any{
						"Vrvirksomhed": map[string]any{
							"cvrNummer": cvrNumber,
							"virksomhedMetadata": map[string]any{
								"nyesteNavn": map[string]any{"navn": name},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}
