package worker

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/tech"
)

func BenchmarkProcessTechPage(b *testing.B) {
	matcher, err := tech.NewFastMatcher()
	if err != nil {
		b.Fatal(err)
	}

	for _, size := range []int{70 << 10, 512 << 10} {
		for _, primary := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/primary=%t", size, primary), func(b *testing.B) {
				filler := strings.Repeat(`<div class="row"><p>lorem ipsum dolor sit amet consectetur</p></div>`, size/70)
				body := `<html><head><meta name="generator" content="WordPress 6.4.2">` +
					`<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script></head><body>` + filler + `</body></html>`
				raw := gzWarc("HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n" + body)
				getter := multiGetter{"f.warc.gz:0": raw}
				item := model.WorklistItem{RootDomain: "example.com", URL: "https://example.com/", WarcFilename: "f.warc.gz", Length: int64(len(raw)), Primary: primary}
				cfg := ShardConfig{Mode: "tech", TechMaxBytes: 128 << 10, Tech: matcher}
				b.ReportAllocs()
				b.SetBytes(int64(len(body)))
				b.ResetTimer()
				for b.Loop() {
					if _, err := processPage(context.Background(), getter, cfg, item, &chunkStats{}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
