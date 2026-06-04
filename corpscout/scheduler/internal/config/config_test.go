package config_test

import (
	"testing"
	"time"

	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
)

func TestLoad_defaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("CORPSCOUT_S3_ACCESS_KEY", "test-access")
	t.Setenv("CORPSCOUT_S3_SECRET_KEY", "test-secret")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ListenAddr != ":8090" {
		t.Errorf("want :8090, got %s", cfg.ListenAddr)
	}
	if cfg.CrawlServiceURL != "http://localhost:8096" {
		t.Errorf("want default crawl service URL, got %s", cfg.CrawlServiceURL)
	}
	if cfg.NATSURL != "" {
		t.Errorf("want empty default NATS URL, got %s", cfg.NATSURL)
	}
	if cfg.NATSRequestTimeout != 180*time.Second {
		t.Errorf("want default NATS request timeout 180s, got %s", cfg.NATSRequestTimeout)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("want default log level info, got %s", cfg.LogLevel)
	}
	if cfg.FXECBSourceURL != "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml" {
		t.Errorf("want default ECB FX URL, got %s", cfg.FXECBSourceURL)
	}
	if cfg.CVRSourceURL != "https://distribution.virk.dk/cvr-permanent/virksomhed/_search" {
		t.Errorf("want default CVR source URL, got %s", cfg.CVRSourceURL)
	}
	if cfg.CVRScrollURL != "https://distribution.virk.dk/_search/scroll" {
		t.Errorf("want default CVR scroll URL, got %s", cfg.CVRScrollURL)
	}
	if cfg.CVRScroll != "1m" {
		t.Errorf("want default CVR scroll value 1m, got %s", cfg.CVRScroll)
	}
	if cfg.AriregisterSourceURL != "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__yldandmed.json.zip" {
		t.Errorf("want default Ariregister general data URL, got %s", cfg.AriregisterSourceURL)
	}
}

func TestLoad_overrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("CORPSCOUT_LISTEN_ADDR", ":9000")
	t.Setenv("CORPSCOUT_S3_ACCESS_KEY", "override-access")
	t.Setenv("CORPSCOUT_S3_SECRET_KEY", "override-secret")
	t.Setenv("CORPSCOUT_CRAWL_SERVICE_URL", "http://crawl-service:8096")
	t.Setenv("CORPSCOUT_NATS_URL", "nats://companycollect:4222")
	t.Setenv("CORPSCOUT_NATS_REQUEST_TIMEOUT_SECONDS", "240")
	t.Setenv("CORPSCOUT_LOG_LEVEL", "debug")
	t.Setenv("CORPSCOUT_FX_ECB_DAILY_URL", "https://example.test/ecb.xml")
	t.Setenv("CORPSCOUT_CVR_DISTRIBUTION_SOURCE_URL", "https://example.test/cvr/_search")
	t.Setenv("CORPSCOUT_CVR_DISTRIBUTION_SCROLL_URL", "https://example.test/_search/scroll")
	t.Setenv("CORPSCOUT_CVR_DISTRIBUTION_SCROLL", "1m")
	t.Setenv("CORPSCOUT_CVR_DISTRIBUTION_USERNAME", "cvr-user")
	t.Setenv("CORPSCOUT_CVR_DISTRIBUTION_PASSWORD", "cvr-pass")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ListenAddr != ":9000" {
		t.Errorf("want :9000, got %s", cfg.ListenAddr)
	}
	if cfg.CrawlServiceURL != "http://crawl-service:8096" {
		t.Errorf("want override crawl service URL, got %s", cfg.CrawlServiceURL)
	}
	if cfg.NATSURL != "nats://companycollect:4222" {
		t.Errorf("want override NATS URL, got %s", cfg.NATSURL)
	}
	if cfg.NATSRequestTimeout != 240*time.Second {
		t.Errorf("want override NATS request timeout 240s, got %s", cfg.NATSRequestTimeout)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("want override log level debug, got %s", cfg.LogLevel)
	}
	if cfg.FXECBSourceURL != "https://example.test/ecb.xml" {
		t.Errorf("want override ECB FX URL, got %s", cfg.FXECBSourceURL)
	}
	if cfg.CVRSourceURL != "https://example.test/cvr/_search" {
		t.Errorf("want override CVR source URL, got %s", cfg.CVRSourceURL)
	}
	if cfg.CVRScrollURL != "https://example.test/_search/scroll" {
		t.Errorf("want override CVR scroll URL, got %s", cfg.CVRScrollURL)
	}
	if cfg.CVRScroll != "1m" {
		t.Errorf("want override CVR scroll value, got %s", cfg.CVRScroll)
	}
	if cfg.CVRUsername != "cvr-user" {
		t.Errorf("want override CVR username, got %s", cfg.CVRUsername)
	}
	if cfg.CVRPassword != "cvr-pass" {
		t.Errorf("want override CVR password, got %s", cfg.CVRPassword)
	}
}

func TestLoad_rejectsInvalidNATSRequestTimeout(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("CORPSCOUT_S3_ACCESS_KEY", "test-access")
	t.Setenv("CORPSCOUT_S3_SECRET_KEY", "test-secret")
	t.Setenv("CORPSCOUT_NATS_REQUEST_TIMEOUT_SECONDS", "0")

	_, err := config.Load()

	if err == nil {
		t.Fatal("expected invalid NATS request timeout error")
	}
}

func TestLoad_rejectsInvalidLogLevel(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("CORPSCOUT_S3_ACCESS_KEY", "test-access")
	t.Setenv("CORPSCOUT_S3_SECRET_KEY", "test-secret")
	t.Setenv("CORPSCOUT_LOG_LEVEL", "verbose")

	_, err := config.Load()

	if err == nil {
		t.Fatal("expected invalid log level error")
	}
}

func TestLoad_requiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("CORPSCOUT_DATABASE_URL", "")
	t.Setenv("CORPSCOUT_S3_ACCESS_KEY", "test-access")
	t.Setenv("CORPSCOUT_S3_SECRET_KEY", "test-secret")

	_, err := config.Load()

	if err == nil {
		t.Fatal("expected missing database URL error")
	}
}

func TestLoad_requiresS3Credentials(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("CORPSCOUT_S3_ACCESS_KEY", "")
	t.Setenv("CORPSCOUT_S3_SECRET_KEY", "")

	_, err := config.Load()

	if err == nil {
		t.Fatal("expected missing S3 credential error")
	}
}
