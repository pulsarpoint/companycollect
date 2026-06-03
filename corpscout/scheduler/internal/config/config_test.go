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
	if cfg.BRREGTermTranslationResultSubject != "brreg.translation.terms.result" {
		t.Errorf("want default BRREG term translation result subject, got %s", cfg.BRREGTermTranslationResultSubject)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("want default log level info, got %s", cfg.LogLevel)
	}
	if cfg.FXECBSourceURL != "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml" {
		t.Errorf("want default ECB FX URL, got %s", cfg.FXECBSourceURL)
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
	t.Setenv("CORPSCOUT_BRREG_TERM_TRANSLATION_RESULT_SUBJECT", "custom.brreg.translation.terms.result")
	t.Setenv("CORPSCOUT_LOG_LEVEL", "debug")
	t.Setenv("CORPSCOUT_FX_ECB_DAILY_URL", "https://example.test/ecb.xml")

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
	if cfg.BRREGTermTranslationResultSubject != "custom.brreg.translation.terms.result" {
		t.Errorf("want override BRREG term translation result subject, got %s", cfg.BRREGTermTranslationResultSubject)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("want override log level debug, got %s", cfg.LogLevel)
	}
	if cfg.FXECBSourceURL != "https://example.test/ecb.xml" {
		t.Errorf("want override ECB FX URL, got %s", cfg.FXECBSourceURL)
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
