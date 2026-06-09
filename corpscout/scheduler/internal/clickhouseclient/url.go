package clickhouseclient

import (
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
)

type Target struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

func ParseNativeURL(rawURL string) (Target, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return Target{}, errors.Wrap(err, "parse clickhouse native url")
	}
	if parsed.Scheme != "clickhouse" {
		return Target{}, errors.Errorf("clickhouse native url must use clickhouse scheme, got %q", parsed.Scheme)
	}

	target := Target{
		Host:     parsed.Hostname(),
		Port:     parsed.Port(),
		Username: parsed.Query().Get("username"),
		Password: parsed.Query().Get("password"),
		Database: parsed.Query().Get("database"),
	}
	if parsed.User != nil {
		if target.Username == "" {
			target.Username = parsed.User.Username()
		}
		if password, ok := parsed.User.Password(); ok && target.Password == "" {
			target.Password = password
		}
	}
	if target.Port == "" {
		target.Port = "9000"
	}
	if target.Username == "" {
		target.Username = "default"
	}
	if target.Database == "" {
		target.Database = strings.TrimPrefix(parsed.EscapedPath(), "/")
	}
	if target.Host == "" {
		return Target{}, errors.New("clickhouse native url host is required")
	}
	if target.Database == "" {
		return Target{}, errors.New("clickhouse native url database is required")
	}
	return target, nil
}
