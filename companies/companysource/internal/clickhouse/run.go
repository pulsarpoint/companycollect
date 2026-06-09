package clickhouse

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

type MigrationOptions struct {
	RunDir          string
	Database        string
	Out             string
	DownOut         string
	ConfigYAML      []byte
	ClickHouseLocal string
}

type MigrationResult struct {
	UpPath   string
	DownPath string
}

type ImportOptions struct {
	RunDir              string
	Database            string
	ClickHouseNativeURL string
	SourceExportID      string
	ClickHouseImage     string
	DockerMount         string
	ConfigYAML          []byte
}

type ImportResult struct {
	ImportedTables int
	Tables         []string
}

func GenerateMigrationFiles(opts MigrationOptions) (MigrationResult, error) {
	if strings.TrimSpace(opts.RunDir) == "" {
		return MigrationResult{}, errors.New("run dir is required")
	}
	if strings.TrimSpace(opts.Out) == "" {
		return MigrationResult{}, errors.New("out is required")
	}
	if strings.TrimSpace(opts.DownOut) == "" {
		return MigrationResult{}, errors.New("down-out is required")
	}
	cfg, err := ParseConfig(opts.ConfigYAML)
	if err != nil {
		return MigrationResult{}, err
	}
	if database := strings.TrimSpace(opts.Database); database != "" {
		cfg.Database = database
	}
	up, down, err := GenerateMigrations(cfg, opts.RunDir, ClickHouseLocalDescriber{Binary: opts.ClickHouseLocal})
	if err != nil {
		return MigrationResult{}, err
	}
	if err := os.WriteFile(opts.Out, []byte(up), 0o644); err != nil {
		return MigrationResult{}, errors.Wrap(err, "write up migration")
	}
	if err := os.WriteFile(opts.DownOut, []byte(down), 0o644); err != nil {
		return MigrationResult{}, errors.Wrap(err, "write down migration")
	}
	return MigrationResult{UpPath: opts.Out, DownPath: opts.DownOut}, nil
}

func ImportRun(opts ImportOptions) (ImportResult, error) {
	if strings.TrimSpace(opts.RunDir) == "" {
		return ImportResult{}, errors.New("run dir is required")
	}
	if strings.TrimSpace(opts.SourceExportID) == "" {
		return ImportResult{}, errors.New("source-export-id is required")
	}
	if strings.TrimSpace(opts.ClickHouseNativeURL) == "" {
		return ImportResult{}, errors.New("clickhouse-native-url is required")
	}
	cfg, err := ParseConfig(opts.ConfigYAML)
	if err != nil {
		return ImportResult{}, err
	}
	if database := strings.TrimSpace(opts.Database); database != "" {
		cfg.Database = database
	}
	if cfg.Database == "" {
		return ImportResult{}, errors.New("database is required")
	}

	tables := sortedTableNames(cfg.Tables)
	imported := make([]string, 0, len(tables))
	for _, name := range tables {
		table := cfg.Tables[name]
		if err := TruncateTargetTable(opts.ClickHouseNativeURL, opts.ClickHouseImage, cfg.Database, table.Table); err != nil {
			return ImportResult{}, errors.Wrapf(err, "truncate table %s", table.Table)
		}
		if err := ExecuteNativeImport(NativeImportOptions{
			Database:        cfg.Database,
			Table:           table,
			ParquetPath:     filepath.Join(opts.RunDir, table.Parquet),
			SourceExportID:  opts.SourceExportID,
			NativeURL:       opts.ClickHouseNativeURL,
			DockerMount:     opts.DockerMount,
			ClickHouseImage: opts.ClickHouseImage,
		}); err != nil {
			return ImportResult{}, errors.Wrapf(err, "import table %s", table.Table)
		}
		imported = append(imported, table.Table)
	}
	sort.Strings(imported)
	return ImportResult{ImportedTables: len(imported), Tables: imported}, nil
}
