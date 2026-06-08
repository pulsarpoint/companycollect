package main

import (
	"bytes"
	"io"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

const defaultClickHouseImage = "clickhouse/clickhouse-server:25.5"

type TableConfig struct {
	Parquet       string            `yaml:"parquet"`
	Table         string            `yaml:"table"`
	InjectColumns map[string]string `yaml:"inject_columns"`
}

type NativeImportOptions struct {
	Database        string
	Table           TableConfig
	ParquetPath     string
	SourceExportID  string
	ComposeFile     string
	DockerMount     string
	ClickHouseImage string
}

func buildNativeSelectSQL(table TableConfig, parquetPath string, sourceExportID string) (string, error) {
	var injected []string
	for name := range table.InjectColumns {
		switch name {
		case "source_export_id", "ingested_at":
			injected = append(injected, name)
		default:
			return "", errors.Errorf("table %s unknown injected column %s", table.Table, name)
		}
	}
	sort.Strings(injected)

	selectParts := []string{"*"}
	for _, name := range injected {
		switch name {
		case "ingested_at":
			selectParts = append(selectParts, "now64(3) AS "+quoteIdent(name))
		case "source_export_id":
			selectParts = append(selectParts, "toUUID("+clickHouseStringLiteral(sourceExportID)+") AS "+quoteIdent(name))
		}
	}

	return "SELECT " + strings.Join(selectParts, ", ") + "\n" +
		"FROM file(" + clickHouseStringLiteral(parquetPath) + ", Parquet)\n" +
		"FORMAT Native", nil
}

func buildNativeInsertSQL(database string, table string) string {
	return "INSERT INTO " + quoteIdent(database) + "." + quoteIdent(table) + " FORMAT Native"
}

func executeNativeImport(opts NativeImportOptions) error {
	if opts.Database == "" {
		return errors.New("database is required")
	}
	if opts.Table.Table == "" {
		return errors.New("table name is required")
	}
	absolutePath, err := filepath.Abs(opts.ParquetPath)
	if err != nil {
		return errors.Wrap(err, "resolve parquet path")
	}
	selectSQL, err := buildNativeSelectSQL(opts.Table, absolutePath, opts.SourceExportID)
	if err != nil {
		return err
	}

	image := strings.TrimSpace(opts.ClickHouseImage)
	if image == "" {
		image = defaultClickHouseImage
	}
	mount := strings.TrimSpace(opts.DockerMount)
	if mount == "" {
		mount = dockerMountRoot(absolutePath)
	}
	composeFile := strings.TrimSpace(opts.ComposeFile)
	if composeFile == "" {
		composeFile = "../docker-compose.yml"
	}

	localCmd := exec.Command(
		"docker", "run", "--rm",
		"-v", mount+":"+mount+":ro",
		image,
		"clickhouse-local",
		"--query", selectSQL,
	)
	clientCmd := exec.Command(
		"docker", "compose", "-f", composeFile,
		"exec", "-T", "clickhouse",
		"clickhouse-client",
		"--query", buildNativeInsertSQL(opts.Database, opts.Table.Table),
	)

	var localStderr bytes.Buffer
	var clientStderr bytes.Buffer
	localCmd.Stderr = &localStderr
	clientCmd.Stderr = &clientStderr

	reader, writer := io.Pipe()
	localCmd.Stdout = writer
	clientCmd.Stdin = reader

	if err := clientCmd.Start(); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return errors.Wrap(err, "start clickhouse-client import")
	}
	if err := localCmd.Start(); err != nil {
		_ = writer.CloseWithError(err)
		_ = reader.Close()
		_ = clientCmd.Wait()
		return errors.Wrap(err, "start clickhouse-local parquet reader")
	}

	localErr := localCmd.Wait()
	if localErr != nil {
		_ = writer.CloseWithError(localErr)
	} else {
		_ = writer.Close()
	}
	clientErr := clientCmd.Wait()
	_ = reader.Close()

	if localErr != nil {
		return errors.Wrapf(localErr, "clickhouse-local parquet reader failed stderr=%s", strings.TrimSpace(localStderr.String()))
	}
	if clientErr != nil {
		return errors.Wrapf(clientErr, "clickhouse-client import failed stderr=%s", strings.TrimSpace(clientStderr.String()))
	}
	return nil
}

func dockerMountRoot(absolutePath string) string {
	cleaned := filepath.Clean(absolutePath)
	if strings.HasPrefix(cleaned, "/Users/") || cleaned == "/Users" {
		return "/Users"
	}
	volume := filepath.VolumeName(cleaned)
	if volume != "" {
		return volume + string(filepath.Separator)
	}
	return "/"
}

func quoteIdent(value string) string {
	escaped := strings.NewReplacer(
		`\`, `\\`,
		"`", "\\`",
	).Replace(value)
	return "`" + escaped + "`"
}

func clickHouseStringLiteral(value string) string {
	escaped := strings.NewReplacer(
		`\`, `\\`,
		`'`, `\'`,
	).Replace(value)
	return "'" + escaped + "'"
}
