package clickhouse

import (
	"bytes"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

const DefaultClickHouseImage = "clickhouse/clickhouse-server:26.5"
const DefaultCompanycollectHostIP = "100.85.212.113"

type NativeImportOptions struct {
	Database        string
	Table           TableConfig
	ParquetPath     string
	SourceExportID  string
	NativeURL       string
	DockerMount     string
	ClickHouseImage string
}

type ClickHouseTarget struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

func BuildNativeSelectSQL(table TableConfig, parquetPath string, sourceExportID string) (string, error) {
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

func BuildNativeInsertSQL(database string, table string) string {
	return "INSERT INTO " + quoteIdent(database) + "." + quoteIdent(table) + " FORMAT Native"
}

func BuildTruncateSQL(database string, table string) string {
	return "TRUNCATE TABLE " + quoteIdent(database) + "." + quoteIdent(table)
}

func ParseClickHouseNativeURL(rawURL string) (ClickHouseTarget, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ClickHouseTarget{}, errors.New("clickhouse-native-url is required")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ClickHouseTarget{}, errors.Wrap(err, "parse clickhouse native url")
	}
	if parsed.Scheme != "clickhouse" {
		return ClickHouseTarget{}, errors.Errorf("clickhouse native url must use clickhouse scheme, got %q", parsed.Scheme)
	}
	target := ClickHouseTarget{
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
	if target.Database == "" {
		target.Database = strings.TrimPrefix(parsed.EscapedPath(), "/")
	}
	if target.Host == "" {
		return ClickHouseTarget{}, errors.New("clickhouse native url host is required")
	}
	if target.Username == "" {
		target.Username = "default"
	}
	if target.Database == "" {
		return ClickHouseTarget{}, errors.New("clickhouse native url database is required")
	}
	return target, nil
}

func ClickHouseClientDockerArgs(image string, target ClickHouseTarget, query string) []string {
	args := []string{
		"run", "--rm", "-i",
		"--add-host", "host.docker.internal:host-gateway",
	}
	if target.Host == "companycollect" {
		hostIP := strings.TrimSpace(os.Getenv("COMPANYCOLLECT_HOST_IP"))
		if hostIP == "" {
			hostIP = DefaultCompanycollectHostIP
		}
		args = append(args, "--add-host", "companycollect:"+hostIP)
	}
	args = append(args,
		image,
		"clickhouse-client",
		"--host", target.Host,
		"--port", target.Port,
		"--user", target.Username,
		"--database", target.Database,
		"--query", query,
	)
	if target.Password != "" {
		args = append(args[:len(args)-2], "--password", target.Password, args[len(args)-2], args[len(args)-1])
	}
	return args
}

func ExecuteClickHouseQuery(nativeURL string, image string, query string) error {
	target, err := ParseClickHouseNativeURL(nativeURL)
	if err != nil {
		return err
	}
	image = strings.TrimSpace(image)
	if image == "" {
		image = DefaultClickHouseImage
	}
	cmd := exec.Command(
		"docker",
		ClickHouseClientDockerArgs(image, target, query)...,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "run clickhouse query stderr=%s", strings.TrimSpace(stderr.String()))
	}
	return nil
}

func TruncateTargetTable(nativeURL string, image string, database string, table string) error {
	if database == "" {
		return errors.New("database is required")
	}
	if table == "" {
		return errors.New("table name is required")
	}
	return ExecuteClickHouseQuery(nativeURL, image, BuildTruncateSQL(database, table))
}

func ExecuteNativeImport(opts NativeImportOptions) error {
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
	selectSQL, err := BuildNativeSelectSQL(opts.Table, absolutePath, opts.SourceExportID)
	if err != nil {
		return err
	}

	image := strings.TrimSpace(opts.ClickHouseImage)
	if image == "" {
		image = DefaultClickHouseImage
	}
	mount := strings.TrimSpace(opts.DockerMount)
	if mount == "" {
		mount = DockerMountRoot(absolutePath)
	}
	target, err := ParseClickHouseNativeURL(opts.NativeURL)
	if err != nil {
		return err
	}

	localCmd := exec.Command(
		"docker", "run", "--rm",
		"-v", mount+":"+mount+":ro",
		image,
		"clickhouse-local",
		"--query", selectSQL,
	)
	clientCmd := exec.Command(
		"docker",
		ClickHouseClientDockerArgs(image, target, BuildNativeInsertSQL(opts.Database, opts.Table.Table))...,
	)

	reader, writer := io.Pipe()
	localCmd.Stdout = writer
	clientCmd.Stdin = reader

	return RunNativePipeline(localCmd, clientCmd, reader, writer)
}

func RunNativePipeline(localCmd *exec.Cmd, clientCmd *exec.Cmd, reader *io.PipeReader, writer *io.PipeWriter) error {
	var localStderr bytes.Buffer
	var clientStderr bytes.Buffer
	localCmd.Stderr = &localStderr
	clientCmd.Stderr = &clientStderr

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

	type processResult struct {
		err    error
		stderr string
	}

	localDone := make(chan processResult, 1)
	clientDone := make(chan processResult, 1)

	go func() {
		err := localCmd.Wait()
		if err != nil {
			_ = writer.CloseWithError(err)
		} else {
			_ = writer.Close()
		}
		localDone <- processResult{
			err:    err,
			stderr: strings.TrimSpace(localStderr.String()),
		}
	}()

	go func() {
		err := clientCmd.Wait()
		if err != nil {
			_ = reader.CloseWithError(err)
		} else {
			_ = reader.Close()
		}
		clientDone <- processResult{
			err:    err,
			stderr: strings.TrimSpace(clientStderr.String()),
		}
	}()

	clientResult := <-clientDone
	localResult := <-localDone

	if clientResult.err != nil {
		return errors.Wrapf(clientResult.err, "clickhouse-client import failed stderr=%s", clientResult.stderr)
	}
	if localResult.err != nil {
		return errors.Wrapf(localResult.err, "clickhouse-local parquet reader failed stderr=%s", localResult.stderr)
	}
	return nil
}

func DockerMountRoot(absolutePath string) string {
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
