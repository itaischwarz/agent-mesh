// Command rag-connector syncs documents from configured sources (local
// folders, Google Drive, Confluence) into the raw document store.
//
//	go run ./cmd/rag-connector -config cmd/rag-connector/example.yaml
//	go run ./cmd/rag-connector -config my.yaml -source eng-wiki -full
//
// Each run is incremental from the last saved cursor; -full also detects
// deletions. It exits non-zero if any source failed, so it can run as a
// Kubernetes Job or cron entry.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/itaischwarz/agent-mesh/internal/rag/connector"
	"github.com/itaischwarz/agent-mesh/internal/rag/connector/confluence"
	"github.com/itaischwarz/agent-mesh/internal/rag/connector/drive"
	"github.com/itaischwarz/agent-mesh/internal/rag/connector/fs"
)

func main() {
	configPath := flag.String("config", "rag-connector.yaml", "config file")
	only := flag.String("source", "", "sync only the source with this id")
	full := flag.Bool("full", false, "force a full sync, which also detects deletions")
	asJSON := flag.Bool("json", false, "print reports as JSON")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *configPath, *only, *full, *asJSON); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, configPath, only string, full, asJSON bool) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	state := &connector.FileStateStore{Dir: cfg.StateDir}
	sink := &connector.DirSink{Dir: cfg.SinkDir}

	var failed []string
	matched := false
	for _, src := range cfg.Sources {
		if only != "" && src.ID != only {
			continue
		}
		matched = true
		rep, err := syncSource(ctx, src, state, sink, logger, full)
		printReport(src.ID, rep, err, asJSON)
		if err != nil || (rep != nil && rep.Failed > 0) {
			failed = append(failed, src.ID)
		}
	}
	if !matched {
		return fmt.Errorf("no source with id %q in %s", only, configPath)
	}
	if len(failed) > 0 {
		return fmt.Errorf("sources with errors: %v", failed)
	}
	return nil
}

func syncSource(ctx context.Context, src Source, state connector.StateStore, sink connector.Sink,
	logger *slog.Logger, full bool) (*connector.Report, error) {
	conn, closeConn, err := newConnector(ctx, src)
	if err != nil {
		return nil, err
	}
	defer closeConn()
	s := &connector.Syncer{SourceID: src.ID, Connector: conn, State: state, Sink: sink, Logger: logger}
	return s.Sync(ctx, full)
}

func newConnector(ctx context.Context, src Source) (connector.Connector, func(), error) {
	noop := func() {}
	switch src.Type {
	case "fs":
		c, err := fs.New(src.FS.Root)
		if err != nil {
			return nil, noop, err
		}
		return c, func() { _ = c.Close() }, nil
	case "drive":
		c, err := drive.New(ctx, drive.Config{FolderID: src.Drive.FolderID, CredentialsFile: src.Drive.CredentialsFile})
		return c, noop, err
	case "confluence":
		token := os.Getenv(src.Confluence.TokenEnv)
		if token == "" {
			return nil, noop, fmt.Errorf("source %q: environment variable %s is not set", src.ID, src.Confluence.TokenEnv)
		}
		c, err := confluence.New(confluence.Config{
			BaseURL: src.Confluence.BaseURL, Space: src.Confluence.Space,
			Email: src.Confluence.Email, Token: token,
		})
		return c, noop, err
	}
	return nil, noop, fmt.Errorf("unknown source type %q", src.Type)
}

func printReport(id string, rep *connector.Report, err error, asJSON bool) {
	if asJSON {
		out := struct {
			*connector.Report
			Error string `json:"error,omitempty"`
		}{Report: rep}
		if out.Report == nil {
			out.Report = &connector.Report{SourceID: id}
		}
		if err != nil {
			out.Error = err.Error()
		}
		b, _ := json.Marshal(out)
		fmt.Println(string(b))
		return
	}
	if rep == nil {
		fmt.Printf("%-20s error: %v\n", id, err)
		return
	}
	fmt.Printf("%-20s %-11s fetched=%d unchanged=%d deleted=%d failed=%d (%s)\n",
		id, rep.Mode, rep.Fetched, rep.Unchanged, rep.Deleted, rep.Failed, rep.Duration.Round(1e6))
	for _, f := range rep.Failures {
		fmt.Printf("%-20s   failed %s: %s\n", "", f.ID, f.Error)
	}
	if err != nil {
		fmt.Printf("%-20s   error: %v\n", "", err)
	}
}
