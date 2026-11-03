package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/itaischwarz/agent-mesh/internal/rag/connector"
)

func write(t *testing.T, root, rel, body string, mtime time.Time) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func ids(t *testing.T, seq func(func(connector.Change, error) bool)) []string {
	t.Helper()
	var out []string
	for ch, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ch.ID)
	}
	return out
}

func TestListChangesFetch(t *testing.T) {
	root := t.TempDir()
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	write(t, root, "a.md", "# A", old)
	write(t, root, "sub/b.txt", "bee", recent)
	write(t, root, ".git/config", "hidden", recent)
	write(t, root, ".hidden.md", "hidden", recent)

	c, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()

	if got := ids(t, c.ListAll(ctx)); !slices.Equal(got, []string{"a.md", "sub/b.txt"}) {
		t.Errorf("ListAll = %v", got)
	}

	cursor := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	if got := ids(t, c.Changes(ctx, cursor)); !slices.Equal(got, []string{"sub/b.txt"}) {
		t.Errorf("Changes since March = %v, want only sub/b.txt", got)
	}

	doc, err := c.Fetch(ctx, "a.md")
	if err != nil {
		t.Fatal(err)
	}
	if string(doc.Body) != "# A" || doc.MimeType != "text/markdown" || doc.Title != "a.md" {
		t.Errorf("doc = %+v", doc)
	}
	for ch, err := range c.ListAll(ctx) {
		if err == nil && ch.ID == "a.md" && ch.Version != doc.Version {
			t.Errorf("listing version %q != fetched version %q", ch.Version, doc.Version)
		}
	}

	if _, err := c.Fetch(ctx, "missing.md"); !errors.Is(err, connector.ErrNotFound) {
		t.Errorf("missing file: err = %v, want ErrNotFound", err)
	}
	if _, err := c.Fetch(ctx, "../outside"); err == nil {
		t.Error("path traversal should be rejected")
	}
}

func TestSyncEndToEnd(t *testing.T) {
	root, out := t.TempDir(), t.TempDir()
	write(t, root, "keep.md", "keep", time.Now())
	write(t, root, "drop.md", "drop", time.Now())

	c, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := &connector.Syncer{
		SourceID:  "local",
		Connector: c,
		State:     &connector.FileStateStore{Dir: filepath.Join(out, "state")},
		Sink:      &connector.DirSink{Dir: filepath.Join(out, "raw")},
	}
	ctx := context.Background()

	if rep, err := s.Sync(ctx, false); err != nil || rep.Fetched != 2 {
		t.Fatalf("first sync: %+v, %v", rep, err)
	}
	if rep, err := s.Sync(ctx, false); err != nil || rep.Fetched != 0 {
		t.Fatalf("second sync should find nothing new: %+v, %v", rep, err)
	}
	if err := os.Remove(filepath.Join(root, "drop.md")); err != nil {
		t.Fatal(err)
	}
	if rep, err := s.Sync(ctx, true); err != nil || rep.Deleted != 1 || rep.Unchanged != 1 {
		t.Fatalf("full sync after delete: %+v, %v", rep, err)
	}
}
