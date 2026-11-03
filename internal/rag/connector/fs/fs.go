// Package fs is a connector for a local folder. It needs no credentials, so
// it's the easiest way to exercise the sync pipeline end to end.
//
// Document ids are slash-separated paths relative to the root. Hidden files
// and directories (names starting with ".") are skipped. Deletions are only
// detected by full syncs, and files whose modification time is set into the
// past (e.g. cp -p) are only picked up by full syncs too.
package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"iter"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/itaischwarz/agent-mesh/internal/rag/connector"
)

// MaxFileSize is the largest file Fetch will read.
const MaxFileSize = 50 << 20

// Connector syncs the files under one root directory.
type Connector struct {
	dir  string
	root *os.Root
}

var _ connector.Connector = (*Connector)(nil)

// New opens root for syncing.
func New(root string) (*Connector, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	r, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", abs, err)
	}
	return &Connector{dir: abs, root: r}, nil
}

// Close releases the root directory handle.
func (c *Connector) Close() error { return c.root.Close() }

func (c *Connector) StartCursor(context.Context) (string, error) {
	return time.Now().UTC().Format(time.RFC3339Nano), nil
}

type entry struct {
	id      string
	modTime time.Time
	size    int64
}

func (e entry) version() string { return fmt.Sprintf("%d-%d", e.modTime.UnixNano(), e.size) }

// Changes yields files modified at or after cursor, oldest first. Each change
// carries its own modification time as the cursor; files touched in the same
// instant are re-listed next time and skipped as unchanged.
func (c *Connector) Changes(ctx context.Context, cursor string) iter.Seq2[connector.Change, error] {
	return func(yield func(connector.Change, error) bool) {
		since, err := time.Parse(time.RFC3339Nano, cursor)
		if err != nil {
			yield(connector.Change{}, fmt.Errorf("invalid cursor %q: %w", cursor, err))
			return
		}
		entries, err := c.walk(ctx)
		if err != nil {
			yield(connector.Change{}, err)
			return
		}
		slices.SortFunc(entries, func(a, b entry) int {
			if d := a.modTime.Compare(b.modTime); d != 0 {
				return d
			}
			return strings.Compare(a.id, b.id)
		})
		for _, e := range entries {
			if e.modTime.Before(since) {
				continue
			}
			ch := connector.Change{ID: e.id, Version: e.version(), Cursor: e.modTime.UTC().Format(time.RFC3339Nano)}
			if !yield(ch, nil) {
				return
			}
		}
	}
}

func (c *Connector) ListAll(ctx context.Context) iter.Seq2[connector.Change, error] {
	return func(yield func(connector.Change, error) bool) {
		entries, err := c.walk(ctx)
		if err != nil {
			yield(connector.Change{}, err)
			return
		}
		for _, e := range entries {
			if !yield(connector.Change{ID: e.id, Version: e.version()}, nil) {
				return
			}
		}
	}
}

func (c *Connector) walk(ctx context.Context) ([]entry, error) {
	var out []entry
	err := iofs.WalkDir(c.root.FS(), ".", func(p string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if p != "." && strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return iofs.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out = append(out, entry{id: p, modTime: info.ModTime(), size: info.Size()})
		return nil
	})
	return out, err
}

func (c *Connector) Fetch(_ context.Context, id string) (*connector.Document, error) {
	if !iofs.ValidPath(id) || id == "." {
		return nil, fmt.Errorf("invalid document id %q", id)
	}
	f, err := c.root.Open(id)
	if errors.Is(err, os.ErrNotExist) {
		return nil, connector.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, connector.ErrNotFound
	}
	if info.Size() > MaxFileSize {
		return nil, fmt.Errorf("%s is %d bytes, over the %d byte limit", id, info.Size(), MaxFileSize)
	}
	body, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	e := entry{id: id, modTime: info.ModTime(), size: info.Size()}
	return &connector.Document{
		ID:         id,
		Title:      path.Base(id),
		URL:        "file://" + filepath.ToSlash(filepath.Join(c.dir, filepath.FromSlash(id))),
		MimeType:   detectMIME(id, body),
		Body:       body,
		ModifiedAt: info.ModTime().UTC(),
		Version:    e.version(),
	}, nil
}

// Extensions the standard library doesn't know but the pipeline cares about.
var extraMIME = map[string]string{
	".md":       "text/markdown",
	".markdown": "text/markdown",
	".txt":      "text/plain",
	".rst":      "text/x-rst",
	".yaml":     "application/yaml",
	".yml":      "application/yaml",
}

func detectMIME(name string, body []byte) string {
	ext := strings.ToLower(path.Ext(name))
	if t, ok := extraMIME[ext]; ok {
		return t
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return http.DetectContentType(body)
}
