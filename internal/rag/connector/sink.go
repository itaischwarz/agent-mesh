package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
)

// Sink receives synced documents. Implementations must be idempotent: the
// same Put or Delete may arrive more than once.
type Sink interface {
	Put(ctx context.Context, sourceID string, doc *Document) error
	Delete(ctx context.Context, sourceID, id string) error
}

// DirSink stores documents on disk, standing in for the raw object store:
//
//	<Dir>/<source>/<key>.json   metadata (Document without Body)
//	<Dir>/<source>/<key>.body   raw content
//
// key is a hash of the document id, since source ids aren't safe file names.
type DirSink struct {
	Dir string
}

func (s *DirSink) paths(sourceID, id string) (meta, body string) {
	sum := sha256.Sum256([]byte(id))
	base := filepath.Join(s.Dir, url.PathEscape(sourceID), hex.EncodeToString(sum[:16]))
	return base + ".json", base + ".body"
}

func (s *DirSink) Put(_ context.Context, sourceID string, doc *Document) error {
	metaPath, bodyPath := s.paths(sourceID, doc.ID)
	meta, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	// Body first: a metadata file only appears once its body is in place.
	if err := writeFileAtomic(bodyPath, doc.Body); err != nil {
		return err
	}
	return writeFileAtomic(metaPath, meta)
}

func (s *DirSink) Delete(_ context.Context, sourceID, id string) error {
	metaPath, bodyPath := s.paths(sourceID, id)
	for _, p := range []string{metaPath, bodyPath} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
