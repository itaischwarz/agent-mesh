package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// State is what Syncer remembers about one source between runs.
type State struct {
	// Cursor is where the next incremental sync resumes. Empty means the
	// source has never completed a sync, so the next run is a full sync.
	Cursor string `json:"cursor"`
	// Versions maps each delivered document's id to the version delivered.
	Versions     map[string]string `json:"versions"`
	LastSync     time.Time         `json:"lastSync"`
	LastFullSync time.Time         `json:"lastFullSync"`
}

// StateStore persists State per source.
type StateStore interface {
	Load(ctx context.Context, sourceID string) (*State, error)
	Save(ctx context.Context, sourceID string, st *State) error
}

// FileStateStore keeps one JSON file per source in Dir.
type FileStateStore struct {
	Dir string
}

func (s *FileStateStore) path(sourceID string) (string, error) {
	if sourceID == "" {
		return "", errors.New("empty source id")
	}
	return filepath.Join(s.Dir, url.PathEscape(sourceID)+".json"), nil
}

func (s *FileStateStore) Load(_ context.Context, sourceID string) (*State, error) {
	p, err := s.path(sourceID)
	if err != nil {
		return nil, err
	}
	st := &State{}
	b, err := os.ReadFile(p)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, err
	default:
		if err := json.Unmarshal(b, st); err != nil {
			return nil, fmt.Errorf("parsing state %s: %w", p, err)
		}
	}
	if st.Versions == nil {
		st.Versions = map[string]string{}
	}
	return st, nil
}

func (s *FileStateStore) Save(_ context.Context, sourceID string, st *State) error {
	p, err := s.path(sourceID)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(p, b)
}

// writeFileAtomic writes via a temp file and rename, so a crash never leaves
// a half-written file behind.
func writeFileAtomic(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
