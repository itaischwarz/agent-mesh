package connector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Syncer runs syncs for one source: it pulls changes from Connector, delivers
// documents to Sink, and records progress in State.
//
// Delivery is at-least-once. The cursor is saved only after everything before
// it has reached the sink, and never past a document that failed, so a crash
// or error means redoing some work on the next run, never losing a change.
type Syncer struct {
	SourceID  string
	Connector Connector
	State     StateStore
	Sink      Sink
	// MaxAttempts bounds retries of a document fetch on retryable errors.
	// Defaults to 5.
	MaxAttempts int
	// CommitEvery saves the cursor after this many cursor advances during an
	// incremental sync. Defaults to 100.
	CommitEvery int
	Logger      *slog.Logger
}

// Mode is the kind of sync that ran.
type Mode string

const (
	ModeIncremental Mode = "incremental"
	ModeFull        Mode = "full"
)

// Report summarizes one sync.
type Report struct {
	SourceID  string        `json:"sourceId"`
	Mode      Mode          `json:"mode"`
	Fetched   int           `json:"fetched"`
	Unchanged int           `json:"unchanged"`
	Deleted   int           `json:"deleted"`
	Failed    int           `json:"failed"`
	Failures  []Failure     `json:"failures,omitempty"`
	Cursor    string        `json:"cursor"`
	Duration  time.Duration `json:"duration"`
}

// Failure is one document that couldn't be synced.
type Failure struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}

// Sync runs one sync. It's a full sync if full is set or the source has never
// synced; otherwise it's incremental from the saved cursor. A non-nil Report
// is returned even when err is set, describing what got done.
func (s *Syncer) Sync(ctx context.Context, full bool) (*Report, error) {
	began := time.Now()
	st, err := s.State.Load(ctx, s.SourceID)
	if err != nil {
		return nil, fmt.Errorf("loading state: %w", err)
	}
	rep := &Report{SourceID: s.SourceID, Mode: ModeIncremental}
	if full || st.Cursor == "" {
		rep.Mode = ModeFull
		err = s.full(ctx, st, rep)
	} else {
		err = s.incremental(ctx, st, rep)
	}
	st.LastSync = time.Now()
	if saveErr := s.State.Save(ctx, s.SourceID, st); saveErr != nil && err == nil {
		err = fmt.Errorf("saving state: %w", saveErr)
	}
	rep.Cursor = st.Cursor
	rep.Duration = time.Since(began)
	s.logger().Info("sync finished", "source", s.SourceID, "mode", rep.Mode,
		"fetched", rep.Fetched, "unchanged", rep.Unchanged, "deleted", rep.Deleted,
		"failed", rep.Failed, "duration", rep.Duration, "error", err)
	return rep, err
}

func (s *Syncer) full(ctx context.Context, st *State, rep *Report) error {
	// Captured before listing, so changes made during the listing are
	// replayed by the next incremental sync rather than lost.
	start, err := s.Connector.StartCursor(ctx)
	if err != nil {
		return fmt.Errorf("getting start cursor: %w", err)
	}
	seen := map[string]bool{}
	healthy := true
	for ch, err := range s.Connector.ListAll(ctx) {
		if err != nil {
			// No delete sweep: anything not yet listed would look deleted.
			return fmt.Errorf("listing documents: %w", err)
		}
		if ch.ID == "" || ch.Deleted {
			continue
		}
		seen[ch.ID] = true
		if !s.apply(ctx, st, rep, ch) {
			healthy = false
		}
	}
	for id := range st.Versions {
		if !seen[id] && !s.remove(ctx, st, rep, id) {
			healthy = false
		}
	}
	// With failures, keep the old cursor: a first sync then stays "never
	// synced" and the next run is full again, retrying the failed documents.
	if healthy {
		st.Cursor = start
		st.LastFullSync = time.Now()
	}
	return nil
}

func (s *Syncer) incremental(ctx context.Context, st *State, rep *Report) error {
	commitEvery := s.CommitEvery
	if commitEvery <= 0 {
		commitEvery = 100
	}
	healthy := true
	advances := 0
	for ch, err := range s.Connector.Changes(ctx, st.Cursor) {
		if err != nil {
			return fmt.Errorf("listing changes: %w", err)
		}
		if ch.ID != "" {
			ok := true
			if ch.Deleted {
				ok = s.remove(ctx, st, rep, ch.ID)
			} else {
				ok = s.apply(ctx, st, rep, ch)
			}
			// Keep processing later changes, but stop advancing the cursor:
			// the next run must come back for this one.
			healthy = healthy && ok
		}
		if healthy && ch.Cursor != "" {
			st.Cursor = ch.Cursor
			if advances++; advances%commitEvery == 0 {
				if err := s.State.Save(ctx, s.SourceID, st); err != nil {
					return fmt.Errorf("saving state: %w", err)
				}
			}
		}
	}
	return nil
}

// apply fetches and delivers one changed document, unless the version shows
// it's unchanged. It reports whether the document is fully handled.
func (s *Syncer) apply(ctx context.Context, st *State, rep *Report, ch Change) bool {
	if prev, ok := st.Versions[ch.ID]; ok && ch.Version != "" && prev == ch.Version {
		rep.Unchanged++
		return true
	}
	var doc *Document
	err := Retry(ctx, s.maxAttempts(), func() error {
		var err error
		doc, err = s.Connector.Fetch(ctx, ch.ID)
		return err
	})
	if errors.Is(err, ErrNotFound) {
		return s.remove(ctx, st, rep, ch.ID)
	}
	if err != nil {
		s.fail(rep, ch.ID, fmt.Errorf("fetching: %w", err))
		return false
	}
	if err := s.Sink.Put(ctx, s.SourceID, doc); err != nil {
		s.fail(rep, ch.ID, fmt.Errorf("storing: %w", err))
		return false
	}
	v := doc.Version
	if v == "" {
		v = ch.Version
	}
	st.Versions[ch.ID] = v
	rep.Fetched++
	return true
}

// remove deletes a previously delivered document from the sink.
func (s *Syncer) remove(ctx context.Context, st *State, rep *Report, id string) bool {
	if _, known := st.Versions[id]; !known {
		return true
	}
	if err := s.Sink.Delete(ctx, s.SourceID, id); err != nil {
		s.fail(rep, id, fmt.Errorf("deleting: %w", err))
		return false
	}
	delete(st.Versions, id)
	rep.Deleted++
	return true
}

func (s *Syncer) fail(rep *Report, id string, err error) {
	rep.Failed++
	rep.Failures = append(rep.Failures, Failure{ID: id, Error: err.Error()})
	s.logger().Warn("document sync failed", "source", s.SourceID, "id", id, "error", err)
}

func (s *Syncer) maxAttempts() int {
	if s.MaxAttempts > 0 {
		return s.MaxAttempts
	}
	return 5
}

func (s *Syncer) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}
