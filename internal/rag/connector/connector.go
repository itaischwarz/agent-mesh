// Package connector pulls documents from external sources (local folders,
// Google Drive, Confluence) into the knowledge platform.
//
// Each source implements Connector, which only knows how to list changes and
// fetch documents. Syncer handles everything sources have in common: cursors,
// skipping unchanged documents, retries, and deletions.
package connector

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"
)

// Connector is implemented once per source type.
type Connector interface {
	// StartCursor returns a cursor for "now". Syncer captures it before a
	// full listing, so anything that changes during the listing is picked up
	// by the next incremental sync.
	StartCursor(ctx context.Context) (string, error)

	// Changes yields documents changed since cursor, in order. It lists
	// metadata only; bodies come from Fetch. Sources that can't report
	// deletions incrementally leave them to full syncs.
	Changes(ctx context.Context, cursor string) iter.Seq2[Change, error]

	// ListAll yields every document currently in scope. Syncer diffs it
	// against what it has delivered before to find deletions.
	ListAll(ctx context.Context) iter.Seq2[Change, error]

	// Fetch downloads one document. It returns ErrNotFound if the document
	// no longer exists or is no longer in scope.
	Fetch(ctx context.Context, id string) (*Document, error)
}

// Change describes one document that changed, or a cursor checkpoint.
type Change struct {
	// ID is the source-specific document id. Empty for a checkpoint.
	ID string
	// Deleted reports that the document was removed or left scope.
	Deleted bool
	// Version changes whenever the document's content changes, so Syncer can
	// skip unchanged documents without fetching them. It must use the same
	// format as Document.Version from Fetch.
	Version string
	// Cursor, if set, is safe to resume from once this change and every
	// earlier one has been processed.
	Cursor string
}

// Document is a fetched document, normalized across sources.
type Document struct {
	ID         string            `json:"id"`
	Title      string            `json:"title"`
	URL        string            `json:"url,omitempty"`
	MimeType   string            `json:"mimeType"`
	Body       []byte            `json:"-"`
	ModifiedAt time.Time         `json:"modifiedAt"`
	Version    string            `json:"version"`
	ACL        []Principal       `json:"acl,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Principal is someone allowed to read a document.
type Principal struct {
	Kind string `json:"kind"`
	ID   string `json:"id,omitempty"`
}

// Principal kinds.
const (
	PrincipalUser   = "user"
	PrincipalGroup  = "group"
	PrincipalDomain = "domain"
	PrincipalAnyone = "anyone"
	// PrincipalSpace means "whoever can read the containing space", for
	// sources (like Confluence) where unrestricted documents inherit access.
	PrincipalSpace = "space"
)

// ErrNotFound reports that a document no longer exists or is out of scope.
var ErrNotFound = errors.New("document not found")

// RetryableError marks a transient failure, such as rate limiting or a 5xx.
type RetryableError struct {
	Err error
	// After is how long the source asked us to wait; zero means use backoff.
	After time.Duration
}

func (e *RetryableError) Error() string { return fmt.Sprintf("retryable: %v", e.Err) }
func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable wraps err as a RetryableError.
func Retryable(err error, after time.Duration) error {
	return &RetryableError{Err: err, After: after}
}

// wait sleeps for d or until ctx is done. A variable so tests can skip sleeps.
var wait = func(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Retry calls fn until it succeeds, returns a non-retryable error, or
// attempts run out, backing off exponentially between tries.
func Retry(ctx context.Context, attempts int, fn func() error) error {
	if attempts < 1 {
		attempts = 1
	}
	for attempt := 1; ; attempt++ {
		err := fn()
		var re *RetryableError
		if err == nil || !errors.As(err, &re) || attempt >= attempts {
			return err
		}
		d := re.After
		if d <= 0 {
			d = backoff(attempt)
		}
		if err := wait(ctx, d); err != nil {
			return err
		}
	}
}

func backoff(attempt int) time.Duration {
	d := 500 * time.Millisecond << (attempt - 1)
	if d > 30*time.Second || d <= 0 {
		d = 30 * time.Second
	}
	return d
}
