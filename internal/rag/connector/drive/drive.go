// Package drive is a connector for one Google Drive folder and everything
// beneath it, using the Drive v3 API with a service account.
//
// Incremental syncs use the Changes API, which reports edits and deletions
// across the whole drive; changes outside the folder are filtered out (and a
// file moved out of the folder counts as deleted). Moving a whole subfolder
// out is only noticed by a full sync.
//
// Google Docs are exported as markdown, Sheets as CSV, and Slides as plain
// text. Other Google-native types (forms, drawings, shortcuts) are skipped.
package drive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/itaischwarz/agent-mesh/internal/rag/connector"
)

const (
	folderMIME = "application/vnd.google-apps.folder"
	googleMIME = "application/vnd.google-apps."

	// DefaultMaxFileSize is the largest non-Google file Fetch downloads.
	DefaultMaxFileSize = 50 << 20

	listAttempts = 5
)

// exportAs maps Google-native types to the format they're exported in.
var exportAs = map[string]string{
	"application/vnd.google-apps.document":     "text/markdown",
	"application/vnd.google-apps.spreadsheet":  "text/csv",
	"application/vnd.google-apps.presentation": "text/plain",
}

const (
	changeFields = "nextPageToken,newStartPageToken,changes(fileId,removed,file(id,mimeType,version,trashed,parents,size))"
	listFields   = "nextPageToken,files(id,mimeType,version,size)"
	fetchFields  = "id,name,mimeType,modifiedTime,version,trashed,parents,size,webViewLink,permissions(type,emailAddress,domain)"
)

// Config selects what to sync.
type Config struct {
	// FolderID is the Drive folder to sync, recursively.
	FolderID string
	// CredentialsFile is a service account key. The folder must be shared
	// with the service account's email. Empty uses Application Default
	// Credentials.
	CredentialsFile string
	// MaxFileSize caps downloads of non-Google files; 0 means the default.
	MaxFileSize int64
}

// Connector syncs one Drive folder.
type Connector struct {
	svc      *drive.Service
	folderID string
	maxSize  int64

	mu      sync.Mutex
	parents map[string][]string // folder id -> its parents, for scope checks
}

var _ connector.Connector = (*Connector)(nil)

// New creates a Drive connector. Extra client options are appended last;
// tests use them to point at a fake server.
func New(ctx context.Context, cfg Config, opts ...option.ClientOption) (*Connector, error) {
	if cfg.FolderID == "" {
		return nil, errors.New("drive: folder id is required")
	}
	base := []option.ClientOption{option.WithScopes(drive.DriveReadonlyScope)}
	if cfg.CredentialsFile != "" {
		base = append(base, option.WithAuthCredentialsFile(option.ServiceAccount, cfg.CredentialsFile))
	}
	svc, err := drive.NewService(ctx, append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("drive: creating client: %w", err)
	}
	maxSize := cfg.MaxFileSize
	if maxSize <= 0 {
		maxSize = DefaultMaxFileSize
	}
	return &Connector{svc: svc, folderID: cfg.FolderID, maxSize: maxSize, parents: map[string][]string{}}, nil
}

func (c *Connector) StartCursor(ctx context.Context) (string, error) {
	var token string
	err := connector.Retry(ctx, listAttempts, func() error {
		r, err := c.svc.Changes.GetStartPageToken().SupportsAllDrives(true).Context(ctx).Do()
		if err != nil {
			return classify(err)
		}
		token = r.StartPageToken
		return nil
	})
	return token, err
}

// Changes pages through the Changes API from cursor. The cursor advances at
// page boundaries; resuming mid-page replays the page, and Syncer skips the
// already-delivered documents by version.
func (c *Connector) Changes(ctx context.Context, cursor string) iter.Seq2[connector.Change, error] {
	return func(yield func(connector.Change, error) bool) {
		token := cursor
		for {
			var page *drive.ChangeList
			err := connector.Retry(ctx, listAttempts, func() error {
				var err error
				page, err = c.svc.Changes.List(token).
					IncludeRemoved(true).
					SupportsAllDrives(true).
					IncludeItemsFromAllDrives(true).
					PageSize(100).
					Fields(changeFields).
					Context(ctx).Do()
				return classify(err)
			})
			if err != nil {
				yield(connector.Change{}, fmt.Errorf("listing changes: %w", err))
				return
			}
			for _, ch := range page.Changes {
				out, ok, err := c.toChange(ctx, ch)
				if err != nil {
					yield(connector.Change{}, err)
					return
				}
				if ok && !yield(out, nil) {
					return
				}
			}
			if page.NextPageToken == "" {
				yield(connector.Change{Cursor: page.NewStartPageToken}, nil)
				return
			}
			token = page.NextPageToken
			if !yield(connector.Change{Cursor: token}, nil) {
				return
			}
		}
	}
}

func (c *Connector) toChange(ctx context.Context, ch *drive.Change) (connector.Change, bool, error) {
	f := ch.File
	if ch.Removed || f == nil || f.Trashed {
		return connector.Change{ID: ch.FileId, Deleted: true}, true, nil
	}
	if f.MimeType == folderMIME {
		c.mu.Lock()
		c.parents[f.Id] = f.Parents // keep the scope cache current
		c.mu.Unlock()
		return connector.Change{}, false, nil
	}
	in, err := c.inScope(ctx, f.Parents)
	if err != nil {
		return connector.Change{}, false, err
	}
	if !in {
		// Never in scope, or moved out of it; deleting an unknown id is a no-op.
		return connector.Change{ID: f.Id, Deleted: true}, true, nil
	}
	if !c.syncable(f) {
		return connector.Change{}, false, nil
	}
	return connector.Change{ID: f.Id, Version: version(f)}, true, nil
}

// ListAll walks the folder tree breadth-first.
func (c *Connector) ListAll(ctx context.Context) iter.Seq2[connector.Change, error] {
	return func(yield func(connector.Change, error) bool) {
		queue := []string{c.folderID}
		seen := map[string]bool{c.folderID: true}
		for len(queue) > 0 {
			folder := queue[0]
			queue = queue[1:]
			q := fmt.Sprintf("'%s' in parents and trashed = false", escapeQuery(folder))
			pageToken := ""
			for {
				var page *drive.FileList
				err := connector.Retry(ctx, listAttempts, func() error {
					var err error
					page, err = c.svc.Files.List().Q(q).
						SupportsAllDrives(true).
						IncludeItemsFromAllDrives(true).
						PageSize(1000).
						PageToken(pageToken).
						Fields(listFields).
						Context(ctx).Do()
					return classify(err)
				})
				if err != nil {
					yield(connector.Change{}, fmt.Errorf("listing folder %s: %w", folder, err))
					return
				}
				for _, f := range page.Files {
					if seen[f.Id] { // files can have several parents
						continue
					}
					seen[f.Id] = true
					if f.MimeType == folderMIME {
						queue = append(queue, f.Id)
						continue
					}
					if !c.syncable(f) {
						continue
					}
					if !yield(connector.Change{ID: f.Id, Version: version(f)}, nil) {
						return
					}
				}
				if pageToken = page.NextPageToken; pageToken == "" {
					break
				}
			}
		}
	}
}

func (c *Connector) Fetch(ctx context.Context, id string) (*connector.Document, error) {
	f, err := c.svc.Files.Get(id).SupportsAllDrives(true).Fields(fetchFields).Context(ctx).Do()
	if err != nil {
		return nil, classify(err)
	}
	if f.Trashed || f.MimeType == folderMIME || !c.syncable(f) {
		return nil, connector.ErrNotFound
	}
	if in, err := c.inScope(ctx, f.Parents); err != nil {
		return nil, err
	} else if !in {
		return nil, connector.ErrNotFound
	}
	body, mimeType, err := c.content(ctx, f)
	if err != nil {
		return nil, err
	}
	modified, _ := time.Parse(time.RFC3339, f.ModifiedTime)
	return &connector.Document{
		ID:         f.Id,
		Title:      f.Name,
		URL:        f.WebViewLink,
		MimeType:   mimeType,
		Body:       body,
		ModifiedAt: modified.UTC(),
		Version:    version(f),
		ACL:        acl(f.Permissions),
		Metadata:   map[string]string{"driveMimeType": f.MimeType},
	}, nil
}

func (c *Connector) content(ctx context.Context, f *drive.File) ([]byte, string, error) {
	if target, ok := exportAs[f.MimeType]; ok {
		resp, err := c.svc.Files.Export(f.Id, target).Context(ctx).Download()
		if err != nil && target == "text/markdown" && isBadRequest(err) {
			// Fall back if markdown export isn't available for this file.
			target = "text/plain"
			resp, err = c.svc.Files.Export(f.Id, target).Context(ctx).Download()
		}
		if err != nil {
			return nil, "", classify(err)
		}
		body, err := readLimited(resp.Body, c.maxSize)
		return body, target, err
	}
	resp, err := c.svc.Files.Get(f.Id).SupportsAllDrives(true).Context(ctx).Download()
	if err != nil {
		return nil, "", classify(err)
	}
	body, err := readLimited(resp.Body, c.maxSize)
	return body, f.MimeType, err
}

// syncable reports whether a file is a type and size we fetch.
func (c *Connector) syncable(f *drive.File) bool {
	if strings.HasPrefix(f.MimeType, googleMIME) {
		_, ok := exportAs[f.MimeType]
		return ok
	}
	return f.Size <= c.maxSize
}

// inScope reports whether any ancestor of a file with these parents is the
// synced folder. Folder parents are cached for the connector's lifetime.
func (c *Connector) inScope(ctx context.Context, parents []string) (bool, error) {
	seen := map[string]bool{}
	frontier := parents
	for depth := 0; len(frontier) > 0 && depth < 64; depth++ {
		var next []string
		for _, p := range frontier {
			if p == c.folderID {
				return true, nil
			}
			if seen[p] {
				continue
			}
			seen[p] = true
			pp, err := c.parentsOf(ctx, p)
			if err != nil {
				return false, err
			}
			next = append(next, pp...)
		}
		frontier = next
	}
	return false, nil
}

func (c *Connector) parentsOf(ctx context.Context, id string) ([]string, error) {
	c.mu.Lock()
	pp, ok := c.parents[id]
	c.mu.Unlock()
	if ok {
		return pp, nil
	}
	var f *drive.File
	err := connector.Retry(ctx, listAttempts, func() error {
		var err error
		f, err = c.svc.Files.Get(id).SupportsAllDrives(true).Fields("parents").Context(ctx).Do()
		return classify(err)
	})
	switch {
	case errors.Is(err, connector.ErrNotFound):
		pp = nil // gone, or not visible to us: not an ancestor path we can follow
	case err != nil:
		return nil, fmt.Errorf("resolving parents of %s: %w", id, err)
	default:
		pp = f.Parents
	}
	c.mu.Lock()
	c.parents[id] = pp
	c.mu.Unlock()
	return pp, nil
}

func version(f *drive.File) string { return strconv.FormatInt(f.Version, 10) }

func acl(perms []*drive.Permission) []connector.Principal {
	var out []connector.Principal
	for _, p := range perms {
		switch p.Type {
		case "user", "group":
			out = append(out, connector.Principal{Kind: p.Type, ID: p.EmailAddress})
		case "domain":
			out = append(out, connector.Principal{Kind: connector.PrincipalDomain, ID: p.Domain})
		case "anyone":
			out = append(out, connector.Principal{Kind: connector.PrincipalAnyone})
		}
	}
	return out
}

// classify maps Drive API errors onto the connector's error kinds.
func classify(err error) error {
	var gerr *googleapi.Error
	if err == nil || !errors.As(err, &gerr) {
		return err
	}
	switch {
	case gerr.Code == http.StatusNotFound:
		return fmt.Errorf("%w: %v", connector.ErrNotFound, err)
	case gerr.Code == http.StatusTooManyRequests || gerr.Code >= 500:
		return connector.Retryable(err, retryAfter(gerr.Header))
	case gerr.Code == http.StatusForbidden:
		for _, e := range gerr.Errors {
			if e.Reason == "rateLimitExceeded" || e.Reason == "userRateLimitExceeded" {
				return connector.Retryable(err, retryAfter(gerr.Header))
			}
		}
	}
	return err
}

func isBadRequest(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == http.StatusBadRequest
}

func retryAfter(h http.Header) time.Duration {
	if s, err := strconv.Atoi(h.Get("Retry-After")); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return 0
}

func readLimited(r io.ReadCloser, max int64) ([]byte, error) {
	defer r.Close()
	body, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, fmt.Errorf("file is over the %d byte limit", max)
	}
	return body, nil
}

func escapeQuery(s string) string {
	return strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s)
}
