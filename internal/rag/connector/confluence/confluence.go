// Package confluence is a connector for the pages of one Confluence Cloud
// space, using an Atlassian API token.
//
// Listing uses CQL search (REST v1, since v2 has no CQL); page bodies come
// from REST v2 in storage format (XHTML with Confluence macros), which the
// extraction stage converts to text. CQL can't report deletions, so deleted
// pages are only removed by full syncs.
//
// ACLs come from each page's own read restrictions. Unrestricted pages get a
// "space" principal (readable by whoever can read the space). Restrictions
// inherited from ancestor pages are not resolved yet.
package confluence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/itaischwarz/agent-mesh/internal/rag/connector"
)

// StorageMIME is the MIME type documents carry: Confluence storage format.
const StorageMIME = "application/vnd.atlassian.confluence.storage+xml"

// cqlOverlap is how far before the cursor incremental syncs start listing.
// CQL dates have minute precision and use the account's timezone, so an
// overlap guarantees nothing is missed; repeats are skipped by version.
const cqlOverlap = 24 * time.Hour

const listAttempts = 5

// Config selects the site and space to sync.
type Config struct {
	// BaseURL is the site root, e.g. https://example.atlassian.net.
	BaseURL string
	// Space is the space key, e.g. ENG.
	Space string
	// Email and Token are an Atlassian account email and API token.
	Email string
	Token string
	// HTTPClient defaults to a client with a 30s timeout.
	HTTPClient *http.Client
}

// Connector syncs the pages of one space.
type Connector struct {
	cfg Config
	hc  *http.Client
}

var _ connector.Connector = (*Connector)(nil)

// New creates a Confluence connector.
func New(cfg Config) (*Connector, error) {
	switch {
	case cfg.BaseURL == "":
		return nil, errors.New("confluence: base URL is required")
	case cfg.Space == "":
		return nil, errors.New("confluence: space key is required")
	case cfg.Email == "" || cfg.Token == "":
		return nil, errors.New("confluence: email and API token are required")
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return &Connector{cfg: cfg, hc: hc}, nil
}

func (c *Connector) StartCursor(context.Context) (string, error) {
	return time.Now().UTC().Format(time.RFC3339), nil
}

// Changes lists pages modified since cursor (minus the overlap), oldest
// first. The cursor only ever moves forward.
func (c *Connector) Changes(ctx context.Context, cursor string) iter.Seq2[connector.Change, error] {
	return func(yield func(connector.Change, error) bool) {
		since, err := time.Parse(time.RFC3339, cursor)
		if err != nil {
			yield(connector.Change{}, fmt.Errorf("invalid cursor %q: %w", cursor, err))
			return
		}
		cql := fmt.Sprintf(`space = %s AND type = page AND lastmodified >= "%s" ORDER BY lastmodified ASC`,
			quoteCQL(c.cfg.Space), since.Add(-cqlOverlap).Format("2006/01/02 15:04"))
		latest := since
		for p, err := range c.search(ctx, cql) {
			if err != nil {
				yield(connector.Change{}, err)
				return
			}
			ch := connector.Change{ID: p.ID, Version: strconv.Itoa(p.Version.Number)}
			if when, err := time.Parse(time.RFC3339, p.Version.When); err == nil && when.After(latest) {
				latest = when
				ch.Cursor = when.UTC().Format(time.RFC3339)
			}
			if !yield(ch, nil) {
				return
			}
		}
	}
}

func (c *Connector) ListAll(ctx context.Context) iter.Seq2[connector.Change, error] {
	return func(yield func(connector.Change, error) bool) {
		cql := fmt.Sprintf(`space = %s AND type = page`, quoteCQL(c.cfg.Space))
		for p, err := range c.search(ctx, cql) {
			if err != nil {
				yield(connector.Change{}, err)
				return
			}
			if !yield(connector.Change{ID: p.ID, Version: strconv.Itoa(p.Version.Number)}, nil) {
				return
			}
		}
	}
}

type searchPage struct {
	Results []searchResult `json:"results"`
	Links   struct {
		Base string `json:"base"`
		Next string `json:"next"`
	} `json:"_links"`
}

type searchResult struct {
	ID      string `json:"id"`
	Version struct {
		Number int    `json:"number"`
		When   string `json:"when"`
	} `json:"version"`
}

// search pages through CQL results.
func (c *Connector) search(ctx context.Context, cql string) iter.Seq2[searchResult, error] {
	return func(yield func(searchResult, error) bool) {
		q := url.Values{"cql": {cql}, "limit": {"100"}, "expand": {"version"}}
		next := c.cfg.BaseURL + "/wiki/rest/api/content/search?" + q.Encode()
		for next != "" {
			var page searchPage
			err := connector.Retry(ctx, listAttempts, func() error { return c.getJSON(ctx, next, &page) })
			if err != nil {
				yield(searchResult{}, fmt.Errorf("searching pages: %w", err))
				return
			}
			for _, r := range page.Results {
				if !yield(r, nil) {
					return
				}
			}
			next = c.resolve(page.Links.Base, page.Links.Next)
		}
	}
}

// resolve turns a relative "next" link into an absolute URL.
func (c *Connector) resolve(base, next string) string {
	if next == "" || strings.HasPrefix(next, "http://") || strings.HasPrefix(next, "https://") {
		return next
	}
	if base == "" {
		base = c.cfg.BaseURL + "/wiki"
	}
	return strings.TrimRight(base, "/") + next
}

type page struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Status  string `json:"status"`
	Version struct {
		Number    int    `json:"number"`
		CreatedAt string `json:"createdAt"`
	} `json:"version"`
	Body struct {
		Storage struct {
			Value string `json:"value"`
		} `json:"storage"`
	} `json:"body"`
	Links struct {
		WebUI string `json:"webui"`
	} `json:"_links"`
}

func (c *Connector) Fetch(ctx context.Context, id string) (*connector.Document, error) {
	var p page
	u := fmt.Sprintf("%s/wiki/api/v2/pages/%s?body-format=storage", c.cfg.BaseURL, url.PathEscape(id))
	if err := c.getJSON(ctx, u, &p); err != nil {
		return nil, err
	}
	if p.Status != "" && p.Status != "current" {
		return nil, connector.ErrNotFound // trashed, archived, or a draft
	}
	principals, err := c.readACL(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("reading restrictions: %w", err)
	}
	modified, _ := time.Parse(time.RFC3339, p.Version.CreatedAt)
	doc := &connector.Document{
		ID:         p.ID,
		Title:      p.Title,
		MimeType:   StorageMIME,
		Body:       []byte(p.Body.Storage.Value),
		ModifiedAt: modified.UTC(),
		Version:    strconv.Itoa(p.Version.Number),
		ACL:        principals,
		Metadata:   map[string]string{"space": c.cfg.Space},
	}
	if p.Links.WebUI != "" {
		doc.URL = c.cfg.BaseURL + "/wiki" + p.Links.WebUI
	}
	return doc, nil
}

type restrictions struct {
	Restrictions struct {
		User struct {
			Results []struct {
				AccountID string `json:"accountId"`
			} `json:"results"`
		} `json:"user"`
		Group struct {
			Results []struct {
				Name string `json:"name"`
			} `json:"results"`
		} `json:"group"`
	} `json:"restrictions"`
}

func (c *Connector) readACL(ctx context.Context, id string) ([]connector.Principal, error) {
	var r restrictions
	u := fmt.Sprintf("%s/wiki/rest/api/content/%s/restriction/byOperation/read?expand=restrictions.user,restrictions.group",
		c.cfg.BaseURL, url.PathEscape(id))
	if err := c.getJSON(ctx, u, &r); err != nil {
		return nil, err
	}
	var out []connector.Principal
	for _, u := range r.Restrictions.User.Results {
		out = append(out, connector.Principal{Kind: connector.PrincipalUser, ID: u.AccountID})
	}
	for _, g := range r.Restrictions.Group.Results {
		out = append(out, connector.Principal{Kind: connector.PrincipalGroup, ID: g.Name})
	}
	if len(out) == 0 {
		out = []connector.Principal{{Kind: connector.PrincipalSpace, ID: c.cfg.Space}}
	}
	return out, nil
}

func (c *Connector) getJSON(ctx context.Context, u string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.cfg.Email, c.cfg.Token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return connector.Retryable(err, 0) // network errors are worth retrying
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
		return json.NewDecoder(resp.Body).Decode(out)
	case resp.StatusCode == http.StatusNotFound:
		return connector.ErrNotFound
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return connector.Retryable(httpError(resp), retryAfter(resp.Header))
	default:
		return httpError(resp)
	}
}

func httpError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("confluence: %s %s: %s", resp.Request.Method, resp.Status, strings.TrimSpace(string(b)))
}

func retryAfter(h http.Header) time.Duration {
	if s, err := strconv.Atoi(h.Get("Retry-After")); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return 0
}

// quoteCQL quotes a value for use in a CQL expression.
func quoteCQL(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}
