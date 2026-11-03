package confluence

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itaischwarz/agent-mesh/internal/rag/connector"
)

// fakeSite serves CQL search (two pages of results), page bodies, and
// restrictions. The first search request is rate limited once.
func fakeSite(t *testing.T) (*httptest.Server, *[]string) {
	t.Helper()
	var cqls []string
	var throttled atomic.Bool
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	result := func(id string, n int, when string) map[string]any {
		return map[string]any{"id": id, "version": map[string]any{"number": n, "when": when}}
	}
	mux.HandleFunc("/wiki/rest/api/content/search", func(w http.ResponseWriter, r *http.Request) {
		if user, pass, _ := r.BasicAuth(); user != "me@x.com" || pass != "tok" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !throttled.Swap(true) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		if r.URL.Query().Get("cursor") == "p2" {
			writeJSON(w, map[string]any{"results": []any{result("3", 1, "2026-10-03T09:00:00.000Z")}})
			return
		}
		cqls = append(cqls, r.URL.Query().Get("cql"))
		writeJSON(w, map[string]any{
			"results": []any{
				result("1", 4, "2026-10-01T09:00:00.000Z"),
				result("2", 2, "2026-10-02T09:00:00.000Z"),
			},
			"_links": map[string]string{
				"base": "http://" + r.Host + "/wiki",
				"next": "/rest/api/content/search?cursor=p2",
			},
		})
	})
	mux.HandleFunc("/wiki/api/v2/pages/", func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.URL.Path, "/wiki/api/v2/pages/") {
		case "1":
			writeJSON(w, map[string]any{
				"id": "1", "title": "Runbook", "status": "current",
				"version": map[string]any{"number": 4, "createdAt": "2026-10-01T09:00:00.000Z"},
				"body":    map[string]any{"storage": map[string]any{"value": "<p>Restart it.</p>"}},
				"_links":  map[string]string{"webui": "/spaces/ENG/pages/1/Runbook"},
			})
		case "2":
			writeJSON(w, map[string]any{"id": "2", "status": "trashed"})
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("/wiki/rest/api/content/1/restriction/byOperation/read", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"restrictions": map[string]any{
			"user":  map[string]any{"results": []any{map[string]string{"accountId": "acc-1"}}},
			"group": map[string]any{"results": []any{map[string]string{"name": "sre"}}},
		}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &cqls
}

func newTestConnector(t *testing.T, base string) *Connector {
	t.Helper()
	c, err := New(Config{BaseURL: base, Space: "ENG", Email: "me@x.com", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestChangesPaginatesRetriesAndOnlyAdvancesCursor(t *testing.T) {
	srv, cqls := fakeSite(t)
	c := newTestConnector(t, srv.URL)

	var got []connector.Change
	for ch, err := range c.Changes(context.Background(), "2026-10-02T00:00:00Z") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ch)
	}
	want := []connector.Change{
		{ID: "1", Version: "4"}, // in the overlap window: listed, but no cursor regression
		{ID: "2", Version: "2", Cursor: "2026-10-02T09:00:00Z"},
		{ID: "3", Version: "1", Cursor: "2026-10-03T09:00:00Z"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Changes =\n %+v\nwant\n %+v", got, want)
	}
	wantCQL := `space = "ENG" AND type = page AND lastmodified >= "2026/10/01 00:00" ORDER BY lastmodified ASC`
	if len(*cqls) != 1 || (*cqls)[0] != wantCQL {
		t.Errorf("CQL = %q, want %q", *cqls, wantCQL)
	}
}

func TestFetch(t *testing.T) {
	srv, _ := fakeSite(t)
	c := newTestConnector(t, srv.URL)
	ctx := context.Background()

	doc, err := c.Fetch(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Title != "Runbook" || string(doc.Body) != "<p>Restart it.</p>" || doc.Version != "4" ||
		doc.MimeType != StorageMIME || doc.URL != srv.URL+"/wiki/spaces/ENG/pages/1/Runbook" {
		t.Errorf("doc = %+v", doc)
	}
	if !doc.ModifiedAt.Equal(time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("ModifiedAt = %v", doc.ModifiedAt)
	}
	wantACL := []connector.Principal{{Kind: "user", ID: "acc-1"}, {Kind: "group", ID: "sre"}}
	if !slices.Equal(doc.ACL, wantACL) {
		t.Errorf("ACL = %v, want %v", doc.ACL, wantACL)
	}

	for _, id := range []string{"2", "404"} { // trashed, missing
		if _, err := c.Fetch(ctx, id); !errors.Is(err, connector.ErrNotFound) {
			t.Errorf("Fetch(%s) err = %v, want ErrNotFound", id, err)
		}
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Config{BaseURL: "https://x", Space: "ENG"}); err == nil {
		t.Error("want error without credentials")
	}
}
