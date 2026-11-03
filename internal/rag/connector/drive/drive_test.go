package drive

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"google.golang.org/api/option"

	"github.com/itaischwarz/agent-mesh/internal/rag/connector"
)

// fakeDrive serves the slice of the Drive v3 API the connector uses.
//
//	root/            (synced folder)
//	  notes.txt      plain file
//	  spec           Google Doc
//	  form           Google Form (unsupported, skipped)
//	  sub/
//	    deep.txt
//	elsewhere/stray.txt   (outside the synced folder)
func fakeDrive(t *testing.T) *httptest.Server {
	t.Helper()
	files := map[string]map[string]any{
		"root":      {"id": "root", "mimeType": folderMIME, "parents": []string{"mydrive"}},
		"sub":       {"id": "sub", "mimeType": folderMIME, "parents": []string{"root"}},
		"elsewhere": {"id": "elsewhere", "mimeType": folderMIME, "parents": []string{"mydrive"}},
		"mydrive":   {"id": "mydrive", "mimeType": folderMIME},
		"notes": {"id": "notes", "name": "notes.txt", "mimeType": "text/plain", "version": "7", "size": "5",
			"parents": []string{"root"}, "modifiedTime": "2026-10-01T10:00:00Z", "webViewLink": "https://drive/notes",
			"permissions": []map[string]string{{"type": "user", "emailAddress": "a@x.com"}, {"type": "domain", "domain": "x.com"}}},
		"spec":  {"id": "spec", "name": "Spec", "mimeType": "application/vnd.google-apps.document", "version": "3", "parents": []string{"root"}},
		"form":  {"id": "form", "name": "Form", "mimeType": "application/vnd.google-apps.form", "version": "1", "parents": []string{"root"}},
		"deep":  {"id": "deep", "name": "deep.txt", "mimeType": "text/plain", "version": "2", "size": "4", "parents": []string{"sub"}},
		"stray": {"id": "stray", "name": "stray.txt", "mimeType": "text/plain", "version": "1", "size": "1", "parents": []string{"elsewhere"}},
	}
	children := map[string][]string{"root": {"notes", "spec", "form", "sub"}, "sub": {"deep"}}

	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("/changes/startPageToken", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]string{"startPageToken": "100"})
	})
	mux.HandleFunc("/changes", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("pageToken") {
		case "100":
			writeJSON(w, map[string]any{"nextPageToken": "101", "changes": []map[string]any{
				{"fileId": "notes", "file": files["notes"]},
				{"fileId": "gone", "removed": true},
			}})
		case "101":
			writeJSON(w, map[string]any{"newStartPageToken": "102", "changes": []map[string]any{
				{"fileId": "stray", "file": files["stray"]},
				{"fileId": "sub", "file": files["sub"]},
				{"fileId": "form", "file": files["form"]},
			}})
		default:
			http.Error(w, "bad token", http.StatusBadRequest)
		}
	})
	mux.HandleFunc("/files", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q") // "'<id>' in parents and trashed = false"
		parent := strings.TrimPrefix(strings.SplitN(q, "' in", 2)[0], "'")
		var out []map[string]any
		for _, id := range children[parent] {
			out = append(out, files[id])
		}
		writeJSON(w, map[string]any{"files": out})
	})
	mux.HandleFunc("/files/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/files/")
		id, export := strings.CutSuffix(rest, "/export")
		f, ok := files[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			writeJSON(w, map[string]any{"error": map[string]any{"code": 404, "message": "not found"}})
			return
		}
		switch {
		case export:
			if got := r.URL.Query().Get("mimeType"); got != "text/markdown" {
				t.Errorf("export mimeType = %q, want text/markdown", got)
			}
			_, _ = w.Write([]byte("# Spec"))
		case r.URL.Query().Get("alt") == "media":
			_, _ = w.Write([]byte("hello"))
		default:
			writeJSON(w, f)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newTestConnector(t *testing.T) *Connector {
	t.Helper()
	srv := fakeDrive(t)
	c, err := New(context.Background(), Config{FolderID: "root"},
		option.WithEndpoint(srv.URL+"/"), option.WithoutAuthentication(), option.WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestChanges(t *testing.T) {
	c := newTestConnector(t)
	ctx := context.Background()

	start, err := c.StartCursor(ctx)
	if err != nil || start != "100" {
		t.Fatalf("StartCursor = %q, %v", start, err)
	}
	var got []connector.Change
	for ch, err := range c.Changes(ctx, start) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ch)
	}
	want := []connector.Change{
		{ID: "notes", Version: "7"},
		{ID: "gone", Deleted: true},
		{Cursor: "101"},
		{ID: "stray", Deleted: true}, // outside the folder
		// "sub" is a folder and "form" is unsupported: both skipped.
		{Cursor: "102"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Changes =\n %+v\nwant\n %+v", got, want)
	}
}

func TestListAllRecursesAndSkipsUnsupported(t *testing.T) {
	c := newTestConnector(t)
	var ids []string
	for ch, err := range c.ListAll(context.Background()) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, ch.ID)
	}
	if want := []string{"notes", "spec", "deep"}; !slices.Equal(ids, want) {
		t.Fatalf("ListAll = %v, want %v", ids, want)
	}
}

func TestFetch(t *testing.T) {
	c := newTestConnector(t)
	ctx := context.Background()

	doc, err := c.Fetch(ctx, "notes")
	if err != nil {
		t.Fatal(err)
	}
	if string(doc.Body) != "hello" || doc.MimeType != "text/plain" || doc.Version != "7" || doc.URL != "https://drive/notes" {
		t.Errorf("notes = %+v", doc)
	}
	wantACL := []connector.Principal{{Kind: "user", ID: "a@x.com"}, {Kind: "domain", ID: "x.com"}}
	if !slices.Equal(doc.ACL, wantACL) {
		t.Errorf("ACL = %v, want %v", doc.ACL, wantACL)
	}

	spec, err := c.Fetch(ctx, "spec")
	if err != nil {
		t.Fatal(err)
	}
	if string(spec.Body) != "# Spec" || spec.MimeType != "text/markdown" {
		t.Errorf("exported doc = %+v", spec)
	}

	for _, id := range []string{"missing", "stray", "form"} {
		if _, err := c.Fetch(ctx, id); !errors.Is(err, connector.ErrNotFound) {
			t.Errorf("Fetch(%s) err = %v, want ErrNotFound", id, err)
		}
	}
}
