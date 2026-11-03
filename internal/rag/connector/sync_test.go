package connector

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"testing"
	"time"
)

// fakeConnector serves docs for ListAll/Fetch and a scripted Changes stream.
type fakeConnector struct {
	docs      map[string]string // id -> version
	changes   []Change
	listErr   error              // yielded by ListAll after its first document
	fetchErrs map[string][]error // per id, consumed one per Fetch call
	start     string
	fetched   []string
	gotCursor string
}

func (f *fakeConnector) StartCursor(context.Context) (string, error) { return f.start, nil }

func (f *fakeConnector) Changes(_ context.Context, cursor string) iter.Seq2[Change, error] {
	f.gotCursor = cursor
	return func(yield func(Change, error) bool) {
		for _, c := range f.changes {
			if !yield(c, nil) {
				return
			}
		}
	}
}

func (f *fakeConnector) ListAll(context.Context) iter.Seq2[Change, error] {
	return func(yield func(Change, error) bool) {
		for i, id := range slices.Sorted(maps.Keys(f.docs)) {
			if i == 1 && f.listErr != nil {
				yield(Change{}, f.listErr)
				return
			}
			if !yield(Change{ID: id, Version: f.docs[id]}, nil) {
				return
			}
		}
	}
}

func (f *fakeConnector) Fetch(_ context.Context, id string) (*Document, error) {
	f.fetched = append(f.fetched, id)
	if errs := f.fetchErrs[id]; len(errs) > 0 {
		f.fetchErrs[id] = errs[1:]
		if errs[0] != nil {
			return nil, errs[0]
		}
	}
	v, ok := f.docs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return &Document{ID: id, Version: v, Body: []byte("body of " + id)}, nil
}

type memState struct{ st *State }

func (m *memState) Load(context.Context, string) (*State, error) {
	if m.st == nil {
		m.st = &State{Versions: map[string]string{}}
	}
	return m.st, nil
}
func (m *memState) Save(context.Context, string, *State) error { return nil }

type memSink struct{ docs map[string]*Document }

func (m *memSink) Put(_ context.Context, _ string, d *Document) error {
	if m.docs == nil {
		m.docs = map[string]*Document{}
	}
	m.docs[d.ID] = d
	return nil
}
func (m *memSink) Delete(_ context.Context, _, id string) error {
	delete(m.docs, id)
	return nil
}

func newSyncer(c *fakeConnector, st *State) (*Syncer, *memState, *memSink) {
	state, sink := &memState{st: st}, &memSink{}
	return &Syncer{SourceID: "src", Connector: c, State: state, Sink: sink}, state, sink
}

func noSleep(t *testing.T) {
	orig := wait
	wait = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { wait = orig })
}

func TestFirstSyncIsFull(t *testing.T) {
	c := &fakeConnector{docs: map[string]string{"a": "1", "b": "1"}, start: "cur-0"}
	s, state, sink := newSyncer(c, nil)

	rep, err := s.Sync(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mode != ModeFull || rep.Fetched != 2 {
		t.Fatalf("report = %+v, want full sync fetching 2", rep)
	}
	if state.st.Cursor != "cur-0" || len(sink.docs) != 2 {
		t.Fatalf("cursor %q, sink %d docs", state.st.Cursor, len(sink.docs))
	}
}

func TestIncrementalSkipsUnchangedAndAdvancesCursor(t *testing.T) {
	c := &fakeConnector{
		docs: map[string]string{"a": "1", "b": "2"},
		changes: []Change{
			{ID: "a", Version: "1", Cursor: "c1"}, // unchanged
			{ID: "b", Version: "2", Cursor: "c2"}, // changed
		},
	}
	s, state, _ := newSyncer(c, &State{Cursor: "c0", Versions: map[string]string{"a": "1", "b": "1"}})

	rep, err := s.Sync(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if c.gotCursor != "c0" {
		t.Errorf("Changes called with cursor %q, want c0", c.gotCursor)
	}
	if !slices.Equal(c.fetched, []string{"b"}) || rep.Unchanged != 1 {
		t.Errorf("fetched %v, unchanged %d; want only b fetched", c.fetched, rep.Unchanged)
	}
	if state.st.Cursor != "c2" || state.st.Versions["b"] != "2" {
		t.Errorf("state = %+v", state.st)
	}
}

func TestIncrementalDelete(t *testing.T) {
	c := &fakeConnector{changes: []Change{{ID: "a", Deleted: true, Cursor: "c1"}}}
	s, state, sink := newSyncer(c, &State{Cursor: "c0", Versions: map[string]string{"a": "1"}})
	sink.docs = map[string]*Document{"a": {ID: "a"}}

	rep, err := s.Sync(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted != 1 || len(sink.docs) != 0 || len(state.st.Versions) != 0 {
		t.Fatalf("delete not applied: report %+v, sink %v", rep, sink.docs)
	}
}

func TestFullSyncSweepsMissingDocuments(t *testing.T) {
	c := &fakeConnector{docs: map[string]string{"a": "1", "b": "1"}, start: "c9"}
	s, state, sink := newSyncer(c, &State{Cursor: "c0", Versions: map[string]string{"a": "1", "b": "1", "gone": "1"}})
	sink.docs = map[string]*Document{"gone": {ID: "gone"}}

	rep, err := s.Sync(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted != 1 || rep.Unchanged != 2 || sink.docs["gone"] != nil {
		t.Fatalf("report %+v", rep)
	}
	if state.st.Cursor != "c9" {
		t.Errorf("cursor %q, want c9", state.st.Cursor)
	}
}

func TestListingErrorSkipsDeleteSweep(t *testing.T) {
	c := &fakeConnector{docs: map[string]string{"a": "1", "b": "1"}, listErr: errors.New("boom")}
	s, state, _ := newSyncer(c, &State{Cursor: "c0", Versions: map[string]string{"a": "1", "b": "1"}})

	if _, err := s.Sync(context.Background(), true); err == nil {
		t.Fatal("want error from failed listing")
	}
	if len(state.st.Versions) != 2 || state.st.Cursor != "c0" {
		t.Fatalf("partial listing changed state: %+v", state.st)
	}
}

func TestFailureHoldsCursorButKeepsGoing(t *testing.T) {
	c := &fakeConnector{
		docs: map[string]string{"x": "1", "y": "1", "z": "1"},
		changes: []Change{
			{ID: "x", Version: "1", Cursor: "c1"},
			{ID: "y", Version: "1", Cursor: "c2"},
			{ID: "z", Version: "1", Cursor: "c3"},
		},
		fetchErrs: map[string][]error{"y": {errors.New("permanent")}},
	}
	s, state, sink := newSyncer(c, &State{Cursor: "c0", Versions: map[string]string{}})

	rep, err := s.Sync(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed != 1 || rep.Failures[0].ID != "y" {
		t.Fatalf("report %+v", rep)
	}
	if state.st.Cursor != "c1" {
		t.Errorf("cursor %q advanced past the failed document, want c1", state.st.Cursor)
	}
	if sink.docs["z"] == nil {
		t.Error("documents after the failure should still be delivered")
	}
}

func TestRetryableFetchIsRetried(t *testing.T) {
	noSleep(t)
	rate := Retryable(errors.New("429"), 0)
	c := &fakeConnector{
		docs:      map[string]string{"a": "1"},
		changes:   []Change{{ID: "a", Version: "1", Cursor: "c1"}},
		fetchErrs: map[string][]error{"a": {rate, rate}},
	}
	s, state, _ := newSyncer(c, &State{Cursor: "c0", Versions: map[string]string{}})

	rep, err := s.Sync(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Fetched != 1 || len(c.fetched) != 3 || state.st.Cursor != "c1" {
		t.Fatalf("report %+v after %d fetch attempts", rep, len(c.fetched))
	}
}

func TestFetchNotFoundDeletes(t *testing.T) {
	c := &fakeConnector{changes: []Change{{ID: "a", Version: "2", Cursor: "c1"}}}
	s, state, _ := newSyncer(c, &State{Cursor: "c0", Versions: map[string]string{"a": "1"}})

	rep, err := s.Sync(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Deleted != 1 || state.st.Cursor != "c1" {
		t.Fatalf("report %+v", rep)
	}
}

func TestFileStateStoreAndDirSink(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store := &FileStateStore{Dir: dir}
	in := &State{Cursor: "c1", Versions: map[string]string{"a/b": "3"}}
	if err := store.Save(ctx, "my source", in); err != nil {
		t.Fatal(err)
	}
	out, err := store.Load(ctx, "my source")
	if err != nil || out.Cursor != "c1" || out.Versions["a/b"] != "3" {
		t.Fatalf("round trip = %+v, %v", out, err)
	}
	empty, err := store.Load(ctx, "never-synced")
	if err != nil || empty.Cursor != "" || empty.Versions == nil {
		t.Fatalf("missing state = %+v, %v", empty, err)
	}

	sink := &DirSink{Dir: dir}
	doc := &Document{ID: "../../etc/passwd", Title: "t", Body: []byte("hi")}
	if err := sink.Put(ctx, "src", doc); err != nil {
		t.Fatal(err)
	}
	if err := sink.Delete(ctx, "src", doc.ID); err != nil {
		t.Fatal(err)
	}
	if err := sink.Delete(ctx, "src", doc.ID); err != nil {
		t.Fatalf("second delete should be a no-op: %v", err)
	}
}
