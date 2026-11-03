# Knowledge platform plan (RAG for agent-mesh)

Status: draft, pre-implementation. Items marked **[decide]** need a call before
the phase that depends on them starts.

## 1. What we're building

A Kubernetes-native knowledge platform that agents in the mesh use for context:

1. **Connectors** pull documents from Confluence, Google Drive, and direct
   uploads, keeping them in sync incrementally.
2. **An ingestion pipeline** extracts, chunks, embeds, and indexes them, under
   any number of **indexing profiles** (chunking strategy × embedding model ×
   index settings).
3. **A retrieval service** answers agent queries, with two quality layers:
   query **rewriting** and result **grading** that tells the agent what's
   missing and what to try next.
4. **An MCP server** exposes retrieval to agents as tools.
5. **A benchmark harness** measures which indexing profile and which quality
   layers actually retrieve better, on your own data.
6. **Prometheus observability** across all of it.

Reference points: Google's Vertex AI RAG Engine (corpora, async import,
retrieval), agentic-RAG patterns (Corrective RAG, Self-RAG), and the existing
mesh's Kubernetes-native style: sources and profiles are CRDs reconciled by
controllers.

### Non-goals (for now)
- Answer generation. The platform returns context; the agent's own LLM answers.
- Multi-cluster deployments, or a UI beyond Grafana.

## 2. Architecture

```
                 ┌────────────── control plane (operator) ──────────────┐
                 │  DataSource CRD   IndexProfile CRD   Corpus CRD      │
                 └──────┬──────────────────┬─────────────────────────────┘
                        │ schedules syncs  │ declares how to index
                        ▼                  ▼
Confluence ─┐   ┌──────────────┐   ┌──────────┐   ┌──────────────────────────┐
Drive ──────┼──>│ connector    │──>│ raw store│──>│ NATS JetStream           │
Upload API ─┘   │ runners      │   │ (MinIO)  │   │ docs.changed / deleted   │
                └──────────────┘   └──────────┘   └────────────┬─────────────┘
                                                               ▼
                                     ┌──────────────────────────────────────┐
                                     │ index workers (one consumer group per │
                                     │ IndexProfile): extract → chunk →      │
                                     │ embed → upsert                        │
                                     └───────────────┬──────────────────────┘
                                                     ▼
                               Qdrant (one collection per corpus × profile)
                               Postgres (sources, documents, cursors, jobs, eval runs)
                                                     ▲
agent ──MCP──> rag-mcp ──gRPC──> retrieval service: rewrite → retrieve → grade
                                                     ▲
rag-bench ──gRPC──────────────────────────────────────┘   (runs eval sets per profile)

Every component ──/metrics──> Prometheus ──> Grafana dashboards + alert rules
```

Design choices that matter:

- **Raw documents are kept (MinIO).** This is what makes benchmarking cheap:
  re-indexing under a new profile replays stored documents instead of
  re-fetching them from Confluence or Drive and hitting their rate limits.
- **A queue between fetching and indexing (NATS JetStream).** It gives
  backpressure, retries, a dead-letter subject, and lets index workers scale
  independently of connectors. Each `IndexProfile` is its own consumer group,
  so one document change fans out to every profile.
- **Postgres is the system of record** for metadata: sources, documents
  (content hash, ACL, version), sync cursors, ingestion jobs, and benchmark
  runs. Qdrant only holds vectors and their payloads.
- **Kubernetes-native control plane.** Sources and profiles are declared as CRDs
  and reconciled, matching how agents already work in the mesh.

## 3. Components

### 3.1 Connectors
A common interface, so every source looks the same downstream:

```go
type Connector interface {
    // List yields documents changed since cursor, plus a new cursor.
    Changes(ctx context.Context, cursor string) iter.Seq2[Change, error]
    Fetch(ctx context.Context, ref DocumentRef) (*RawDocument, error)
}
// Change is an upsert or a delete; RawDocument carries body, MIME type,
// title, URL, modified time, and ACL (allowed users/groups).
```

| Source | Auth | Incremental sync | Notes |
|---|---|---|---|
| Google Drive | Service account (+ domain-wide delegation for Workspace) | Changes API page tokens | Export Docs/Sheets to text; ACL from permissions |
| Confluence Cloud | API token or OAuth | CQL `lastmodified >= cursor` | Storage-format XHTML → markdown; page restrictions → ACL |
| Upload API | — | n/a (push) | Same path as the gRPC `IngestDocument` |

- **Deletes matter.** A doc removed at the source must be removed from every
  index, or agents retrieve stale or forbidden content. Full syncs reconcile by
  diffing against Postgres.
- **Rate limits:** per-connector token bucket, retries with exponential backoff.
- **Credentials** come from Kubernetes Secrets referenced by the `DataSource`.

### 3.2 Control plane (CRDs)

| CRD | Declares | Controller does |
|---|---|---|
| `DataSource` | connector type, scope (Drive folder / Confluence space), schedule, secret ref, target corpus | Runs syncs as Jobs on the schedule; tracks cursor, last sync, doc counts in status |
| `IndexProfile` | chunker + params, embedding model, Qdrant index settings (HNSW m/ef, quantization) | Creates the collection, starts a consumer group, backfills from the raw store |
| `Corpus` | name, which sources feed it, default profile | Aggregates status (docs, chunks, freshness) |

### 3.3 Ingestion pipeline
1. **Extract** text by MIME type (markdown, HTML, Confluence storage format,
   Google Docs export, PDF later).
2. **Chunk** using the profile's strategy (see §4).
3. **Embed** in batches through an `Embedder` interface.
4. **Upsert** to the profile's Qdrant collection, replacing the document's
   previous chunks. Payload: document id, source, URL, heading path, ACL,
   content hash.

Skip unchanged documents by content hash, so a re-sync only re-embeds what
changed.

### 3.4 Retrieval service (gRPC)
- **IngestionService** (user-facing): `CreateCorpus`, `IngestDocument`,
  `UploadDocument` (client streaming), `GetIngestionJob`, `DeleteDocument`.
- **RetrievalService** (agent-facing):

| RPC | Purpose |
|---|---|
| `RewriteQuery` | question + origin (user/agent) + history + task context → standalone question + 1–3 retrieval queries |
| `Retrieve` | Search one or more queries under a profile, merge and dedupe; the fast path |
| `GradeRetrieval` | Verdict (SUFFICIENT / PARTIAL / INSUFFICIENT), per-chunk relevance, `missing`, `suggested_queries` |
| `Search` | rewrite → retrieve → grade, with a `max_rounds` retry budget |

- **ACL-aware:** requests carry the principal (user or agent identity), and
  Qdrant filters on the ACL payload. This is designed in from day one even if
  it's enforced later, because retrofitting it means re-indexing everything.
- **[decide] Retry loop:** agent-driven (recommended: `max_rounds = 1`, and the
  agent's LLM decides whether to search again) vs server-driven
  (`max_rounds > 1`, capped at 3).

### 3.5 MCP server (`rag-mcp`)
- Official MCP Go SDK over Streamable HTTP.
- Tools: `rag_search` (wraps `Search`) and `rag_retrieve` (wraps `Retrieve`).
- The `rag_search` description tells the agent to retry with
  `suggested_queries` when the verdict isn't SUFFICIENT. Treat that wording as
  API and test it.
- First consumer: the ADK-based `deepresearch` agent.

### 3.6 Mesh integration
- `Agent.spec.knowledge` (corpus + profile) → `AgentReconciler` injects
  `RAG_MCP_URL` and `RAG_CORPUS`.
- **[decide]** Delegation prompt refinement (`RefinePrompt`, called by the mesh
  before dispatch). Never silently replace the original message: keep it on
  the Task and send both.

## 4. Benchmarking indexing strategies

The question it answers: *for this corpus, which profile retrieves best, at
what cost?*

**Dimensions to compare (each combination is an `IndexProfile`):**

| Dimension | Options |
|---|---|
| Chunking | fixed-size tokens; recursive by headings; sentence-window; semantic (embedding-similarity breaks); parent-document (retrieve small, return big) |
| Chunk size / overlap | e.g. 256 / 512 / 1024 tokens, 0–20% overlap |
| Embedding model | 2–3 models (API vs local; dimensions) |
| Retrieval | dense; BM25 (Qdrant sparse vectors); hybrid with reciprocal rank fusion |
| Reranking | off vs cross-encoder rerank of top 50 |
| Index | HNSW `m`/`ef`; scalar/binary quantization |
| Quality layers | rewrite on/off; grader on/off |

**Evaluation set:**
- Hand-labeled: ~30 questions with the documents that answer them.
- Synthetic, to scale: an LLM generates question/answer pairs from sampled
  chunks. Synthetic sets are biased toward the chunking that produced them, so
  generate them from raw documents, not chunks, and keep a hand-labeled set as
  ground truth.

**Metrics:** recall@k, MRR, nDCG@10 (relevance); p50/p95 latency (speed);
index size, embedding tokens, ingest time (cost); grader agreement with labels.

**Harness:** `rag-bench` CLI. It runs an eval set against N profiles, writes
results to Postgres and a markdown report, and pushes metrics to Prometheus
(Pushgateway) so runs can be compared in Grafana. It can be wrapped in a
`Benchmark` CRD later.

## 5. Observability (Prometheus)

Deployed with kube-prometheus-stack. Every component exposes `/metrics`,
scraped via `ServiceMonitor`s.

| Area | Key metrics |
|---|---|
| Connectors | `rag_connector_docs_synced_total{source,result}`, `rag_connector_sync_duration_seconds`, `rag_connector_last_success_timestamp`, `rag_connector_api_errors_total{source,code}` |
| Pipeline | `rag_pipeline_queue_depth{profile}`, `rag_pipeline_stage_duration_seconds{stage,profile}`, `rag_embedding_tokens_total{model}`, `rag_pipeline_dlq_total` |
| Retrieval | gRPC RED metrics per method (go-grpc-middleware), `rag_retrieval_stage_duration_seconds{stage}`, `rag_grade_verdict_total{verdict}`, `rag_search_rounds` |
| LLM calls | `rag_llm_requests_total{purpose,model,result}`, `rag_llm_tokens_total{purpose,direction}` |
| Mesh | `mesh_tasks_total{state}`, `mesh_dispatch_duration_seconds`, plus controller-runtime's built-in reconcile metrics |

- **Dashboards:** source freshness, pipeline throughput and lag, retrieval
  latency and verdict mix, LLM spend, benchmark comparisons.
- **Alerts:** sync failing or stale (> 2× schedule), DLQ growing, retrieval p95
  over budget, rising INSUFFICIENT rate (often the first sign a source broke).
- **Conventions:** never put document ids or query text in labels
  (cardinality); put them in traces instead. OpenTelemetry tracing is an
  optional add-on, using GenAI span names.

## 6. Repository layout

```
api/rag/v1/rag.proto           # gRPC contract (buf)
api/v1alpha1/                  # + DataSource, IndexProfile, Corpus types
cmd/rag-server/                # ingestion + retrieval gRPC
cmd/rag-worker/                # index workers
cmd/rag-connector/             # one-shot sync, run as a Job
cmd/rag-mcp/                   # MCP adapter
cmd/rag-bench/                 # benchmark CLI
internal/rag/{connector,extract,chunk,embed,index,retrieve,grade,rewrite,store}/
internal/controller/           # + datasource, indexprofile controllers
config/rag/                    # Postgres, Qdrant, MinIO, NATS, monitoring manifests
```

## 7. Phases

Each phase ends with something runnable. Ownership: you write core logic; I
set up scaffolding, manifests, and wiring.

| # | Phase | Done when | You | Me |
|---|---|---|---|---|
| 0 | Foundations | stack deploys on kind; proto generates; Grafana shows targets up | proto contract | Postgres/Qdrant/MinIO/NATS/Prometheus manifests, buf, Makefile, CI |
| 1 | Core pipeline | upload a doc → chunks searchable via `Retrieve` | chunker, embedder, indexer | rag-server/worker mains, queue wiring, metrics plumbing |
| 2 | MCP + first agent | `deepresearch` answers from the corpus | MCP tool handlers | rag-mcp binary + deploy |
| 3 | Connectors | a `DataSource` for a Drive folder and a Confluence space stays in sync, deletes included | Drive + Confluence connectors | DataSource CRD + controller, Job runner, secrets |
| 4 | Benchmarking | report compares ≥3 profiles on recall/latency/cost | chunking strategies, eval metrics | IndexProfile CRD, rag-bench scaffolding, Grafana board |
| 5 | Quality layers | benchmark shows rewrite/grade lift (or not) | grader, rewriter, `Search` loop | — |
| 6 | Hardening | alerts fire correctly; ACL filter enforced | ACL enforcement, hybrid/rerank | durable jobs, auth, dashboards, alert rules |

Benchmarking (4) deliberately comes before the quality layers (5), so you can
measure what rewriting and grading actually buy instead of assuming.

## 8. Decisions needed

| Decision | Needed by | Recommendation |
|---|---|---|
| LLM + embedding provider | phase 1 | Gemini (genai SDK, already used by deepresearch) + local Ollama option |
| Retry loop owner | phase 5 | Agent-driven |
| Delegation prompt refinement in scope? | phase 5 | Later; keep originals |
| Drive auth model | phase 3 | Service account scoped to one shared folder for the playground |
| Confluence instance | phase 3 | A free Confluence Cloud site |

## 9. Risks

- **Scope.** This is a lot of moving parts. The phase gates exist so it's
  always runnable; resist starting phase N+1 before N works end to end.
- **Grader quality is the whole quality-layer feature.** A confidently wrong
  judge misleads agents. The benchmark is the mitigation.
- **Stale or forbidden content.** Missed deletes or ACL gaps leak data into
  agent prompts. Full-sync reconciliation and ACL filters address this.
- **Prompt injection via documents.** Retrieved text is untrusted: delimit it,
  never follow it as instructions.
- **Local resource use.** Postgres + Qdrant + MinIO + NATS + Prometheus on kind
  needs ~6–8GB RAM; every manifest sets resource limits.
