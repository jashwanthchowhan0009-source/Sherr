# SherrByte Intelligence Processing Engine (Sherr-I)
## Engineering Research Document v1.0

*Role: Principal Research Scientist / Chief Architect review. Scope: proven algorithms only — nothing invented, nothing speculative. Every recommendation is tiered (V1 = now, V1.1 = weeks, V2 = post-funding, V3 = scale) and grounded in the existing stack: FastAPI · Supabase Postgres + pgvector · Redis Streams · Gemini→Groq cascade.*

---

## 0. Design philosophy (the rules everything below obeys)

1. **Deterministic before neural.** LLMs are used exactly twice in the pipeline (extraction, rewrite). Every downstream computation — graphs, patterns, ranking, scoring — is deterministic, explainable, and re-runnable. This is what makes enterprise trust and audit possible, and it is the opposite of the "everything is an LLM call" architecture that is currently fashionable and currently bankrupting startups on inference costs.
2. **Postgres until a named metric says otherwise.** Every algorithm below is chosen for implementability in SQL + Python workers on one Postgres. Each layer lists its *graduation trigger* — the measurement that justifies new infrastructure. Architecture that adds Neo4j/Kafka/Spark on day one is résumé-driven engineering.
3. **Materialize at write time, serve reads cheap.** Heavy computation happens on ingest or on schedule and lands in tables. API routes only SELECT. This single rule is what makes 100k users cheap.
4. **Everything emits evidence.** Every derived fact carries `{source_ids, counts, confidence, method}`. Explainability is not a feature layer — it is a column on every table.

---

## 1. Free & low-cost data sources (curated, India-first)

Not exhaustive — *curated for what strengthens Sherr-I's actual positioning* (external market/narrative intelligence, India-native). Each entry: what it provides / free limits / why it matters / tier.

### Tier V1 — plug in now (adapter each, ~1 day each)
| Source | Provides | Free limits | Why it matters |
|---|---|---|---|
| **GDELT 2.0** | Global news events, entities, tone, geo — updated every 15 min | Fully free (BigQuery public dataset + raw files) | The single highest-value addition: pre-extracted events + tone at world scale; cross-validates your own extraction |
| **Wikidata** | Canonical entities, aliases, relationships (P-properties) | Free API, dumps | **Solves entity resolution aliases at scale** — "TaMo"→Q53268 (Tata Motors); seed `entity_aliases` from it instead of hand-curating |
| **RBI Database on Indian Economy (DBIE)** | Rates, money supply, forex reserves | Free | India macro signals for cross-domain chains |
| **data.gov.in** | 100k+ Indian govt datasets (agriculture, prices, transport) | Free API (key) | Mandi prices ↔ weather ↔ news chains — a uniquely Indian moat foreign tools ignore |
| **NSE/BSE bhavcopy** | Official daily OHLC + delivery data | Free downloads | Ground-truth prices for event→price detector (replaces scraped/unofficial feeds) |
| **Open-Meteo** | Weather + historical | Free, no key, 10k req/day | Already in your weather adapter's class; historical API enables backtests |
| **SEC EDGAR** | US filings full-text + real-time | Free (rate-limited 10 req/s) | For India-listed ADRs and FII narrative tracking |

### Tier V1.1–V2
| Source | Provides | Why |
|---|---|---|
| **OpenCorporates** | 200M+ company records | Entity enrichment for B2B reports (free for open data projects, else API tiers) |
| **OpenAlex** | 250M scholarly works, citations | Research-domain adapter; "emerging tech" detection via citation velocity |
| **Crossref** | DOI metadata, free | Cheap research-paper metadata without OpenAlex's volume |
| **MCA (India) via API Setu** | Indian company registry | KYC-grade entity data for enterprise reports |
| **Common Crawl** | Web-scale corpus | V2+ only — storage/compute heavy; use targeted crawls first |
| **GitHub Archive** | All public GitHub events | Tech-sector emergence signals (new repo velocity around a topic) |

**Rejected for now:** satellite imagery (Planet/Sentinel — processing cost ≫ value for narrative intelligence), Common Crawl at V1 (infra), Twitter/X API (pricing hostile; velocity detector works on multi-source news instead).

---

## 2. Algorithm survey

Format per entry: **Purpose · Complexity · Where it fits in Sherr-I · Verdict (tier + why)**. Industry-standard citations noted where the pedigree is the argument.

### 2.1 Graph theory

| Algorithm | Purpose | Complexity | Sherr-I fit | Verdict |
|---|---|---|---|---|
| **BFS/DFS** | Traversal, reachability, k-hop neighborhoods | O(V+E) | Entity exploration ("what's near RBI"), path evidence for chains | **V1** — recursive CTE, hop-capped at 2–3, fan-out capped on hubs |
| **Connected Components** | Find isolated subgraphs | O(V+E) | Story clustering sanity check; orphan entity detection | **V1** — trivial in SQL over cooccurrence |
| **Degree Centrality** | Who is structurally important | O(E) | Influence ranking, hub detection | **V1** — a GROUP BY; already implied by cooccurrence |
| **PageRank** | Importance weighted by neighbors' importance (Google, 1998 — the canonical "importance" algorithm) | O(k·E) iterative | Entity importance for ranking and report ordering | **V1.1** — Python batch job (networkx/scipy on the cooccurrence edge list), nightly, results to a column. NOT in SQL |
| **Personalized PageRank** | Importance *relative to* a seed node (basis of Twitter's Who-To-Follow) | O(k·E) | "What matters around Adani" — per-client entity ranking for B2B reports | **V2** — killer B2B feature; needs stable graph first |
| **Louvain / Leiden** | Community detection via modularity (Leiden fixes Louvain's disconnected-community bug — use Leiden) | ~O(E log V) | Auto-discovered topic clusters ("the crypto-regulation cluster") | **V2** — python-igraph batch; meaningful only once graph is dense |
| **Label Propagation** | Cheap community detection | O(k·E) | Same as above, lower quality, faster | Skip — if doing communities at all, Leiden's quality is worth it |
| **Betweenness Centrality** | Bridge nodes between communities | O(V·E) — expensive | "Broker" entities connecting two narratives | **V2, sampled** (Brandes approximation); exact is unaffordable |
| **Jaccard Node Similarity** | Structural analogs via neighbor overlap | O(Σdeg²) candidate-restricted | "Companies like X", hidden-link candidates | **V1.1** — SQL over cooccurrence, only for pairs sharing ≥1 neighbor, never all-pairs |
| **Adamic-Adar / Common Neighbors** | Link prediction (the classic non-ML baselines — still competitive) | 2-hop bounded | **Hidden Link detector**: pairs sharing many neighbors but never co-occurring | **V1.1** — high demo value, cheap |
| **node2vec / graph embeddings** | Structural similarity via random walks | Training cost | Better similarity than Jaccard | **V3** — pgvector already gives text-embedding similarity; graph embeddings are a refinement, not a need |
| **SimRank** | Recursive role similarity | O(V²·d²) | — | **Reject** — cost/benefit fails vs Jaccard + embeddings |

**Knowledge-graph construction verdict:** V1 KG = `entities` + `cooccurrence` (implicit edges) + typed `relationships` table filled *lazily* by LLM extraction where confidence is high. Do **not** attempt full ontology (RDF/OWL/SPARQL) — every startup that started with an ontology died maintaining it. Property-graph-in-Postgres until traversal p95 > 500ms at 3 hops, *then* evaluate Neo4j/Memgraph.

### 2.2 Information retrieval

| Algorithm | Purpose | Sherr-I fit | Verdict |
|---|---|---|---|
| **BM25** | Lexical ranking (the 30-year IR workhorse; default in Elasticsearch/Lucene) | Keyword search over articles | **V1** — Postgres FTS (`ts_rank_cd` is BM25-adjacent) or `pg_search` extension; do NOT add Elasticsearch |
| **TF-IDF** | Term weighting | Keyphrase extraction input | V1 internally; superseded by BM25 for ranking |
| **HNSW ANN** | Approximate nearest neighbor (Malkov & Yashunin 2016; the industry-standard ANN graph) | Semantic search | **✅ Already have it** — pgvector HNSW. FAISS unneeded until >10M vectors or recall issues |
| **Hybrid search (BM25 + vector, RRF fusion)** | Combines lexical precision + semantic recall; Reciprocal Rank Fusion is the standard no-tuning merger | The search engine | **V1.1** — this IS your AlphaSense module: one endpoint, two queries, RRF merge in Python. ~2 days of work |
| **Cross-encoder reranking** | Precision re-scoring of top-k | Search quality | **V2** — a small hosted reranker over top-50 only; measure before adding |
| **Learning to Rank (LambdaMART)** | ML-trained ranking | — | **V3** — needs click data volume you don't have |

### 2.3 Pattern discovery

| Algorithm | Purpose | Sherr-I fit | Verdict |
|---|---|---|---|
| **Co-occurrence analysis + PMI/NPMI** | Association strength discounting popular entities | Core edge weighting — PMI upgrade stops "Modi co-occurs with everything" noise | **V1.1** — add `npmi` column to cooccurrence; one UPDATE query |
| **FP-Growth** | Frequent itemset mining without candidate explosion (superseded Apriori ~2000) | Recurring entity *sets* (not just pairs): {RBI, NPCI, UPI} as a motif | **V2** — mlxtend batch job; pairs suffice for V1 demos |
| **Apriori** | Same, older | — | **Reject** — FP-Growth dominates it |
| **Sequential pattern mining (PrefixSpan)** | Ordered patterns: A then B then C | Event-chain templates ("raid → resignation → stock drop") | **V2** — needs reliable event typing first |
| **Burst detection (Kleinberg 2002)** | Rigorous "burst" state detection via infinite-state automaton — the academic standard behind trend detection | Upgrade to velocity detector when z-scores prove noisy | **V2** — z-score + slope is the right V1 |
| **Temporal correlation (lagged Pearson/Spearman)** | Leading-indicator pairs | **Shipped** (PR #144) | ✅ V1. Add Spearman variant V1.1 (robust to outliers) |
| **Concept drift / topic evolution** | How narratives mutate | Story evolution tracking via embedding centroid drift per entity per week | **V2** — cheap version: cosine distance between weekly centroids |
| **Anomaly detection (z-score, MAD, IQR)** | Deviation from baseline | Volume/sentiment anomalies per entity | **V1.1** — use **MAD** (median absolute deviation), not σ: news volume is heavy-tailed and σ-based z-scores false-alarm constantly. This choice matters |
| **CUSUM / change-point (PELT)** | Detect *when* a regime changed | "Narrative turned negative on date X" — precise dates for B2B reports | **V1.1 CUSUM** (20 lines of Python, sequential, cheap), PELT via `ruptures` V2 |
| **EWMA** | Smoothed baselines | Baseline for anomaly/velocity detectors | **V1.1** — one-line recurrence, better than rolling mean for streaming |

### 2.4 Time series

Rolling windows (**V1**, have it) · EWMA (**V1.1**, above) · CUSUM (**V1.1**, above) · Cross-correlation with lags (**V1**, shipped as temporal detector) · STL decomposition for seasonality (**V2** — weekly news cycles will pollute trends; statsmodels batch) · **Forecasting (ARIMA/Prophet): Reject for product.** SherrByte detects and explains; it does not predict. This is a legal/credibility position (SEBI), not just engineering.

### 2.5 Network science
Covered by 2.1 for structure. Additions: **Independent Cascade / Linear Threshold propagation models — V3** (influence simulation for PR clients: "if this story starts here, where does it spread"); **scale-free/small-world analysis — reject as product** (descriptive academics, no customer value).

### 2.6 Recommendation systems

| Approach | Sherr-I fit | Verdict |
|---|---|---|
| **Content-based (embedding similarity + interest profile)** | Feed personalization | **✅ Have it** — formalize: user vector = EWMA of read-article embeddings |
| **Item-item collaborative filtering** (Amazon 2003 — the classic) | "Readers of X read Y" | **V1.1** — precomputed item-item cosine on interaction matrix, nightly; works at your DAU |
| **Matrix factorization (ALS)** | Latent factors | **V2** — needs more interaction density |
| **Session-based (SASRec etc.)** | Sequence-aware recs | **V3** — transformer overkill at current scale |
| **Multi-armed bandit (ε-greedy)** | Explore/exploit for feed slots | **V2** — one feed slot serves exploration; solves filter-bubble + cold-start cheaply |

### 2.7 NLP

| Technique | Verdict |
|---|---|
| **NER** | ✅ Have (LLM extraction). Add **GLiNER** (small, zero-shot, free) as V1.1 cross-check to cut LLM hallucinated entities — extraction disagreement → flag for review. Cheap insurance |
| **Entity linking** | **V1.1** — link canonical entities to Wikidata QIDs; inherits aliases, types, and industry codes for free. Highest-leverage NLP upgrade available |
| **Relationship extraction** | Keep lazy/high-confidence-only (locked decision). Typed predicates V2 |
| **Topic modeling** | **BERTopic-style** (embed → UMAP → HDBSCAN) **V2**; classic LDA **reject** (embedding-cluster approaches dominate it on short news text) |
| **TextRank / keyphrase** | **V1.1** — headline keyphrases for report generation; 50 lines, no model |
| **Coreference resolution** | **Reject** until sentence-level extraction exists — article-level entity lists don't need it |
| **Event extraction (typed: acquisition/raid/launch/policy)** | **V2** — prerequisite for event chains + PrefixSpan; do via constrained LLM output with strict schema |

### 2.8 Decision intelligence

| Technique | Verdict |
|---|---|
| **Rule engine** | **V1** — cross-domain chains ARE rules. Keep rules as data (YAML/table), not code, so adding a chain = adding a row |
| **Evidence aggregation — log-odds / naive Bayes combination** | **V1.1** — replace ad-hoc confidence with: each evidence source contributes log-likelihood ratio; sum → sigmoid → calibrated confidence. Simple, explainable, principled |
| **Bayesian updating** | V2 formalization of the above |
| **Risk scoring** | **V2** — weighted composite over (volume anomaly, sentiment drop, centrality, velocity); weights start hand-set, tuned by eval loop |
| **Calibration (Platt/isotonic)** | **V2** — once eval data exists, calibrate confidence so "80%" means 80%. Enterprises notice |

### 2.9 Data engineering

| Technique | Verdict |
|---|---|
| **Entity resolution** | ✅ Shipped (PR #141). Upgrade path: Wikidata linking (V1.1) → **Splink** (probabilistic Fellegi-Sunter record linkage, used by UK govt) when enterprise data arrives (V2) |
| **Dedup — MinHash/SimHash** | **V1.1** — syndicated/wire content inflates all counts (same PTI story in 30 outlets = fake burst). SimHash on cleaned text, 64-bit, Hamming ≤3 → same story. **This protects every detector's integrity** — arguably the most important V1.1 item |
| **Schema matching / data fusion** | V2-V3, enterprise uploads era |
| **Knowledge fusion (truth-finding)** | V3 — source-reliability-weighted claim resolution; needs claim extraction first |

---

## 3. Sherr-I layered architecture

Eight layers; each: purpose · algorithms · tables · failure cases · scaling trigger. (Layers 1–3 largely shipped — listed for completeness and gaps.)

### L1 Ingestion
Collectors → Redis Streams. **Add:** GDELT + data.gov.in + bhavcopy adapters; per-source rate budgets; dead-letter stream for poison messages. *Failure:* source schema drift → adapter versioning + contract tests. *Trigger:* queue lag > 5 min sustained → add workers.

### L2 Normalization
Cleaning, quality score, **SimHash dedup (new)**, entity resolution → canonical ids, adapters → `domain_signals`. *Failure:* over-merge in entity resolution (two companies fused) → alias table is append-only + manual unmerge log. *Trigger:* extraction disagreement rate (LLM vs GLiNER) > 15% → prompt/model revision.

### L3 Knowledge
Tables: `entities`, `entity_aliases` (+`wikidata_qid`), `domain_signals`, `articles`, embeddings (pgvector HNSW), `cooccurrence` (+`npmi`), lazy `relationships`. *Failure:* hub-entity blowup (Modi connects to everything) → NPMI weighting + fan-out caps. *Trigger:* 3-hop traversal p95 > 500ms → evaluate graph DB (this is the ONLY Neo4j trigger).

### L4 Pattern Intelligence
The detectors (§4) as scheduled jobs → `insights` with evidence. *Failure:* threshold drift as corpus grows → thresholds live in a `detector_config` table, tuned by eval loop, never hardcoded. *Trigger:* detector runtime > 30 min nightly → incremental computation or dedicated worker.

### L5 Decision Intelligence
Rule table for chains; log-odds evidence aggregation → calibrated confidence; risk composite (V2). Language constraint enforced here: *observed/detected*, never *predicted/advised*.

### L6 Search
Hybrid retrieval: Postgres FTS (BM25) + pgvector kNN → RRF fusion → filters (entity/date/domain/VIBGYOR). One endpoint. *Trigger:* recall complaints or >5M docs → reranker, then dedicated search infra.

### L7 Serving Intelligence
`/patterns`, `/search`, feed, B2B report generator (insights + PageRank ordering + keyphrases → PDF/email). Redis cache + CDN, 60s TTL on hot endpoints. Reads only.

### L8 Learning loop
Eval sampling (precision per detector per week) → `detector_metrics` → threshold auto-tune within bounds → weekly report. Item-item CF retrain nightly. *This layer is the compounding moat; it is a scheduled job, not a platform.*

---

## 4. Pattern Engine detector catalog

Deterministic, no model training. Format: **signal · method · tier**.

**Shipped:** 1. Emergence (new pairs) · 2. Temporal correlation (lagged) — PR #144.

**V1.1 (each ≈1–3 days, all on existing tables):**
3. **Velocity/burst** — story cluster source-count slope, EWMA baseline, MAD z-score
4. **Volume anomaly** — entity mention spike vs MAD baseline
5. **Sentiment shift** — CUSUM on entity sentiment stream → change date
6. **Relationship strength delta** — NPMI(t) vs NPMI(t-1) rising pairs
7. **Hidden link** — Adamic-Adar high, direct co-occurrence zero
8. **Dying narrative** — velocity negative, volume decaying (PR clients pay for "it's over")
9. **Event→price reaction** — cluster timestamp × bhavcopy windows (shipped as design; wire to official data)

**V2:**
10. Topic evolution (weekly centroid drift) · 11. Community evolution (Leiden snapshots diffed) · 12. Entity role shift (centrality trajectory) · 13. Event chains (typed events + PrefixSpan) · 14. Cross-domain chains v2 (learned lag priors from #2) · 15. Weak signals (low-volume + high-novelty + credible-source composite) · 16. Influence propagation (source→source lag graph: who breaks stories, who follows) · 17. Coordinated narrative detection (burst + near-duplicate text across low-credibility sources — misinformation flag, government-relations gold) · 18. Knowledge-gap (expected-but-missing edges among structural analogs)

**V3:** 19. Causal indicators (Granger-style tests, heavily caveated, research mode) · 20. Hypothesis generation (link prediction framed as testable watchlist items)

Every detector writes: `type, entities, score, explain_json{method, evidence_ids, counts, baseline, confidence}` — uniform schema so the app, reports, and eval loop treat all detectors identically.

---

## 5. Rejected alternatives (decision record)

| Rejected | In favor of | Why |
|---|---|---|
| Neo4j day one | Postgres + trigger | Traversal need unproven; ops burden; the trigger is written down |
| Kafka | Redis Streams | Same semantics at your scale; Kafka is a team's job |
| Elasticsearch | Postgres FTS + pgvector + RRF | One database; hybrid search covers the need |
| Airflow/Prefect | APScheduler + Streams | Orchestrator for <20 jobs is overhead |
| LDA | Embedding clustering | Short-text performance; you already pay for embeddings |
| Apriori | FP-Growth | Strictly dominated |
| Forecasting products | Detection products | Credibility + SEBI exposure; "we explain what happened first" is the honest sell |
| Ontology/RDF | Property tables | Maintenance kills; schema-on-need wins |
| Microservices | Modular monolith + workers | One team; deploy simplicity; split on measured pain |

---

## 6. Ten-year sanity

The bet that must stay true in 2036: **connected, explainable, evidence-carrying knowledge outlives any particular model.** LLMs will commoditize extraction further (good — L2 gets cheaper and better); the graph, the signal history, the calibrated detectors, and the per-client relevance profiles compound and do not commoditize. Every layer above is model-agnostic: swap Gemini/Groq for anything; L3–L8 do not notice. That is the definition of owning your intelligence rather than renting it.

*End of document.*
