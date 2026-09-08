"""proof/ — the 30-day RIL proof run.

One company (Reliance Industries), four hand-authored edges, one permanent log.

The engine here does exactly one thing and nothing more: it reads the four edges
someone seeded by hand in `sherrbyte_app.ril_edges`, and for each one asks "did
two or more of this edge's observable signals move in the same window?". Every
firing is appended to `sherrbyte_app.ril_proof_log` — whether or not it would
ever render a card — because the log is the product evidence.

It never invents an edge, never emits a percentage, never touches another
company, and never adds an asset class. See the layer split:

  signals.py    signal definitions + the no-lookahead daily-move math (pure)
  data.py       reads market_ticks and the financial-source corpus
  evaluator.py  evaluates the four edges for one session/window -> firings
  backfill.py   runs the whole history backwards, logs firings, and measures
                RIL's forward reaction to report the honest hit rate
"""
