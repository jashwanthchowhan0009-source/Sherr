"""filings/ — exchange & regulator FILINGS as a first-class ingestion source.

The analog engine had nothing to say because the financial corpus was ~38
articles deep. This package fixes the SOURCE, not the engine: it ingests
structured filings from the two Indian exchanges (BSE, NSE corporate
announcements) and the two regulators (RBI, SEBI press releases), which together
publish hundreds of filings a day.

The layer split, deliberately kept so the doctor can run without the database:

  sources.py    the ONE registry of the four sources + their documented shapes
  classify.py   filing-type -> event_class, by RULES, no LLM, zero provider calls
  parse.py      pure parsers: documented raw shape -> Filing dataclass (+ failures)
  doctor.py     fetch each source live and REPORT (status, sample raw, parsed/failed)
  ingest.py     fetch -> parse -> resolve entity/instrument -> upsert (the DB path)

Nothing here ever calls a language model. A filing is evidence, not prose to
rewrite; event_class comes from a keyword table you can read and argue with.
"""
