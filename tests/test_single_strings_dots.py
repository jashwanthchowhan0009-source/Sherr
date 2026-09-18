"""Single-source articles now get Strings/Dots too (not just synthesised events).

Most rows never reach the multi-source synthesis pass, so their dossier Strings
and Dots panes sat 'pending' forever. The single-article rewrite now also emits
`strings` (a grounded causal timeline) and `dots` (the cross-domain read), and
both are validated through the SAME compliance parser synthesis uses and written
into the article's strings/dots columns.
"""
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(_ROOT, "main.py")
AIP = os.path.join(_ROOT, "ai_processor.py")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


def test_prompt_and_schema_ask_for_strings_and_dots():
    a = _read(AIP)
    # Prompt rules
    assert "10. strings" in a and "11. dots" in a
    assert "NEVER invent history" in a
    assert "NEVER fabricate a market/instrument impact" in a
    # Schema fields
    assert '"strings":' in a and '"dots":' in a and "_DOT_SCHEMA_LC" in a


def test_writer_exists_and_reuses_the_synthesis_parsers():
    m = _read(MAIN)
    assert "def _write_single_strings_dots(" in m
    block = m[m.index("def _write_single_strings_dots("):
              m.index("def _write_single_strings_dots(") + 1400]
    # ONE blocklist / ONE parser — reuse synthesis + the shared hook_check.
    assert "synthesis._parse_strings(" in block
    assert "synthesis._parse_dots(" in block
    assert "ai_processor._hook_check()" in block


def test_both_rewrite_paths_write_strings_dots():
    m = _read(MAIN)
    # It must be called from the ingest path (run_ai_batch) AND the reprocess path.
    assert m.count("_write_single_strings_dots(conn, row[\"id\"], result)") >= 2


def test_a_column_is_only_written_when_non_empty():
    # A later, richer synthesis pass must not be clobbered by an empty
    # single-source answer, so each column is written only when it has content.
    m = _read(MAIN)
    block = m[m.index("def _write_single_strings_dots("):
              m.index("def _write_single_strings_dots(") + 1400]
    assert "if strings:" in block and "if dots:" in block
