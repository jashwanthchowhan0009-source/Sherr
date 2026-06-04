from app.text_utils import (
    clean_html_fragments, hamming_distance, jaccard_similarity, normalize_title,
    simhash, simhash_similar, title_fingerprint, truncate_to_words, url_fingerprint,
)


def test_clean_html_strips_tags_and_entities():
    raw = "<p>Hello &amp; welcome</p><script>bad()</script> read more at example.com"
    out = clean_html_fragments(raw)
    assert "<" not in out and ">" not in out
    assert "&amp;" not in out and "&" in out
    assert "bad()" not in out


def test_normalize_title_drops_prefixes():
    assert normalize_title("Breaking: Nifty hits record!") == "nifty hits record"


def test_title_fingerprint_is_stable_and_prefix_insensitive():
    a = title_fingerprint("Breaking: Nifty hits record")
    b = title_fingerprint("Nifty hits record")
    assert a == b
    assert len(a) == 16


def test_url_fingerprint_ignores_query_and_trailing_slash():
    a = url_fingerprint("https://x.com/story/")
    b = url_fingerprint("https://x.com/story?utm=1")
    assert a == b


def test_simhash_exact_repost_and_order_independence():
    # Dedup layer 3 targets byte-identical / reordered reposts of the same body.
    base = "The central bank raised interest rates by 25 basis points today."
    reordered = "today The central bank raised interest rates by 25 basis points"
    far = "A new species of frog was discovered in the Amazon rainforest."
    assert simhash(base) == simhash(base)                       # deterministic
    assert simhash_similar(simhash(base), simhash(reordered), max_distance=3)
    assert not simhash_similar(simhash(base), simhash(far), max_distance=3)


def test_hamming_distance():
    assert hamming_distance(0b1010, 0b1000) == 1


def test_jaccard_similarity_range():
    assert jaccard_similarity("apple banana", "apple banana") == 1.0
    assert jaccard_similarity("apple", "orange") == 0.0


def test_truncate_to_words():
    assert truncate_to_words("a b c d e", 3).startswith("a b c")
