

def test_bypass_writes_the_same_terminal_status_as_publish_pending():
    """bypass.py wrote 'passed' while publish_pending.py wrote 'published' for the
    same state. Two names for one state means any query filtering on either
    silently misses half the rows it should see."""
    from pathlib import Path
    src = Path(__file__).resolve().parents[1].joinpath(
        "app/pipeline/bypass.py").read_text()
    assert "status = 'published'" in src
    assert "status = 'passed'" not in src
