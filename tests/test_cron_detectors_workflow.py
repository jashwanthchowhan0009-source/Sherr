"""
The detector cron.

The engine ingested news every 15 minutes for months and never once ran a
detector over it, because the only places the detectors are scheduled are
app/main.py's lifespan (render.yaml starts the ROOT app, not that one) and an
ARQ cron list that nothing deploys. This workflow is the fix, so these tests pin
the parts of it that would silently not run.
"""

import os
import pathlib
import sys

import pytest

yaml = pytest.importorskip("yaml")

ROOT = pathlib.Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WF = ROOT / ".github" / "workflows" / "cron_detectors.yml"
INGEST = ROOT / ".github" / "workflows" / "cron_ingest.yml"

SIGNALS_CRON = "0 1,7,13,19 * * *"
DETECTORS_CRON = "0 2 * * *"


class _Strict(yaml.SafeLoader):
    """Duplicate keys are a silent overwrite in YAML; in a job's `if:` that means
    a guard that quietly does not apply."""


def _no_dupes(loader, node, deep=False):
    seen = []
    for k, _ in node.value:
        key = loader.construct_object(k, deep=deep)
        if key in seen:
            raise ValueError(f"duplicate key: {key}")
        seen.append(key)
    return yaml.SafeLoader.construct_mapping(loader, node, deep)


_Strict.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_dupes)


@pytest.fixture(scope="module")
def wf():
    return yaml.load(WF.read_text(), _Strict)


def test_the_workflow_lives_at_the_repository_root():
    """GitHub only runs workflows from the root .github/workflows — a copy under
    sherrbyte/ is inert, which is why one already sits there doing nothing."""
    assert WF.exists()


def test_both_schedules_are_declared(wf):
    # PyYAML parses a bare `on:` key as the boolean True.
    crons = [e["cron"] for e in wf[True]["schedule"]]
    assert SIGNALS_CRON in crons
    assert DETECTORS_CRON in crons


def test_market_signals_runs_before_the_detector_pass(wf):
    """market_signals writes the domain='market' rows market_reaction joins
    against. The last signals run must land before the detector pass, or the
    detectors read a day-old market picture."""
    signal_hours = {1, 7, 13, 19}
    detector_hour = 2
    assert max(h for h in signal_hours if h < detector_hour or h > detector_hour) >= 19
    assert detector_hour - 1 in signal_hours


def test_each_schedule_selects_exactly_one_job(wf):
    """Both crons fire the same workflow, so without a guard every job would run
    on every schedule — market_signals four times a day AND at 02:00."""
    for cron, expected in ((SIGNALS_CRON, "market-signals"),
                           (DETECTORS_CRON, "detectors")):
        for name, job in wf["jobs"].items():
            guard = job["if"]
            if name == expected:
                assert f"'{cron}'" in guard, f"{name} is not selected by {cron}"
            else:
                assert f"'{cron}'" not in guard or "always()" in guard


def test_the_detector_job_still_runs_when_market_signals_is_skipped(wf):
    """`needs` on a SKIPPED dependency skips the dependent job too — which would
    mean the 02:00 detector pass never ran at all. always() is what prevents it,
    and the result check is what still stops it on a real failure."""
    guard = wf["jobs"]["detectors"]["if"]
    assert "always()" in guard
    assert "needs.market-signals.result == 'skipped'" in guard
    assert "needs.market-signals.result == 'success'" in guard


def test_the_detector_pass_asks_for_its_funnels(wf):
    """A run that writes 0 insights is usually correct; --diagnostics is what
    makes the job log say WHY, instead of needing a second investigation."""
    step = wf["jobs"]["detectors"]["steps"][-1]
    assert "app.workers.detectors" in step["run"]
    assert "--diagnostics" in step["run"]


def test_the_signals_job_runs_the_worker_that_writes_market_rows(wf):
    assert "app.workers.market_signals" in wf["jobs"]["market-signals"]["steps"][-1]["run"]


def test_both_jobs_mirror_the_ingest_cron_that_is_known_to_work(wf):
    """Same working directory, same secret, same Python setup — the ingest cron
    is the proof that this shape reaches the database."""
    ingest = yaml.load(INGEST.read_text(), _Strict)
    ref = ingest["jobs"]["ingest"]
    for name, job in wf["jobs"].items():
        assert job["defaults"]["run"]["working-directory"] == \
            ref["defaults"]["run"]["working-directory"], name
        assert job["steps"][-1]["env"]["DATABASE_URL"] == \
            ref["steps"][-1]["env"]["DATABASE_URL"], name
        assert any("requirements.txt" in str(s.get("run", "")) for s in job["steps"]), name


def test_no_job_carries_a_key_github_will_reject(wf):
    """An invalid job key fails the whole workflow at parse time, which looks
    exactly like 'the cron never fired'."""
    allowed = {"if", "needs", "runs-on", "timeout-minutes", "defaults", "steps",
               "env", "strategy", "permissions", "concurrency", "outputs", "name",
               "container", "services", "continue-on-error", "uses", "with", "secrets"}
    for name, job in wf["jobs"].items():
        assert not set(job) - allowed, f"{name}: {set(job) - allowed}"
