"""The test suite has to actually run on a pull request.

Nothing in this repository ran pytest until `.github/workflows/tests.yml`
existed: the other workflows are schedules, and the only check a PR carried was
a Vercel preview deploy that builds the static tree against no backend. Six
failures sat on main long enough to be quoted in PR bodies as "pre-existing".

So these assert the two things that make the workflow worth having — that it
fires on a PR, and that it runs the whole suite — plus the Python version,
which is load-bearing rather than taste.
"""
import os

import pytest

yaml = pytest.importorskip("yaml")

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_WF = os.path.join(_ROOT, ".github", "workflows", "tests.yml")


@pytest.fixture(scope="module")
def wf():
    with open(_WF, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _triggers(wf):
    # PyYAML reads an unquoted `on:` key as the boolean True, which is why the
    # cron workflow's tests index it the same way.
    return wf.get("on") or wf[True]


def test_it_runs_on_a_pull_request(wf):
    """The whole point. A workflow that only ran on push to main would report
    the failure after the merge that introduced it."""
    assert "pull_request" in _triggers(wf)


def test_it_also_runs_on_main_so_a_red_base_is_visible(wf):
    push = _triggers(wf)["push"]
    assert push["branches"] == ["main"]


def test_it_runs_the_whole_suite_and_does_not_stop_at_the_first_failure(wf):
    """`-x` would hide how much else is broken, and the suite costs ~40s."""
    step = [s for s in wf["jobs"]["pytest"]["steps"]
            if "pytest tests" in (s.get("run") or "")]
    assert len(step) == 1, "expected exactly one step running the suite"
    run = step[0]["run"]
    assert " -x" not in run and "--exitfirst" not in run
    assert "--deselect" not in run and "-k " not in run, (
        "the suite runs whole: a deselect here is how a failure becomes permanent")


def test_the_python_version_is_the_one_requirements_can_install(wf):
    """requirements.txt pins psycopg[binary]==3.2.1, which has no wheel for
    3.13 — a clean install there fails outright. 3.12 also matches the ingest
    and detector crons, which are the proof this shape installs."""
    setup = [s for s in wf["jobs"]["pytest"]["steps"]
             if str(s.get("uses", "")).startswith("actions/setup-python")]
    assert setup, "no setup-python step"
    assert str(setup[0]["with"]["python-version"]) == "3.12"


def test_it_installs_the_test_only_dependencies_too(wf):
    """pytest and PyYAML are not in requirements.txt, and should not be — but a
    job that installs only requirements.txt cannot collect this file."""
    install = " ".join(s.get("run") or "" for s in wf["jobs"]["pytest"]["steps"])
    assert "requirements.txt" in install
    assert "requirements-dev.txt" in install


def test_requirements_dev_carries_what_the_suite_imports_beyond_the_app():
    with open(os.path.join(_ROOT, "requirements-dev.txt"), encoding="utf-8") as fh:
        dev = fh.read().lower()
    for pkg in ("pytest", "pyyaml", "pydantic-settings"):
        assert pkg in dev, f"{pkg} missing from requirements-dev.txt"


def test_the_job_needs_no_secrets():
    """The suite hits sqlite and stubs only. A job with no secrets is one that
    can run on a fork's PR, and one that cannot leak a key into a build log."""
    with open(_WF, encoding="utf-8") as fh:
        raw = fh.read()
    assert "secrets." not in raw
