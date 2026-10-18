"""No branch but `main` gets a Vercel deployment, and `main` always does.

The preview deploy was the only check on a PR before `tests.yml` existed, and it
still cannot say anything useful: `vercel.json` carries headers and no backend,
so a preview serves index.html against no API and the deck renders as skeletons.
It also fails for reasons no diff can reach — the deployments on #269 died with
`BUILD_FAILED / "Resource provisioning failed"` in about a second, before a
build container was allocated, so not one file in the tree was read.

So previews are off for every branch, and `main` — which is the production
deploy, the origin the TWA and every browser load, and the one host
`assetlinks.json` has to be served from — is left alone.

The two patterns are the whole guarantee, and they are written so that NEITHER
can match `main`, rather than disabling everything and re-enabling `main`:

  * `*/**` is every branch whose name contains a slash (`claude/…`, `feat/…`).
    A minimatch `*` does not cross a `/`, which is why the bare `*` the first
    version of this file used never covered `feat/top10-video-studio`.
  * `!(main)` is every single-segment branch that is not `main`.

A map entry resolving to `true` for `main` would depend on Vercel preferring an
exact key over a glob, which its documentation does not promise; under the rule
it does document — "any unspecified branch defaults to true" — an unmatched
`main` deploys, whatever the matcher does with the rest.
"""

import json
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Every branch namespace this repo has used, plus the production branch.
_DISABLED = ("claude/skip-vercel-previews", "feat/top10-video-studio", "dev")
_PRODUCTION = "main"


def _deployment_enabled():
    with open(os.path.join(_ROOT, "vercel.json"), encoding="utf-8") as fh:
        return json.load(fh)["git"]["deploymentEnabled"]


def test_main_is_never_named_so_it_keeps_deploying_to_production():
    rules = _deployment_enabled()
    assert isinstance(rules, dict), (
        "a bare `false` disables automated deployments on every branch, main "
        "included — that is the production deploy the TWA loads"
    )
    for pattern, enabled in rules.items():
        assert enabled is False, (
            f"{pattern!r} is enabled explicitly; main is meant to deploy by "
            "being unmatched, not by out-ranking a glob"
        )
        assert pattern != _PRODUCTION
        # A pattern that crosses path separators would swallow `main` too.
        assert pattern not in ("*", "**", "**/*"), (
            f"{pattern!r} matches the production branch"
        )


def test_every_other_branch_is_covered():
    patterns = set(_deployment_enabled())
    assert "*/**" in patterns, "branches with a slash (claude/…, feat/…) still deploy"
    assert "!(main)" in patterns, "a single-segment branch still deploys"
    for branch in _DISABLED:
        covered = ("*/**" if "/" in branch else "!(main)") in patterns
        assert covered, f"{branch} would still get a preview deployment"
