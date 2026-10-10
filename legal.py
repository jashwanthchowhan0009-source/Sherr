"""The three pages Google Play requires to be reachable on the open web.

Play will not approve a listing without a privacy-policy URL that loads
**outside** the app, with no sign-in, from the Play Console's own crawler. An
app that offers account creation additionally needs a deletion route reachable
the same way — the in-app button is necessary but not sufficient, because Play
checks that someone who has already uninstalled can still get their data
removed.

None of this existed: there was no /privacy, no /terms, and account deletion was
in-app only (and, until this pass, non-functional — see main.account_delete).

What the pages say about DATA is taken from the schema and the call sites, not
from a template:

  * `users` stores email, a salted+hashed password, name, bio, avatar_url,
    language, created_at, last_login, token_version.
  * `user_interactions`, `bookmarks`, `feeds`, `article_comments`,
    `user_preferences`, `notifications`, `push_tokens` are the per-reader rows,
    all keyed by user_id, all removed by POST /account/delete.
  * The processors are the ones the code actually calls.

What the pages say about the COMPANY is configuration, because a policy naming
the wrong legal entity or jurisdiction is worse than no policy. Set these in the
environment before launch:

    LEGAL_ENTITY          registered name that publishes the app
    LEGAL_CONTACT_EMAIL   address that answers privacy requests
    LEGAL_ADDRESS         postal address (Play shows it on the listing)
    LEGAL_JURISDICTION    governing law for the terms
    LEGAL_EFFECTIVE       effective date, ISO (defaults to today)

`legal_config_gaps()` reports which are still unset so the launch check can see
it; /admin/stats surfaces it and the pages themselves say plainly that a value
is unconfigured rather than printing a confident blank.
"""
import os
from datetime import date

_SUPPORT_FALLBACK = "support@thewhitetiger.in"

_FIELDS = {
    "LEGAL_ENTITY": "",
    "LEGAL_CONTACT_EMAIL": _SUPPORT_FALLBACK,
    "LEGAL_ADDRESS": "",
    "LEGAL_JURISDICTION": "",
    "LEGAL_EFFECTIVE": "",
}


def _cfg(key: str) -> str:
    """Read at call time, never at import — so a deploy that sets the variable
    takes effect without a code change, and the tests can monkeypatch it."""
    return (os.getenv(key) or _FIELDS.get(key, "") or "").strip()


def legal_config_gaps() -> list:
    """Which required values are still unset. Empty means ready to publish."""
    return [k for k in ("LEGAL_ENTITY", "LEGAL_CONTACT_EMAIL",
                        "LEGAL_ADDRESS", "LEGAL_JURISDICTION") if not _cfg(k)]


def _unset(label: str) -> str:
    return (f'<span class="unset">[{label} not configured — '
            f'set {label} in the environment]</span>')


def _entity() -> str:
    return _cfg("LEGAL_ENTITY") or _unset("LEGAL_ENTITY")


def _contact() -> str:
    email = _cfg("LEGAL_CONTACT_EMAIL")
    return f'<a href="mailto:{email}">{email}</a>' if email else _unset("LEGAL_CONTACT_EMAIL")


def _effective() -> str:
    return _cfg("LEGAL_EFFECTIVE") or date.today().isoformat()


_CSS = """
:root{--bg:#fff;--fg:#111;--mut:#555;--line:#e5e5e5;--acc:#1a73e8;--warn:#b26b00}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
  --bg:#0d0d0d;--fg:#ededed;--mut:#a0a0a0;--line:#262626;--acc:#7cb0ff;--warn:#e0a33c}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
  font:16px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,sans-serif;
  padding:0 16px}
main{max-width:720px;margin:0 auto;padding:40px 0 72px}
h1{font-size:1.6rem;line-height:1.25;margin:0 0 6px}
h2{font-size:1.1rem;margin:34px 0 10px;padding-top:18px;border-top:1px solid var(--line)}
h2:first-of-type{border-top:0;padding-top:0}
p,li{color:var(--fg)}
.meta{color:var(--mut);font-size:.9rem;margin:0 0 8px}
ul{padding-left:20px}
li{margin:5px 0}
a{color:var(--acc)}
code{background:rgba(127,127,127,.16);padding:1px 5px;border-radius:4px;font-size:.88em}
.unset{color:var(--warn);font-weight:600}
table{border-collapse:collapse;width:100%;margin:10px 0;font-size:.93rem;display:block;overflow-x:auto}
th,td{border:1px solid var(--line);padding:7px 9px;text-align:left;vertical-align:top}
th{background:rgba(127,127,127,.09);font-weight:600}
footer{margin-top:46px;padding-top:16px;border-top:1px solid var(--line);
  color:var(--mut);font-size:.88rem}
.btn{display:inline-block;background:var(--acc);color:#fff;text-decoration:none;
  padding:11px 18px;border-radius:9px;font-weight:600;margin:6px 0}
.note{border-left:3px solid var(--acc);padding:2px 0 2px 14px;margin:16px 0;color:var(--mut)}
"""


def _page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} · SherrByte</title>
<meta name="robots" content="index,follow">
<style>{_CSS}</style>
</head><body><main>
{body}
<footer>
  <a href="/">SherrByte</a> · <a href="/privacy">Privacy</a> ·
  <a href="/terms">Terms</a> · <a href="/delete-account">Delete your account</a>
</footer>
</main></body></html>"""


def privacy_html() -> str:
    return _page("Privacy Policy", f"""
<h1>Privacy Policy</h1>
<p class="meta">SherrByte, published by {_entity()}. Effective {_effective()}.</p>

<h2>What we collect</h2>
<p>Only what the app needs to give you an account and a feed.</p>
<table>
  <tr><th>Data</th><th>Why</th></tr>
  <tr><td>Email address</td><td>Identifies your account; password resets and security notices.</td></tr>
  <tr><td>Password</td><td>Stored only as a salted hash. We never hold the plain text and cannot recover it.</td></tr>
  <tr><td>Display name, username, bio, avatar, profile link</td><td>Optional. Shown on your profile and beside your comments.</td></tr>
  <tr><td>Language and topic interests</td><td>Orders your feed.</td></tr>
  <tr><td>Reads, likes, saves and bookmarks</td><td>Personalises ranking and powers your saved list.</td></tr>
  <tr><td>Comments you post</td><td>Published with your display name.</td></tr>
  <tr><td>Device push token</td><td>Only if you allow notifications. Deleting the app or revoking the permission ends it.</td></tr>
  <tr><td>Account timestamps (created, last sign-in)</td><td>Security and abuse handling.</td></tr>
</table>
<p>We do <strong>not</strong> collect contacts, precise location, photos, call or
SMS logs, or device identifiers for advertising. There is no advertising SDK and
no third-party analytics in the app.</p>

<h2>What we never do</h2>
<ul>
  <li>We do not sell your personal data.</li>
  <li>We do not share it with data brokers or advertisers.</li>
  <li>We do not use your reading history to build a profile for anyone but you.</li>
</ul>

<h2>Who processes data for us</h2>
<p>These providers process data on our behalf, only as needed to run the service:</p>
<ul>
  <li><strong>Supabase</strong> — the database holding your account and activity.</li>
  <li><strong>Render</strong> — application hosting.</li>
  <li><strong>Firebase Cloud Messaging</strong> (Google) — delivers push notifications, if you enable them.</li>
  <li><strong>Google Gemini</strong> and <strong>Groq</strong> — summarise and write article text. They receive <em>news content</em>, never your account data, your reading history or your comments.</li>
  <li><strong>Pexels</strong> — supplies the licensed stock imagery on cards.</li>
  <li>Market and news data providers (CoinGecko, Twelve Data, NewsAPI, Alpha Vantage, OpenWeather, NASA) — we fetch public data from them. They receive no information about you.</li>
</ul>

<h2>How long we keep it</h2>
<p>Your account data lives until you delete it. Deleting your account removes
your user record and every row tied to it — preferences, interactions,
bookmarks, feed ranking, comments, notifications and push tokens — from the
live database. Encrypted backups may retain copies for up to 30 days, after
which they expire.</p>

<h2>Deleting your account and data</h2>
<p>Two routes, both complete and both irreversible:</p>
<ul>
  <li><strong>In the app</strong> — Profile → Security Center → Delete Account.</li>
  <li><strong>On the web</strong> — <a href="/delete-account">sherrbyte.com/delete-account</a>, which works even if you have uninstalled the app.</li>
</ul>

<h2>Your rights</h2>
<p>You can access, correct, export or erase your data, and withdraw consent for
notifications at any time. Under India's Digital Personal Data Protection Act,
the GDPR and comparable laws you may also object to processing or ask us to
restrict it. Write to {_contact()} and we will respond within 30 days.</p>

<h2>Children</h2>
<p>SherrByte is not directed at children under 13, and we do not knowingly
collect their data. If you believe a child has created an account, contact us
and we will remove it.</p>

<h2>Security</h2>
<p>Traffic is encrypted in transit (HTTPS). Passwords are salted and hashed.
Sessions can be revoked on every device at once from Security Center → Log Out
All Devices. No system is perfectly secure, and we will notify affected users of
a breach as the law requires.</p>

<h2>Changes</h2>
<p>If this policy changes materially we will update the effective date above and
notify you in the app before the change takes effect.</p>

<h2>Contact</h2>
<p>{_entity()}<br>{_cfg('LEGAL_ADDRESS') or _unset('LEGAL_ADDRESS')}<br>{_contact()}</p>
""")


def terms_html() -> str:
    jurisdiction = _cfg("LEGAL_JURISDICTION") or _unset("LEGAL_JURISDICTION")
    return _page("Terms of Use", f"""
<h1>Terms of Use</h1>
<p class="meta">SherrByte, published by {_entity()}. Effective {_effective()}.</p>

<h2>The service</h2>
<p>SherrByte aggregates and summarises news from public sources and shows how
past comparable events were followed by movements in financial instruments. Using
the app means accepting these terms.</p>

<h2>Not investment advice</h2>
<div class="note">
<p>Nothing in SherrByte is investment, financial, legal or tax advice, and
nothing in it is a recommendation to buy, sell or hold any security, commodity
or currency. We are not a registered investment adviser or research analyst.</p>
<p>The app reports what has <em>already</em> happened. Scores such as
<code>signal_strength</code> describe how distinctive a past move was against a
measured noise floor — they are not forecasts, probabilities, price targets or
expected returns. Past patterns do not predict future results. Decisions you
take are yours alone; make them with a qualified, licensed adviser.</p>
</div>

<h2>Accuracy</h2>
<p>Summaries are produced automatically and may contain errors, omissions or
stale figures. Market data comes from third parties, may be delayed, and may be
wrong. Always check the original source — every card links to it — before acting
on anything. The service is provided "as is", without warranties of any kind.</p>

<h2>Your account</h2>
<p>You are responsible for keeping your credentials secure and for everything
done under your account. One person, one account. Tell us promptly at
{_contact()} if you suspect unauthorised access.</p>

<h2>Acceptable use</h2>
<ul>
  <li>No scraping, bulk downloading, or automated access outside the app.</li>
  <li>No attempt to breach, probe or overload the service or its accounts.</li>
  <li>No unlawful, hateful, harassing, misleading or infringing content in comments.</li>
  <li>No redistributing article text or imagery as if it were your own.</li>
</ul>
<p>We may remove content or suspend accounts that break these rules.</p>

<h2>Content and intellectual property</h2>
<p>Articles remain the property of their publishers and are shown with
attribution and a link to the source. Imagery is licensed stock or our own.
The SherrByte name, design and software are ours. Your comments stay yours; by
posting you grant us a non-exclusive licence to display them in the app.</p>

<h2>Liability</h2>
<p>To the fullest extent the law allows, we are not liable for indirect,
incidental or consequential loss, or for any trading or investment loss, arising
from your use of SherrByte.</p>

<h2>Ending it</h2>
<p>Delete your account at any time, in the app or at
<a href="/delete-account">/delete-account</a>. We may suspend or end access if
you breach these terms or if we discontinue the service.</p>

<h2>Governing law</h2>
<p>These terms are governed by the laws of {jurisdiction}, and its courts have
exclusive jurisdiction over any dispute.</p>

<h2>Contact</h2>
<p>{_contact()}</p>
""")


def delete_account_html() -> str:
    return _page("Delete your account", f"""
<h1>Delete your SherrByte account</h1>
<p class="meta">Permanent and immediate. There is no undo.</p>

<h2>What gets deleted</h2>
<p>Everything tied to your account, in one step:</p>
<ul>
  <li>your user record — email, password hash, name, bio, avatar and profile link;</li>
  <li>your topic interests and language;</li>
  <li>your reads, likes, saves and bookmarks;</li>
  <li>your feed ranking data;</li>
  <li>your comments;</li>
  <li>your notifications and any push tokens for your devices.</li>
</ul>
<p>Your sessions stop working on every device the moment the account is gone.
Encrypted backups expire within 30 days. Your email address is released, so you
can sign up again later with the same address — it will be a new, empty account.</p>

<h2>If you still have the app</h2>
<p>This is the quickest route, and it confirms the deletion on screen:</p>
<p><strong>Profile → Security Center → Delete Account</strong>, then type
<code>DELETE</code> to confirm.</p>

<h2>If you have uninstalled the app</h2>
<p>Email {_contact()} from <strong>the address on the account</strong>, with the
subject <strong>Delete my account</strong>. We verify that the request comes
from the account holder, delete it, and confirm by reply within 30 days — usually
much sooner.</p>
<p><a class="btn" href="mailto:{_cfg('LEGAL_CONTACT_EMAIL')}?subject=Delete%20my%20account&amp;body=Please%20delete%20my%20SherrByte%20account%20and%20all%20associated%20data.">Email us to delete your account</a></p>

<h2>Just want the notifications to stop?</h2>
<p>You do not have to delete the account. Turn notifications off in
Profile → Settings, or in your device's app settings.</p>

<h2>Questions</h2>
<p>{_contact()} · <a href="/privacy">Privacy Policy</a></p>
""")
