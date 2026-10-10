# Shipping SherrByte to Google Play

Everything in this file is work that happens **outside** the repo — in the Play
Console, in a keystore, and in the Render environment. The code side is done and
tested (`tests/test_play_store_readiness.py`, `tests/test_account_deletion.py`).

---

## The one that silently breaks the app: assetlinks vs Play App Signing

`.well-known/assetlinks.json` currently carries this fingerprint:

```
6C:34:2E:3C:CF:59:64:9D:3F:F7:9B:2C:18:12:FF:10:D6:FF:05:96:62:02:2B:22:0D:89:08:7A:FF:8D:69:53
```

That is the certificate inside `sherrbyte.apk` — subject `CN=SherrByte Admin`,
the local keystore. **It is the upload key, not the key users get.**

Play App Signing strips your signature and re-signs every release with a key
Google holds. The installed app therefore presents a *different* fingerprint
from the one above, Digital Asset Links verification fails, and the Trusted Web
Activity opens **with a browser address bar across the top**. The app still
works; it just looks like a bookmark with a URL bar — which is exactly what
Play's minimum-functionality policy rejects, and what reviewers flag as
"repackaged website".

Nothing warns you. The build succeeds, the upload succeeds, the app installs.

**Fix, after the first upload:**

1. Play Console → your app → **Test and release → Setup → App signing**.
2. Copy the SHA-256 under **App signing key certificate** (not *Upload key
   certificate*).
3. Put **both** fingerprints in `.well-known/assetlinks.json` — the Play one so
   released builds verify, the upload one so your own local/internal-test
   installs keep verifying:

```json
[
  {
    "relation": ["delegate_permission/common.handle_all_urls"],
    "target": {
      "namespace": "android_app",
      "package_name": "app.vercel.sherrbyte.twa",
      "sha256_cert_fingerprints": [
        "<SHA-256 from Play App signing key certificate>",
        "6C:34:2E:3C:CF:59:64:9D:3F:F7:9B:2C:18:12:FF:10:D6:FF:05:96:62:02:2B:22:0D:89:08:7A:FF:8D:69:53"
      ]
    }
  }
]
```

4. Deploy, then confirm it is live and correct:

```bash
curl -s https://<your-domain>/.well-known/assetlinks.json | jq .
```

It must return `200` with `Content-Type: application/json`. (It used to 404 on
Render — the file was committed but no route served it. `main.assetlinks()`
serves it now.)

5. Verify with Google's own checker:

```
https://digitalassetlinks.googleapis.com/v1/statements:list\
?source.web.site=https://<your-domain>\
&relation=delegate_permission/common.handle_all_urls
```

`"maxAge"` present and no `errorCode` means it passed. Install the release build
and confirm there is **no address bar**.

> The origin in `start_url`/`scope` (`manifest.json`), the `host` baked into the
> APK, and the domain serving `assetlinks.json` must be the same host. A redirect
> between `www.` and apex counts as a different host.

---

## The APK cannot be uploaded, and cannot currently be rebuilt

Two separate problems with `sherrbyte.apk`:

**1. Play needs an AAB.** New apps have required Android App Bundle (`.aab`)
since August 2021. An `.apk` upload is rejected at the file picker.

**2. There is no TWA source project in this repo.** `sherrbyte.apk` is a
committed binary with no `twa-manifest.json`, no Gradle project and no keystore
alongside it. You cannot bump `versionCode`, change the target SDK, or re-sign
it — every future release needs that project, and it does not exist here.

Current APK, for reference (read out of its manifest):

| | |
|---|---|
| package | `app.vercel.sherrbyte.twa` |
| versionCode / versionName | `1` / `1.0.0.0` |
| minSdk / targetSdk / compileSdk | 23 / **35** / 36 |
| permissions | `POST_NOTIFICATIONS` |
| signing cert | `CN=SherrByte Admin`, valid to 2081 |

### Rebuild it with Bubblewrap

```bash
npm i -g @bubblewrap/cli
mkdir android && cd android
bubblewrap init --manifest https://<your-domain>/manifest.json
```

Answer the prompts to match the existing app exactly — **the package name must
stay `app.vercel.sherrbyte.twa`** if you ever shipped the current APK to anyone,
since the package name is the app's permanent identity on Play.

Then, before building:

- set `"appVersionCode": 2` and `"appVersionName": "1.0.1"` in `twa-manifest.json`
  (every upload needs a higher `versionCode` than the last);
- set `"targetSdkVersion": 36` — Play requires new apps and updates to target a
  recent API level, the window moves every August, and `compileSdk` is already
  36 so this is a one-line change. Check the current requirement in the Console;
  it tells you outright if the level is too low.

Build and sign:

```bash
bubblewrap build          # produces app-release-bundle.aab + app-release-signed.apk
```

Upload `app-release-bundle.aab`. Keep the `.apk` only for sideload testing.

### Back up the keystore before you upload anything

The keystore Bubblewrap creates (plus its password and key alias) is the **only**
way to publish an update. Lose it and you cannot update this listing, ever —
you would have to publish a new app under a new package name and lose the
install base and reviews.

Store the `.keystore` file and both passwords in a password manager, off this
machine. Do **not** commit them — add to `.gitignore`:

```
*.keystore
*.jks
android/
```

(Enrolling in Play App Signing gives you a key-reset path if the *upload* key is
lost, which is a good reason to enrol. It does not protect an un-enrolled app.)

---

## Console setup you have to fill in

### Set these in the Render environment first

`legal.py` reads them at request time; `/privacy` and `/terms` print a visible
orange "not configured" marker for any that are missing, so check the live pages
after deploying.

| Variable | What it is |
|---|---|
| `LEGAL_ENTITY` | registered name that publishes the app |
| `LEGAL_CONTACT_EMAIL` | address that answers privacy requests (defaults to `support@thewhitetiger.in`) |
| `LEGAL_ADDRESS` | postal address — Play shows it publicly on the listing |
| `LEGAL_JURISDICTION` | governing law for the Terms |
| `LEGAL_EFFECTIVE` | effective date, ISO; defaults to today |

Also confirm `SITE_URL` is the real public origin — it is `sync: false` in
`render.yaml`, so it is unset until you set it, and it drives every canonical
link and sitemap entry.

### App content declarations

- **Privacy policy URL** — `https://<your-domain>/privacy`. Must load signed-out,
  with no app installed. Play re-checks it periodically; a 404 later pulls the app.
- **Data safety form.** Declare, based on what the code actually does:
  - Collected and linked to identity: *email address*, *name*, *user-generated
    content* (comments), *app interactions* (reads, likes, saves).
  - Purposes: app functionality, personalisation, account management.
  - **Not** shared with third parties for advertising. There is no ad SDK and no
    third-party analytics in the app.
  - Encrypted in transit: **yes**. Users can request deletion: **yes**.
- **Account deletion URL** — `https://<your-domain>/delete-account`. Required
  because the app has sign-up. The in-app route (Profile → Security Center →
  Delete Account) now genuinely deletes; this URL covers people who uninstalled.
- **Content rating questionnaire** — answer it as a news app. Expect a higher
  rating because of unmoderated user comments; say so honestly.
- **Target audience** — 13+ or 18+. Do **not** select a children's audience; the
  privacy policy states the app is not directed at under-13s.
- **News apps declaration** — Play has a separate News category form. Be ready
  with the publisher identity, editorial contact, and the fact that the content
  is aggregated-and-summarised from licensed/public feeds with attribution and a
  link to each source.
- **Financial features** — the app shows historical market reactions. It is
  *not* a trading or investment-advice app, and the Terms say so in plain terms.
  Expect to answer this; do not claim advisory or trading functionality.
- **Ads** — declare "No ads".

### Store listing assets you still need

| Asset | Spec | Status |
|---|---|---|
| App icon | 512×512 PNG, 32-bit | ✅ `app-icon.png` works |
| Feature graphic | **1024×500** PNG/JPG, no alpha | ❌ not in repo — required |
| Phone screenshots | 2–8, min 320px, 16:9 or 9:16 | ❌ not in repo — required |
| 7" / 10" tablet screenshots | optional, but needed to list as tablet-compatible | ❌ |
| Short description | ≤ 80 chars | ❌ |
| Full description | ≤ 4000 chars | ❌ |

Screenshots must show the real app. Do not add text claiming predictions,
returns or recommendations — it contradicts the compliance posture in `CLAUDE.md`
and reviewers do read the images.

---

## Before you hit publish

```bash
# every Play-facing route answers
for p in /privacy /terms /delete-account /.well-known/assetlinks.json /manifest.json; do
  printf '%-34s %s\n' "$p" "$(curl -s -o /dev/null -w '%{http_code} %{content_type}' https://<your-domain>$p)"
done
```

- [ ] All five return `200`, and `assetlinks.json` is `application/json`
- [ ] `/privacy` shows no orange "not configured" markers
- [ ] Digital Asset Links checker passes against the **Play** signing key
- [ ] Release build installs with **no address bar**
- [ ] Delete Account in the app actually removes the account (sign in again — it
      must be refused)
- [ ] Keystore backed up somewhere that is not this machine
- [ ] `versionCode` is higher than the last upload
- [ ] Internal testing track first, on a real device, before production

---

## Known limitation to decide on, not a blocker

A TWA is a thin shell: Play reviewers judge it on whether the *web app* is
substantial, and this one is (offline shell, push, personalisation, installable
PWA). If it is rejected under minimum-functionality anyway, the usual remedy is
to add something only the native shell can do — the notification integration
already present is the strongest argument, so make sure push is actually
configured and working before submitting, not stubbed.
