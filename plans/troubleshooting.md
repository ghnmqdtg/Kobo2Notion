# Troubleshooting Guide — Kobo2Notion

Hard-won lessons from the CI/CD, release, and auto-update pipeline. Reference this before debugging similar issues.

---

## macOS Code Signing & Distribution

### App crashes on launch after copying from DMG

**Symptom:** `EXC_CRASH (SIGABRT)` — "mapping process and mapped file (non-platform) have different Team IDs"
**Cause:** Copying an app from a read-only DMG with `cp -R` invalidates the ad-hoc code signature. macOS refuses to load frameworks whose Team ID doesn't match the main binary.
**Fix:** Either install to `/Applications` and re-sign with `codesign --force --deep --sign -`, or (better) build with a proper Developer ID certificate so the signature survives copy.

### `codesign` fails with "unsealed contents present in the bundle root"

**Symptom:** `codesign --force --deep --sign -` errors out.
**Cause:** Unexpected files/directories at the `.app/` root level (outside `Contents/`). In our case, a nested `Kobo2Notion.app` was inside the bundle root.
**Fix:** Remove the stray file/directory, then re-sign.

### Gatekeeper blocks unsigned builds

**Symptom:** macOS shows "app is damaged" or refuses to open.
**Cause:** No code signing certificate configured; app has ad-hoc or no signature.
**Fix:** Strip quarantine (`xattr -rd com.apple.quarantine /path/to/App.app`) for local testing. For distribution, use a Developer ID Application certificate with notarization.

### Setting up macOS code signing in GitHub Actions

**Required secrets** (store in a GitHub Environment, e.g., "Release"):

| Secret                        | How to get it                                                                                                       |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `CSC_LINK`                    | Export "Developer ID Application" cert from Keychain Access as `.p12`, then `base64 -i cert.p12 -o cert-base64.txt` |
| `CSC_KEY_PASSWORD`            | Password set during `.p12` export                                                                                   |
| `APPLE_ID`                    | Apple Developer account email                                                                                       |
| `APPLE_APP_SPECIFIC_PASSWORD` | Generate at appleid.apple.com → App-Specific Passwords                                                              |
| `APPLE_TEAM_ID`               | Found in Apple Developer portal → Membership                                                                        |

**Workflow:** Pass these as `env:` vars to the macOS build step. electron-builder picks them up automatically.

### `afterSign` hook vs `notarize: true` in electron-builder

**Symptom:** Double notarization or conflicting behavior.
**Cause:** electron-builder v24+ has built-in `notarize: true` support in `electron-builder.yml`. A custom `afterSign: './scripts/notarize.js'` hook does the same thing.
**Fix:** Use one or the other. Prefer the built-in `notarize: true` and remove the `afterSign` hook.

---

## Auto-Updater (electron-updater)

### Auto-updater silently fails on macOS — no update dialog

**Symptom:** App launches, no "Update Ready" dialog even though a newer release exists.
**Root causes (check in order):**

1. **Missing `zip` target.** electron-updater on macOS requires a `.zip` artifact, not `.dmg`. The DMG is for manual downloads; the updater needs a zip to extract and replace the app.
   - **Fix:** Add `zip` to `mac.target` in `electron-builder.yml`:
     ```yaml
     mac:
       target:
         - dmg
         - zip
     ```
   - Also update the release workflow to upload `*.zip` files and include them in the `gh release create` command.

2. **App is unsigned.** electron-updater verifies the code signature of the downloaded update. If the currently installed app is unsigned or ad-hoc signed, signature verification fails silently.
   - **Fix:** Build with a proper Developer ID certificate (see signing section above).

3. **`latest-mac.yml` missing from release assets.** The updater fetches this file to discover new versions.
   - **Fix:** Ensure the release workflow uploads `latest*.yml` files.

### Verifying auto-updater works

1. Install version N (must be signed, must include updater code)
2. Release version N+1 (must have `latest-mac.yml` and `.zip` in assets)
3. Relaunch version N — updater checks on launch via `checkForUpdatesAndNotify()`
4. Should see "Update Ready" dialog within seconds

### `latest-mac.yml` structure

The file must reference the zip, not just the dmg:

```yaml
version: 1.0.12
files:
  - url: App-1.0.12-arm64-mac.zip
    sha512: ...
    size: ...
path: App-1.0.12-arm64-mac.zip
sha512: ...
releaseDate: '2026-03-13T...'
```

---

## Dependency Upgrades

### `ai` SDK v5 → v6 breaking changes

- `LanguageModelV1` renamed to `LanguageModel`
- Provider packages (`@ai-sdk/google`, `@ai-sdk/openai`, `@ai-sdk/anthropic`) must be updated from v1 → v3 to return compatible types
- If you only update `ai` without updating providers, you get: `Type 'LanguageModelV1' is not assignable to type 'LanguageModel'`

### `npm audit fix --force` strategy

- Run `npm audit fix` first (non-breaking) — catches most issues
- Then `npm audit fix --force` for breaking changes — requires build + test verification
- Some vulns (like esbuild dev-server) are dev-only and low risk; acceptable to defer

---

## CI/CD Pipeline

### Two workflows trigger on push to main

**Symptom:** Both "Release" and "CI" workflows run; CI may fail.
**Cause:** Both workflows have `on: push: branches: [main]` triggers.
**Impact:** CI failures on main are independent of Release success. Pre-existing lint errors don't block releases.

### `.gitignore` corruption when appending programmatically

**Symptom:** Lines merge together (e.g., `tmp_*.claude/worktrees/`).
**Cause:** The file didn't end with a newline, so `echo "..." >> .gitignore` appended to the last line.
**Fix:** Always verify the file ends with a newline before appending, or use a proper edit tool.

---

## Release Checklist

When creating a new release:

1. Bump version in `package.json`
2. Commit and push to `dev`
3. Merge `dev` → `main`, push to trigger Release workflow
4. Wait for all 3 platform builds (macOS, Windows, Linux) to succeed
5. Verify release assets include: `.dmg`, `.zip`, `.exe`, `.AppImage`, `.deb`, `latest-mac.yml`, `latest-linux.yml`, `latest.yml`
6. Publish the draft release via `gh release edit vX.Y.Z --draft=false`
7. Verify auto-updater detects the new version from a previous install
