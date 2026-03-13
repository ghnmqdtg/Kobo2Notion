# CLAUDE.md — Kobo2Notion

## Project Overview

Electron desktop app that syncs Kobo e-reader highlights/bookmarks to Notion. Built with electron-vite, React (renderer), and Node.js (main/preload).

## Tech Stack

- **Framework:** Electron + electron-vite
- **Frontend:** React, TypeScript, Tailwind CSS, shadcn/ui
- **Backend:** Node.js, SQLite (Kobo DB), Notion API, AI SDK (LLM summarization)
- **Testing:** Jest
- **CI/CD:** GitHub Actions (CI on dev push, Release on main push)
- **Auto-update:** electron-updater

## Key Directories

- `src/main/` — Electron main process (updater, app lifecycle)
- `src/preload/` — Preload scripts (IPC bridge)
- `src/renderer/` — React frontend
- `src/backend/` — Core services (Kobo, Notion, LLM integration)
- `src/config/` — Environment config
- `config/` — ESLint, entitlements
- `.github/workflows/` — CI and Release workflows
- `plans/` — Documentation and troubleshooting

## Build & Test Commands

```bash
npm run build        # typecheck + electron-vite build + electron-rebuild
npm run dev          # Start dev server
npm test             # Run Jest tests
npm run lint         # ESLint
npm run format       # Prettier
```

## Release Process

1. Bump version in `package.json` on `dev`
2. Merge `dev` → `main`, push to trigger Release workflow
3. Release workflow builds for macOS (signed + notarized), Windows, Linux
4. Publish the draft release: `gh release edit vX.Y.Z --draft=false`
5. Verify auto-updater detects the new version

## Troubleshooting

**Before debugging CI/CD, release, signing, or auto-update issues, read `plans/troubleshooting.md`.** It documents every issue encountered and their solutions.

Key gotchas:

- macOS auto-updater requires `zip` target (not just `dmg`)
- macOS code signing secrets are in the "Release" GitHub Environment
- electron-builder `notarize: true` replaces custom `afterSign` hooks
- Test files use `@ts-nocheck` + eslint-disable because they're not in tsconfig

## Commit Convention

`{Type}: {Description}` — e.g., `Feat: Add X`, `Fix: Resolve Y`, `Chore: Update deps`, `Bump: vX.Y.Z`
