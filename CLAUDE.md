# Project rules for Claude

## Git workflow: always work directly on `main`

This project is shared between two machines (MacBook Pro at work, KK-GAMING PC at home) via OneDrive. Both machines run Claude Code against the same working directory.

**Rules:**
- Always commit and push directly to the `main` branch.
- Never create branches — no feature branches, no machine-specific branches, no work-in-progress branches.
- Before committing, verify the current branch is `main`. If not, switch to `main` first.
- If remote has diverged, rebase onto `origin/main` and push — do not create a throwaway branch.

**Why:** A previous session on KK-GAMING created a `main-KK-GAMING` branch. Improvements (2-min bars, configurable stop-loss, MACD exit) were stranded there while production `main` ran old code, causing a position to be held 6+ hours on 2026-04-15. The branches are now consolidated on `main` and must stay that way.
