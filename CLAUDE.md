# CLAUDE.md

## Version Bumping

**Always increment the `version` field in `package.json` when making any code changes** (bug fixes, new features, refactors, etc.). n8n only detects updated community nodes when the version number changes.

- Use **semver** (`MAJOR.MINOR.PATCH`):
  - **PATCH** bump for bug fixes and minor changes (e.g. `0.1.7` → `0.1.8`)
  - **MINOR** bump for new features or new nodes (e.g. `0.1.7` → `0.2.0`)
  - **MAJOR** bump for breaking changes (e.g. `0.1.7` → `1.0.0`)
- Bump the version in the **same commit** as the code changes, not in a separate commit.
