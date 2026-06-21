# Security Notes

## Secret Handling

- Do not commit `.env`, OAuth client exports, API tokens, refresh tokens, or local credential dumps.
- Keep runtime credentials in local-only files or environment variables.
- If a credential was ever stored in this repository before publication, rotate it before making the repository public.

## Local Runtime Data

- `data/` is reserved for local SQLite state and blob storage.
- Observations, attachments, and other runtime artifacts in `data/` are not part of the public repository.
- Use sanitized fixtures or synthetic test data for reproducible examples.

## Publishing Checklist

1. Verify `git status --ignored` does not show tracked secrets or private datasets.
2. Confirm `.env` remains untracked and `.env.example` contains placeholders only.
3. Confirm no OAuth client secret export or database snapshot is staged.
4. Rotate any credential that was previously used in a tracked file.

## Automated Audit

- Run `python ./scripts/public_release_audit.py` before pushing changes to verify that newly tracked files do not contain secrets or local runtime data.
- Run `python ./scripts/public_release_audit.py --check-history` before making the repository public. This stricter mode also fails when git history contains `.env`, `client_secret.json`, `data/` payloads, or `target/`.
- GitHub Actions runs the default audit in `.github/workflows/public-release-guard.yml`, so new leaks are blocked on every PR and on pushes to `main`.
