# Continuous Integration

`.github/workflows/ci.yml` runs on every push to `main` and every pull request.
It is the systemic guarantee that the P0 class of defects (a merge conflict
reaching `main`), leaked secrets, lint regressions, and Flink API breaks never
recur.

## Jobs

| Job | Gate | Fails when |
| :--- | :--- | :--- |
| `conflict-markers` | merge-conflict scan | any tracked file has a `<<<<<<<`/`=======`/`>>>>>>>`/`\|\|\|\|\|\|\|` marker (supersedes the old `conflict-guard.yml`) |
| `lint` | `ruff check .` | a lint error (Pyflakes + pycodestyle E4/E7/E9) on the maintained surface |
| `secret-scan` | `gitleaks detect --no-git` | a secret in the working tree that is not allowlisted in `.gitleaks.toml` |
| `test` | `pytest` | any unit test fails |
| `flink-build` | `mvn -f flink_jobs/pom.xml package` | the Flink job fails to compile / shade |

## Notes

- **Lint scope.** `pyproject.toml` uses Ruff's default correctness ruleset
  (`E4/E7/E9` + `F`) and excludes a set of legacy/exploratory scripts that predate
  this hardening effort (slated for cleanup in the hygiene PRs). New code
  everywhere else is linted.
- **Secret scan** runs `--no-git` (working tree, not history): the originally
  committed secrets were rotated and removed from the tree in PR-02 but remain in
  old commits and cannot be scrubbed without a history rewrite. The known,
  already-rotated `miniopass123` placeholder is allowlisted in `.gitleaks.toml`;
  any *new* secret still fails the build.
- **Dev dependencies** are pinned in `requirements-dev.txt` for reproducible runs.
  `pyspark` is imported (not run) by the pure transform tests, so no JVM is
  needed for `pytest`; tests that require Airflow/Kafka/MinIO/Spotify libraries
  skip gracefully when those are absent.

## Branch protection (owner action)

CI cannot enforce itself — enable it once in **Settings → Branches → Add branch
protection rule** for `main`:

1. **Require status checks to pass before merging** → select `conflict-markers`,
   `lint`, `secret-scan`, `test`, `flink-build`.
2. **Require branches to be up to date before merging.**
3. (Recommended) **Require a pull request before merging.**

After this, a red build blocks the merge button.
