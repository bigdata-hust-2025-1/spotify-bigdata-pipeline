# Changelog

All notable changes to this repository are documented here. Entries are grouped
by the roadmap PR they implement.

## PR-01 — Resolve merge conflicts + add conflict-marker CI guard

**Type:** fix + ci · **Branch:** `pr-001-resolve-merge-conflicts`

### Context
Three core pipeline files were committed to `main` containing unresolved Git
merge conflict markers, making them invalid Python and taking the batch and
stream-producer paths offline (architecture design review, finding A1).

### Changed
- **`spark_jobs/batch/bronze_to_silver_all.py`** — resolved the conflict by
  **keeping** the `categorize_duration` UDF branch (it is referenced downstream
  in `transform_tracks`, so dropping it would break the job) and making the
  ingest date configurable: `INGEST_DATE = os.getenv("INGEST_DATE", "2025-12-21")`.
- **`spark_jobs/batch/silver_to_gold_all.py`** — same `INGEST_DATE` resolution.
- **`spark_jobs/stream/produce_to_kafka.py`** — kept the single topic name
  (`spotify_playback_events`) and replaced **both** hardcoded absolute Windows
  paths (`D:\...`) with a portable, repo-relative default derived from
  `__file__`, overridable via the `TRACKS_DATA_PATH` environment variable.
- **`.github/workflows/conflict-guard.yml`** (new) — CI job that fails on any
  unresolved merge conflict marker in a tracked file, on every push to `main`
  and every pull request.

### Design decisions
1. **Default `INGEST_DATE` = `2025-12-21`, not `2025-12-06`.** The two dates came
   from the two conflict sides. `2025-12-21` is already used by the
   non-conflicted batch jobs (`advanced_analytics.py`, `gold_to_es.py`), so
   defaulting to it keeps the batch layer internally consistent for anyone who
   runs it as-is.
2. **Env override instead of a hardcoded literal.** A hardcoded date is the root
   cause of this class of conflict (two people editing the same literal). Making
   it an env var with a sane default is the minimal correct fix and sets up
   PR-11/PR-12, where Airflow will inject the logical date (`{{ ds }}`).
   Full centralized config (a shared `common/config.py`) is intentionally
   deferred to PR-03 to keep this PR atomic.
3. **Portable data path derived from `__file__`.** Neither committed Windows
   path is correct on any other machine; the fix is to resolve `data/tracks.json`
   relative to the repository, not to pick one bad absolute path over the other.
   The deeper credential/config hardening remains scoped to PR-06.
4. **Precise, self-excluding guard regex.** The guard matches exactly seven
   marker characters at the start of a line followed by a space/tab/EOL boundary
   (`^(<{7}|={7}|>{7}|\|{7})([ \t]|$)`). The boundary prevents false positives on
   decorative rules (e.g. a long line of `=`), the `^` anchor ignores in-prose
   mentions, and the workflow file excludes itself so the pattern text cannot
   match its own definition. Includes the diff3 `|||||||` marker for completeness.

### Verification
- `python -m py_compile` passes for all three files.
- Working tree scans clean for conflict markers.
- The guard regex was proven to (a) match a real 3-way conflict probe and
  (b) not match a decorative long-`=` line.

### New environment variables
| Variable | Default | Used by |
| :--- | :--- | :--- |
| `INGEST_DATE` | `2025-12-21` | `bronze_to_silver_all.py`, `silver_to_gold_all.py` |
| `TRACKS_DATA_PATH` | `<repo>/data/tracks.json` | `produce_to_kafka.py` |

### Rollback
Pure code/CI change with no runtime state impact. `git revert` of the PR restores
the previous files (though those never compiled) and removes the guard workflow.
