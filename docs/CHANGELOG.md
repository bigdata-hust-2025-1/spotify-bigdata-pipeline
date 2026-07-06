# Changelog

All notable changes to this repository are documented here. Entries are grouped
by the roadmap PR they implement.

## PR-22 — Docs split: Implemented vs Roadmap + quickstart + ERD

**Type:** docs · **Branch:** `pr-022-docs-split` (off `main`) · **final roadmap PR**

### Context
The docs presented aspiration as built — the README architecture diagram showed
Trino, an alert system, and a dashboard as if shipped (finding K1, the review's
central gap). Now every claim maps to merged code or is labelled roadmap.

### Added / Changed
- **`README.md`**
  - **Implemented vs Roadmap** — an honest two-part status table: each shipped
    component linked to its code + PR, and the still-aspirational pieces (Trino,
    alert-system consumer, full dashboard, raw-events-to-lake, the
    Iceberg-format cutover) clearly labelled roadmap.
  - **Star-schema ERD** — a rendered Mermaid `erDiagram` (SCD2 dims →
    `fact_playback` at event grain), summarising `docs/DATA_MODEL.md`.
  - **Quickstart** — end-to-end: local tests (`pytest`/`ruff`), `.env` setup,
    Kustomize infra apply, batch ETL run, and the MLOps loop.
  - **Docs index** — links to every shipped doc.
  - A note under the architecture diagram flagging it as the *target* design.

### Acceptance criteria
- [x] Every doc claim maps to running code or is clearly labelled roadmap.
- [x] A reader can run the pipeline from the quickstart.
- [x] ERD present (Mermaid, GitHub-rendered).

The untracked, generated `docs/DATA_ENGINEERING_KNOWLEDGE_BASE.md` is
intentionally not modified (not part of the tracked source).

### Rollback
Docs-only; revert restores the prior README. No runtime impact.

---

> **Roadmap complete.** With PR-22 merged, all 22 roadmap PRs (PR-01…PR-22)
> across Phases 0–3 are delivered.

## PR-21 — Naming & typo cleanup (`sliver` → `silver`)

**Type:** refactor (naming) · **Branch:** `pr-021-naming-typo` (off `main`)

### Context
`minIO/get_data_sliver.py` misspelled "silver" in its filename (finding K3).

### Added / Changed
- **Renamed** `minIO/get_data_sliver.py` → `minIO/get_data_silver.py`. Nothing
  imported it by module name, so no import updates were needed; its content
  already used the correct `spotify-silver` bucket and required no change.
- **`minIO/README.md`** — updated the reference to the corrected filename.

The `"Silver Sliver"` string in `data/tracks.json` is a track **name** (data),
not a typo, and is left as-is.

### Acceptance criteria
- [x] No `sliver` typo in code/filenames (`git grep` clean except the data value).
- [x] Imports updated (none referenced the old name).
- [x] Code identifiers consistent.

### Rollback
Pure rename; revert restores the old filename. No behaviour change.

## PR-20 — Repo hygiene: untrack artifacts, LICENSE, module READMEs

**Type:** chore (hygiene) · **Branch:** `pr-020-repo-hygiene` (off `main`)

### Context
The repo tracked IDE config (`.idea/`) and generated pipeline data
(`data/tracks_csv_output/`, `minIO/data/**.parquet`, `minIO/tracks_full.csv`),
had **no LICENSE**, and most top-level modules had no README (finding K2). The
stray root files (`30%`, `5`, `Socket`) were already gone.

### Added / Changed
- **`.gitignore`** — ignore `.idea/`, `/data/`, `minIO/data/`,
  `minIO/tracks_full.csv`, and `*.parquet` (generated output must never be
  committed).
- **Untracked** (`git rm --cached`, kept locally): the 10 `.idea/` files, the
  generated `data/tracks_csv_output/` CSVs, the two Gold `*.parquet` dumps under
  `minIO/data/`, and `minIO/tracks_full.csv`.
- **`LICENSE`** (new) — MIT (conventional default for a portfolio repo; the
  owner can swap the license/copyright holder).
- **Module READMEs** (new) — `common/`, `spark_jobs/`, `dags/`, `ingestion/`,
  `mlops/`, `tests/`, `azure_iac/`, `cassandra/`, `minIO/`. Every top-level
  module now has one (`flink_jobs/` and `kubernetes/` already did). The `minIO/`
  README labels those scripts as legacy/pre-Iceberg.

### Acceptance criteria
- [x] No junk / IDE / generated-data files tracked (`git ls-files` clean).
- [x] LICENSE present.
- [x] Each top-level module has a README (11/11).

### Rollback
Untracking is reversible (`git add` back), but the ignore rules would need
removing too; purely a tracking/docs change with no runtime impact.

## PR-19 — DR & scalability: recovery docs + object versioning/retention

**Type:** docs + infra (dr) · **Branch:** `pr-019-dr-and-scaling` (off `main`)

### Context
There was no disaster-recovery story — no RPO/RTO, backup/replication, or scaling
analysis (finding F3) — and the Terraform used single-region **LRS** with a
single fixed node pool and no object versioning (finding F4). A clobbered or
bad-batch write had no documented path back.

### Added / Changed
- **`docs/DR_AND_SCALING.md`** (new) — concrete **RPO/RTO per store**, the
  "derive don't back up everything" principle (only Bronze + Iceberg lakehouse
  are systems of record; serving copies are rebuildable), backup/replication
  strategy, **recovery procedures** (Iceberg `rollback_to_snapshot` drill,
  object-version restore, serving-layer rebuild, region failover), and a
  **scaling levers** table (Kafka partitions↔consumers, Spark AQE/shuffle from
  PR-18, Iceberg file sizing, Flink state, Cassandra/ES, AKS autoscaling).
- **`azure_iac/main.tf`** — storage account now takes a configurable
  **replicated** tier (default ZRS, not LRS) and enables **blob versioning** +
  delete/container retention (point-in-time recovery); AKS node pool gains
  **cluster-autoscaling** (min/max); resources tagged; outputs added.
- **`azure_iac/variables.tf`** (new) — `storage_replication_type` (validated
  set, default ZRS), node pool sizing/autoscaling vars, retention window, tags.
- **`azure_iac/versions.tf`** (new) — pins `terraform >= 1.3` and
  `azurerm ~> 4.0` so `init` can't pull a breaking provider major.
- **`kubernetes/base/minio-versioning-job.yaml`** (new, wired into the base
  kustomization) — a `mc` Job enabling **bucket versioning** + best-effort
  noncurrent-version retention on `spotify-bronze/silver/gold/lakehouse`
  (idempotent; `spotify-checkpoints` intentionally excluded — it churns).

### Design decisions
1. **Two-sided versioning.** Azure blob versioning (Terraform) for cloud and a
   MinIO Job for on-cluster/dev, so "object versioning enabled" holds in both
   deployment modes; the same 30-day retention bounds cost on each.
2. **Replication as a validated variable, default ZRS.** Offers LRS→RA-GZRS; the
   default is replicated (survives an AZ loss) instead of the old single-AZ LRS.
3. **Recover by derivation.** RPO is defined by rebuild speed for serving copies
   and by immutable snapshots for the lakehouse — not by backing up everything.
4. **Provider pin added** so the 3.x→4.x attribute renames can't silently break
   the plan.

### Acceptance criteria
- [x] DR doc with concrete RPO/RTO and recovery procedure.
- [x] Object versioning enabled (Azure blob + MinIO Job); Terraform offers a
  replicated storage option (validated variable, default ZRS).

### Validation
`kubectl kustomize overlays/{dev,prod}` renders cleanly (25 objects each,
including the versioning Job). `terraform validate`/`plan` needs the Terraform
CLI (not available in this environment); the HCL is hand-verified against the
pinned `azurerm ~> 4.0` schema (balanced blocks, correct 4.x attribute names,
variable validation).

### Rollback
Docs are non-executable; the Terraform change is reviewed via `plan` before
`apply`; the MinIO Job is idempotent and additive (revert removes it).

## PR-18 — Cost & performance: native categorisation, AQE, file sizing

**Type:** perf (batch) · **Branch:** `pr-018-cost-performance` (off `main`)

### Context
The Bronze→Silver job categorised track duration with a **Python UDF**
(`categorize_duration` / `udf(...)`), which forces a per-row Python↔JVM
round-trip in the hot path (finding C4). `common.spark.build_spark` set no
Adaptive Query Execution, and the Silver Iceberg table declared no target file
size, so `MERGE`/streaming could accumulate tiny files (finding J1/J2).
*(The other J-class items were already resolved earlier: reads are delta-scoped
by `ingest_date`, and the pre-write global `orderBy` was dropped in PR-08.)*

### Added / Changed
- **`spark_jobs/batch/bronze_to_silver_all.py`**
  - Replaced the `categorize_duration` **Python UDF** with a native
    `duration_category_expr()` (`F.when/otherwise`, JVM codegen — no per-row
    Python). Kept a pure `categorize_duration()` as the single source of truth
    for the thresholds (`DURATION_SHORT_MS=180_000`, `DURATION_MEDIUM_MS=300_000`),
    unit-tested with no JVM. Dropped the now-unused `udf` / `StringType` imports.
  - Empty date-partitions now short-circuit with `df.isEmpty()` instead of a
    `count()==0` guard; the full `count()` is taken only as a real metric on the
    non-empty path (faster empty-backfill dates).
  - Silver Iceberg table created with
    `TBLPROPERTIES ('write.target-file-size-bytes'='134217728')` (~128 MiB) to
    target compaction-friendly file sizes on write.
- **`common/spark.py`** — `build_spark` now enables Adaptive Query Execution
  (`spark.sql.adaptive.enabled`, `…coalescePartitions.enabled`), both
  env-overridable (`SPARK_AQE_ENABLED` / `SPARK_AQE_COALESCE`), so small
  post-shuffle partitions coalesce at runtime.
- **`tests/test_transforms.py`** (new) — a JVM-free value-matrix test locking the
  categorisation boundaries (None/0 → `Unknown`, the 3-min / 5-min edges), plus a
  Spark-backed test asserting the native expression equals the pure function on
  the same matrix (skips where no JVM is available, per the repo's Spark-test
  policy).

### Design decisions
1. **Pure reference + native mirror.** The boundary semantics stay in a pure
   Python function (testable in CI with no Spark); the hot path uses the native
   expression built from the *same* constants, so correctness is CI-verifiable
   and the expensive UDF round-trip is gone.
2. **`isEmpty()` for the guard, `count()` for the metric.** Only the emptiness
   decision is short-circuited; the row-count observability is preserved.
3. **File sizing on write + AQE coalesce**, complementing (not replacing) the
   PR-08 compaction job — smaller blast radius than rewriting the write path.

### Acceptance criteria
- [x] A run processes only the new date's data (reads are `ingest_date`-scoped).
- [x] No pre-write `orderBy`; no `count()`-as-guard; no Python UDF in the hot path.
- [x] Output file sizes targeted (`write.target-file-size-bytes` + AQE coalesce).

### Rollback
Pure code revert; outputs are equivalent data (the native expression is
behaviour-identical to the old UDF), so rollback is behaviour-preserving.

## PR-17 — Deployment consolidation: Kustomize base/overlays + pinned Helm values

**Type:** chore (k8s) · **Branch:** `pr-017-deployment-consolidation` (off `main`)

### Context
`kubernetes/` had five overlapping Airflow values files (`airflow-values.yaml`
in two directories, `airflow.values.yaml`, `.utf8`, `.current`), a 2332-line
`airflow.manifest.yaml` (a rendered-manifest dump), and `fix_operator.yaml`
(a live `kubectl get deployment -o yaml` of the Spark Operator, complete with
`resourceVersion`/`uid`/`status`). Images `kafka-ui` and `minio` floated on
`:latest`, and there was no dev/prod separation (finding I2).

### Added / Changed
- **`kubernetes/base/`** (new) — one Kustomize base bundling the raw infra
  manifests (`kafka`, `kafka-ui`, `minio`, `mongodb`, `elk`, `airflow-postgres`,
  `rbac-airflow-bigdata`) plus `namespaces.yaml`, which now also declares the
  **`airflow`** namespace that `airflow-postgres.yaml` targeted but nothing
  created.
- **`kubernetes/overlays/{dev,prod}`** (new) — `dev` is the single-node base;
  `prod` layers patches (two `kafka-ui` replicas, a larger Elasticsearch heap +
  memory). Both render offline via `kubectl kustomize`.
- **`kubernetes/helm/airflow/values.yaml`** — the single canonical Airflow Helm
  values (moved from the well-documented `kubernetes/airflow-values.yaml`).
- **`kubernetes/helm/spark-operator/values.yaml`** (new) — replaces the
  `fix_operator.yaml` live dump with declarative, chart-pinned values
  (namespace `bigdata`, image `ghcr.io/kubeflow/spark-operator/controller:2.4.0`,
  metrics on).
- **Pinned images** — `provectuslabs/kafka-ui:v0.7.2`,
  `minio/minio:RELEASE.2024-01-16T16-07-38Z` (off `:latest`).
- **`kubernetes/README.md`** (new) — the two-layer deploy runbook (Kustomize for
  infra, Helm for Airflow + Spark Operator).

### Removed
`airflow/airflow-values.yaml`, `airflow/airflow.values.yaml`,
`airflow/airflow.values.utf8.yaml`, `airflow/airflow.values.current.yaml`,
`airflow/airflow.manifest.yaml`, `fix_operator.yaml`, and the old root
`namespace.yaml` (folded into `base/namespaces.yaml`).

### Design decisions
1. **Kustomize for raw infra, Helm for platforms.** The infra manifests are
   hand-authored YAML that Kustomize layers cleanly (and validates fully offline
   with `kubectl kustomize`); Airflow and the Spark Operator are upstream charts,
   so they stay as pinned Helm values rather than being force-fit into Kustomize.
2. **No global namespace transformer.** Resources deliberately span `bigdata`
   and `airflow`, so each manifest keeps its own `namespace:` and the base does
   not override it.
3. **Stateful singletons stay single-replica.** `prod` scales only stateless
   components; MinIO/Elasticsearch-data/Postgres need a real HA storage story
   (deferred to PR-19) before scaling, so the overlay does not fake it.
4. **`fix_operator.yaml` distilled, not kept.** A live-object dump is not
   reproducible; only its meaningful settings were re-expressed as chart values.

### Acceptance criteria
- [x] One values source per environment; no `.utf8`/`.current`/duplicate files.
- [x] All charts/images version-pinned.
- [x] `kubectl kustomize overlays/{dev,prod}` renders cleanly (24 objects each).

### Rollback
Revert restores the prior manifests (kept in git history); infra is declarative,
so re-applying the old files is deterministic. Coordinate with any live cluster
before applying.

## PR-16 — MLOps loop: feature table -> training -> MLflow registry

**Type:** feat (mlops) · **Branch:** `pr-016-mlops-loop` (off `main`)

### Context
`mlops/train_anomaly_model.py` trained on `s3a://datalake/gold/user_behaviors.parquet`
— a path **no job produced** — so the MLOps story was disconnected (findings
D3, C6). There was no feature pipeline, no registry staging, and no link to
pipeline data.

### Added / Changed
- **`spark_jobs/batch/build_features.py`** (new) — aggregates the star-schema
  fact `gold_star.fact_playback` (PR-09) into a per-user feature table
  (`play_count`, `play_duration`, `total_play_duration_ms`, `skip_count`,
  `skip_rate`, `distinct_tracks`, `as_of_date`). Publishes both an **Iceberg**
  table (`lakehouse.gold.user_features`, `MERGE`-upsert on `user_id` →
  idempotent) and a flat **Parquet** export at `FEATURES_PATH` for the trainer.
  Structured JSON logging (PR-13); fails loudly.
- **`mlops/train_anomaly_model.py`** (rewrite) — reads the real feature table;
  pure `train_isolation_forest()` core (validates the `FEATURE_COLUMNS`
  contract, deterministic `random_state`, deferred `sklearn` import) +
  `run_training()` MLflow wrapper (deferred `mlflow` import) that logs
  params/metrics, `log_model`s with `registered_model_name`, and transitions the
  new version to `Staging`.
- **`tests/test_mlops.py`** (new, 6 tests) — JVM-free pandas + sklearn: feature
  contract holds, a planted outlier is flagged `-1`, missing-column / empty
  table raise, training is reproducible.
- **`docs/MLOPS.md`** (new) — lifecycle diagram, feature schema, config, run +
  validation.
- **`requirements.txt`** (+`scikit-learn`, `mlflow`) and **`requirements-dev.txt`**
  (+`pandas`, `scikit-learn`) so CI runs the MLOps tests.

### Design decisions
1. **Pure training core + deferred ML deps.** `train_isolation_forest` is pure
   pandas/sklearn (no Spark, no MLflow), so the loop's decision logic is fully
   unit-tested in CI without a cluster — mirroring the `common.spark` /
   `common.quality` split. `mlflow` is imported only inside `run_training`.
2. **Feature table = Iceberg source of truth + Parquet handoff.** The Iceberg
   table is the lakehouse record (idempotent MERGE on `user_id`); the flat
   Parquet export lets the lightweight sklearn container consume features with
   pandas, no Iceberg catalog required. One env var (`FEATURES_PATH`) binds both
   sides.
3. **Fail loudly on a broken feature contract.** A missing feature column or an
   empty table raises `ValueError` in the pure core, so a bad upstream table
   never yields a silently-degenerate model.
4. **Reuse over reinvention.** Session via `common.spark.build_spark`, table
   naming via `common.modeling.star_table` / `common.spark.gold_table`, MERGE via
   `build_merge_sql`, logging via `common.logging` (PR-03/07/09/13).

### Acceptance criteria
- [x] `train_anomaly_model.py` runs end-to-end against pipeline-produced data
  (feature table exists; no file-not-found) — the disconnected path is removed
  and both jobs share `FEATURES_PATH`.
- [x] A model version is registered in MLflow with logged metrics — `run_training`
  `log_model(registered_model_name=…)` + `log_metrics` + stage transition.
- [x] Feature aggregation unit-tested; training core unit-tested (6/6).

### Rollback
Additive: the feature table + registry entries are new; revert removes
`build_features.py` and restores the prior trainer. No serving dependency.

---

## PR-15 — Data-quality gates between layers

**Type:** feat (quality) · **Branch:** `pr-015-data-quality` (off `main`)

### Context
There were no row-count / null / uniqueness / referential / freshness checks, so
bad data flowed silently to Gold / serving (finding H2).

### Added / Changed
- **`spark_jobs/quality/checks.py`** (new) — reusable gates:
  - Pure evaluators (`non_null_keys`, `unique_keys`, `fk_resolved`,
    `value_in_range`, `freshness`) + `CheckResult` + `DataQualityReport`
    (`log()` emits `dq_check` metrics, `raise_if_failed()` raises
    `DataQualityError` on any BLOCK breach). All JVM-free / unit-testable.
  - Spark-executing helpers (`key_checks`, `range_check`, `fk_check`,
    `freshness_check`) with **deferred** `pyspark` import; `fk_check` reuses
    `common.modeling.unresolved_fk_count_sql` (PR-09).
  - Severity is configurable via `DATA_QUALITY_SEVERITY` = `block` (default) /
    `warn`.
- **`spark_jobs/batch/bronze_to_silver_all.py`** — runs the non-null + uniqueness
  gate on each dataset's business key **before** the Silver write; a breach is
  recorded by the `FailureCollector` (PR-13) so the job exits non-zero and bad
  rows never land in Silver.
- **`tests/test_quality.py`** (new, 10 tests) — seeded null-key / duplicate /
  broken-FK / out-of-range / stale fixtures fail the gate; clean data passes;
  results log as JSON `dq_check` metrics; `warn` never blocks.
- **`docs/DATA_QUALITY.md`** (new).

### Design decisions
1. **Pure evaluators + lazy-Spark executors.** The decision logic is unit-tested
   without a JVM (matching `common.spark`/`common.modeling`); only count
   computation touches Spark. This is why the gate is fully covered in CI (no
   cluster) yet real in production.
2. **Gate before write, fail via FailureCollector.** Running pre-write means bad
   data is never persisted; recording through PR-13's collector means one bad
   dataset fails the whole job loudly (non-zero) rather than silently.
3. **Configurable severity.** `warn` mode lets gates be rolled out on historical
   data before flipping to `block`.

### Verification
`python tests/test_quality.py` → **10/10 PASS**; full suite **67 passed, 3
skipped** under pytest; `ruff` clean. End-to-end DAG-fails-on-corrupted-layer is
the integration test (needs Spark; documented in `docs/DATA_QUALITY.md`).

---

## PR-14 — CI pipeline: lint · secret scan · conflict guard · pytest · Flink build

**Type:** ci · **Branch:** `pr-014-ci-pipeline` (off `main`)

### Context
There was no CI, and a merge conflict had previously reached `main` (I1, I3). CI
is the systemic guarantee that class of defect — plus secrets, lint regressions,
and Flink API breaks — never recurs.

### Added / Changed
- **`.github/workflows/ci.yml`** (new) — five jobs on push-to-main and every PR:
  `conflict-markers` (folds in and **supersedes** `conflict-guard.yml`), `lint`
  (`ruff`), `secret-scan` (`gitleaks --no-git`), `test` (`pytest`), `flink-build`
  (`mvn -f flink_jobs/pom.xml package`).
- **`.github/workflows/conflict-guard.yml`** — **removed** (superseded by the
  `conflict-markers` job).
- **`pyproject.toml`** (new) — Ruff config (default correctness ruleset `E4/E7/E9`
  + `F`, with legacy/exploratory scripts excluded pending hygiene PRs) and pytest
  config (`testpaths=tests`).
- **`requirements-dev.txt`** (new) — pinned `ruff`, `pytest`, `PyYAML`, `pyspark`.
- **`.gitleaks.toml`** (new) — default rules + an allowlist for the known,
  already-rotated `miniopass123` placeholder; documents the `--no-git` rationale.
- **`docs/ci.md`** (new) — the gates and the owner steps to enable branch
  protection.
- **`tests/test_dags.py`**, **`tests/test_batch_pipeline.py`** — switched their
  custom skip exception to `unittest.SkipTest` so the optional-dependency tests
  **skip** under pytest (previously they errored, which would have made the CI
  `test` job red).

### Design decisions
1. **Correctness ruleset, legacy excluded.** Lint enforces the rules the
   maintained code already satisfies (undefined names, unused imports, syntax) and
   excludes pre-existing non-compliant legacy scripts, so the gate is green on
   `main` today and real for new code. Line-length (E501) is intentionally not
   enforced to avoid churning unrelated pre-existing lines.
2. **`gitleaks --no-git` + allowlist.** Scanning the working tree (not history)
   keeps the gate meaningful for new changes without failing on the rotated
   secrets still living in old commits, which cannot be scrubbed without a rewrite.
3. **`pyspark` in dev deps, not a JVM.** The pure transform tests only *import*
   pyspark (SQL-string builders), so `pytest` needs no Spark cluster; other tests
   skip when Airflow/Kafka/MinIO/Spotify libs are absent.
4. **One Flink build job** catches the API-break class (A3) that PR-05/PR-10
   addressed at the source.

### Verification (locally, mirroring the CI jobs)
- `ruff check .` → **all checks passed**.
- `pytest` → **57 passed, 3 skipped** (optional-dep tests skip cleanly).
- `mvn -f flink_jobs/pom.xml clean package` → **BUILD SUCCESS**.
- conflict-marker scan → none. Tree scanned for PEM keys / cloud tokens / literal
  passwords → only the allowlisted `miniopass123`.
- The live pass/fail of the GitHub Actions run and enabling branch protection are
  owner-side (documented in `docs/ci.md`); `gh` is unauthenticated here.

---

## PR-13 — Fail-fast error handling + structured JSON logging + stage metrics

**Type:** feat (observability) · **Branch:** `pr-013-structured-logging` (stacked on `pr-012-batch-orchestration`)

### Context
The batch jobs wrapped each dataset in `except Exception: print(); return`, so a
partial failure was swallowed and the job still exited **0** (findings C5, H1).
`print`-logging is also unparseable and there were no metrics.

### Added / Changed
- **`common/logging.py`** (new) — pure-stdlib (no JVM):
  - `get_logger()` emits single-line **JSON** to stdout, each record carrying a
    process `run_id` (pinnable via `PIPELINE_RUN_ID`); idempotent per name.
  - `log_metrics()` / `stage_timer` emit per-stage row counts + duration.
  - `FailureCollector` isolates per-dataset failures (attempt them all) but
    `raise_if_any()` re-raises a `PipelineError` at the end → **non-zero exit**.
- **`spark_jobs/batch/bronze_to_silver_all.py`**, **`silver_to_gold_all.py`**,
  **`gold_to_es.py`** — the three orchestrated (PR-12 DAG) batch jobs: `print`
  replaced with the JSON logger; each dataset wrapped in a `stage_timer`
  (rows_in/rows_out/duration) and driven from a `main()` loop that uses
  `FailureCollector` — a failing dataset now fails the whole job loudly.
- **`tests/test_logging.py`** (new) — JSON+run-id, metrics fields, `stage_timer`
  start/end/error (no swallow), idempotent logger, and FailureCollector
  isolate-then-raise-non-zero.

### Design decisions
1. **Isolate then fail loudly.** Per-dataset isolation is still useful (one bad
   dataset shouldn't skip the rest), but the job must exit non-zero if any failed
   — `FailureCollector` gives both, replacing the silent `print(); return`.
2. **JSON to stdout with a run id.** Operable/greppable logs that correlate
   across a run (and across Airflow tasks when `PIPELINE_RUN_ID` is set).
3. **Scope: the orchestrated batch path first.** The three jobs on the PR-12 DAG
   (which carried the swallow-bug) are converted now; the remaining exploratory
   `minIO/*`, `get_data_*`, `mlops/*`, and stream scripts adopt the same helper
   incrementally (additive-first, keeps the diff reviewable).

### Verification
`python tests/test_logging.py` → **7/7 PASS** (JSON parse, run id, metrics,
fail-loud non-zero). Full suite green; `ruff` clean. The Spark jobs import
`pyspark` at module top so they run on a cluster (AST-validated here).

---

## PR-12 — Airflow orchestrates Spark on K8s + idempotent, scheduled maintenance

**Type:** feat (airflow) · **Branch:** `pr-012-batch-orchestration` (stacked on `pr-011-airflow-hygiene`; merge PR-11 first)

### Context
Heavy work (crawl + upload) ran inside the Airflow worker, and the Spark batch
ETL had no schedule — it was orphaned from orchestration entirely (findings E3,
F2). There was no idempotent, backfillable batch DAG.

### Added / Changed
- **`dags/spotify_batch_pipeline.py`** (new) — a separate `dag_id` that submits
  every heavy step to Kubernetes and waits on it: `crawl` (pod) → `land_bronze`
  (pod) → `bronze_to_silver` → `silver_to_gold` → `gold_to_es` → `maintenance`,
  each Spark step a `SparkKubernetesOperator` + `SparkKubernetesSensor` pair so
  the driver's success/failure becomes the task's state. `catchup=True` +
  `max_active_runs=1` make it backfillable one date at a time.
- **`spark_jobs/batch/yaml/run_{bronze_to_silver,silver_to_gold,gold_to_es}.yaml`** —
  reconciled with the DAG: per-run name `…-{{ ds_nodash }}` (backfill-safe), the
  logical date `DT={{ ds }}` in driver+executor, and MinIO credentials moved from
  a **hardcoded `miniopass123`** to a `minio-credentials` `secretKeyRef` (closes a
  secret PR-02 had missed in these specs).
- **`spark_jobs/batch/yaml/run_maintenance.yaml`** (new) — submits `maintenance.py`
  (PR-08) with the Iceberg runtime as the final batch step.
- **`docs/orchestration.md`** (new) — flow, backfill semantics, cluster
  prerequisites (incl. creating the `minio-credentials` secret), validation.
- **`tests/test_batch_pipeline.py`** (new) — renders every SparkApplication spec
  and asserts per-run name, `DT`, and `secretKeyRef` (no plaintext secret); a
  `DagBag` structure test that skips without Airflow (runs in CI).

### Design decisions
1. **Submit + sensor per Spark step.** The roadmap asks for K8s submission "with
   sensors"; the sensor blocks on the driver so Airflow reflects real job state
   instead of fire-and-forget. The sensor reads the submitted application's name
   from the operator's XCom, so it tracks the exact per-run `SparkApplication`.
2. **Per-run `SparkApplication` names + serialised backfill.** `{{ ds_nodash }}`
   in `metadata.name` avoids collisions across dates; `max_active_runs=1` avoids
   concurrent writers on the same Iceberg tables. Idempotent steps make the
   backfill correct and duplicate-free.
3. **Credentials via Secret, not YAML.** Reconciling the specs was the right
   moment to remove the hardcoded MinIO password and pull from a Secret.
4. **Separate `dag_id`, legacy DAG untouched.** Rollback is deleting one file;
   no shared state (additive-first, main stays green).

### Verification
`python tests/test_batch_pipeline.py` → 4 spec tests PASS (DAG-import skips
locally, runs in CI); full existing suite still green. `py_compile` + `ruff` clean
on the new DAG. Live K8s submission / two-date backfill require a minikube cluster
(documented in `docs/orchestration.md`, not runnable here).

---

## PR-11 — Airflow hygiene: clients-in-tasks, drop `depends_on_past`, logical-date

**Type:** fix (airflow) · **Branch:** `pr-011-airflow-hygiene` (off `main`)

### Context
`dags/spotify_pipeline.py` imported a live `KafkaProducer`, and both task
modules built their Spotify/Kafka/MinIO clients — and even **ran a full crawl /
consume loop** — at module scope. Airflow re-parses the DAGs folder every few
seconds, so every parse opened real network connections (findings E1, E2, E4).
`depends_on_past=True` combined with `retries` could deadlock the schedule; the
schedule comment ("daily") contradicted its `*/7 * * * *` cron; `owner` was the
placeholder `your_name`; and the ingest date was not templated.

### Changed
- **`dags/tasks/crawl_spotify.py`** — removed the module-level Spotify client,
  `KafkaProducer`, topic-creation calls, and the entire run-at-import block. Added
  `build_spotify()` / `build_producer()` factories; `crawl_new_releases(sp=None)`
  builds lazily; new `run_crawl(logical_date, sp=None, producer=None)` task
  entrypoint that owns and always closes the producer it creates (`finally`).
- **`dags/tasks/kafka_to_minio.py`** — removed the module-level MinIO client,
  bucket check, and consume loop. Added `build_minio_client()` / `ensure_bucket()`;
  `consume_and_upload(topic, folder, partition_date=None, client=None)` now
  partitions by the supplied logical date (dropping a stray `+2 days` bug in the
  fallback); new `run_consume(logical_date, client=None)` entrypoint. Removed an
  unused `TopicPartition` import.
- **`dags/spotify_pipeline.py`** — import the entrypoints (no live clients);
  `depends_on_past=False`; real `owner='bigdata-team'`; `tags`; `max_active_runs=1`;
  clarified `schedule_interval` comment; date templated via `op_kwargs={'logical_date': '{{ ds }}'}`.
- **`tests/test_dags.py`** (new) — subprocess socket-guard proving the modules
  open no connection at import; `run_crawl` unit tests (publishes to all topics,
  owns/closes its producer, closes even on error); an Airflow `DagBag` import test
  that skips when Airflow is absent (runs in CI).

### Design decisions
1. **Factories + task entrypoints, clients never at module scope.** Import-time
   is side-effect-free, so the scheduler can parse the DAG cheaply and safely; the
   task callable owns each client's lifecycle and closes it in `finally`.
2. **Scope limited to the DAG-imported `dags/tasks/*` copies.** The standalone
   `ingestion/*` scripts (run as K8s pods with `RUN_ONCE`) are a separate copy and
   intentionally left untouched here.
3. **`{{ ds }}` threaded as `logical_date`.** Backfills/reruns partition bronze
   deterministically instead of by wall-clock or a literal.
4. **`depends_on_past` dropped, `max_active_runs=1` added.** Removes the deadlock
   risk while still preventing overlapping crawls.

### Verification
`python tests/test_dags.py` → crawl no-socket-at-import PASS (spotipy+kafka present
locally), `run_crawl` unit tests PASS; MinIO/Airflow-dependent tests skip locally
and run in CI. `ruff check dags/ tests/test_dags.py` → all checks passed. Full
existing suite still green.

---

## PR-10 — Stateful Flink windowed anomaly detection + Kafka sink

**Type:** feat (flink) · **Branch:** `pr-010-stateful-flink` (off `main`)

### Context
The docs promise stateful windows ("skip > N in 1 min") but the Flink job was a
stateless string `filter` + `print()` sink that matched fields (`action`,
`duration`) which do not even exist in the real event schema (findings A5, D1).

### Changed
- **`AnomalyDetectionJob.java`** — real event-time pipeline: parse JSON →
  `PlaybackEvent` (drop malformed), assign bounded-out-of-orderness watermarks
  from the producer `timestamp`, `keyBy(user_id)`, tumbling event-time window,
  and a `ProcessWindowFunction` counting `skipped` plays. A user over
  `ANOMALY_SKIP_THRESHOLD` skips in the window emits an `Anomaly` to a Kafka
  `KafkaSink` on `TOPIC_ANOMALY`. Checkpointing enabled (durable keyed state).
- **`model/PlaybackEvent.java`**, **`model/Anomaly.java`** (new) — Flink-friendly
  POJOs; Jackson-annotated (snake_case ↔ camelCase), `ignoreUnknown`.
- **`pom.xml`** — add `jackson-databind` (bundled into the uber-jar).
- **`flink_jobs/README.md`** — pipeline diagram + new env vars.

### Design decisions
1. **Event-time windows, not processing-time.** Watermarks from the event
   `timestamp` make the "N skips per minute" detection correct under lateness and
   replay, and reproducible from checkpoints.
2. **`ProcessWindowFunction` counting skips.** Clear and correct at this scale;
   the window's keyed state is what makes the job genuinely stateful (the
   acceptance criterion). Checkpointing makes in-flight windows recoverable.
3. **Threshold/window/topic via env.** Same env-var convention as the Python
   `common.config` (Java can't import it), so `TOPIC_PLAYBACK`/`TOPIC_ANOMALY`
   stay unified platform-wide.
4. **Resilient (de)serialization.** Malformed events and un-serializable
   anomalies are dropped in `flatMap` rather than failing the job.

### Verification
- `mvn -B clean package` → **BUILD SUCCESS**; the shaded uber-jar contains the
  new `com.spotify.anomaly.*` classes + bundled Jackson (verified via `jar tf`).
- Live stateful behaviour (a skip burst → exactly one anomaly, restart-from-
  checkpoint) needs a running Kafka + Flink cluster (documented in the README,
  not runnable here).

### New environment variables
| Variable | Default | Notes |
| :--- | :--- | :--- |
| `TOPIC_ANOMALY` | `spotify_anomaly_events` | Anomaly sink topic (matches `common.config`). |
| `ANOMALY_SKIP_THRESHOLD` | `5` | Skips per user per window that trigger an anomaly. |
| `ANOMALY_WINDOW_SECONDS` | `60` | Tumbling event-time window length. |
| `FLINK_CHECKPOINTS` | *(cluster default)* | Optional checkpoint storage dir. |

### Rollback
`git revert` restores the prior compile-only job. The new job writes to a *new*
topic (`TOPIC_ANOMALY`), so no existing consumer is disrupted; checkpoints are
namespaced per job, so a rollback redeploys the old jar cleanly.

## PR-09 — Dimensional model: star schema + SCD2 dimensions

**Type:** feat (modeling) · **Branch:** `pr-009-star-schema` (stacks on PR-08)

### Context
Silver mirrors source JSON (`artist_ids` as a delimited string) and the legacy
Gold `*_stats` tables flatten-then-reaggregate with no conformed dimensions,
surrogate keys, or history (finding B2). PR-09 adds a proper star schema in a new
`gold_star` Iceberg namespace, **additive** to the untouched legacy `*_stats`.

### Changed
- **`common/modeling.py`** (new) — pure, JVM-free helpers: `star_table()`,
  `attribute_hash_expr()` (null-safe SCD2 change detection),
  `surrogate_key_expr()` (deterministic per-version key), the three SCD2
  statements (`scd2_close_sql` / `scd2_insert_sql`), and
  `unresolved_fk_count_sql()` (referential-integrity check).
- **`spark_jobs/batch/build_dimensions.py`** (new) — SCD2 `dim_artist` /
  `dim_album` / `dim_track` from Silver via the idempotent three-step (stage →
  close changed → insert new/changed). `dim_track` splits `artist_ids` into
  `array<string>` so the model has no delimited-string ids.
- **`spark_jobs/batch/build_facts.py`** (new) — `fact_playback` at event grain
  (`event_id`) from Cassandra `spotify_ks.user_plays`, left-joined to the current
  dimension versions for surrogate keys, `MERGE`-upserted into
  `gold_star.fact_playback` (partitioned by `days(event_time)`).
- **`common/spark.py`** — added an `extra_configs` hook to `build_spark` (for the
  `spark.cassandra.connection.*` host/port). Backward-compatible.
- **`tests/test_modeling.py`** (new, 11) — naming, hash/key exprs, SCD2 SQL
  (close/insert/composite key), FK-check + guards.
- **Docs** — `docs/DATA_MODEL.md` (ERD, grain, surrogate keys, SCD2 flow,
  verification).

### Design decisions
1. **Additive star model in `gold_star`, Silver untouched.** The roadmap's
   Silver `artist_ids` string→array change would break shared Silver consumers
   (`minIO/silver_to_gold.py`, PR-08 Gold) and violate "main stays green", so the
   array split lives in `dim_track` — the AC "no delimited-string id columns
   remain **in the model**" holds without a Silver schema change. Silver cutover
   deferred.
2. **Deterministic hash surrogate keys.** `sha2(business_key || valid_from)` is
   reproducible (idempotent load) and unique per version — unlike
   `monotonically_increasing_id`, which would churn keys every run.
3. **SCD2 via a 3-step MERGE/anti-join, not one statement.** A single MERGE
   cannot both close the old version and insert the new one for the same key;
   close-then-anti-join-insert is the standard, idempotent pattern.
4. **Fact from Cassandra (`FACT_SOURCE`).** Raw events are only retained in the
   serving store (PR-04); reading it is pragmatic and the source is configurable
   so a future lake-landed `events` dataset is a config swap.
5. **Pure SQL builders.** All SCD2/key/hash logic is side-effect-free and
   unit-tested with no JVM (mirrors `build_merge_sql`).

### Verification
- `python -m py_compile` on all new/edited files; `ruff (E,F)` clean.
- `python tests/test_modeling.py` (11) + existing suites pass with no Spark/JVM.
- Live SCD2 history / fact FK-resolution need a Spark 3.5 + MinIO + Cassandra
  environment (documented in `docs/DATA_MODEL.md`, not runnable here).

### New environment variables
| Variable | Default | Notes |
| :--- | :--- | :--- |
| `GOLD_STAR_NAMESPACE` | `gold_star` | Iceberg namespace for the star model. |
| `FACT_SOURCE` | `cassandra` | Playback-event source for `fact_playback`. |
| `CASSANDRA_SPARK_CONNECTOR` | `com.datastax.spark:spark-cassandra-connector_2.12:3.5.0` | Batch Cassandra read. |

### Rollback
New namespace/tables are additive; `git revert` removes the builders and helpers,
legacy `*_stats` and Silver are untouched. Drop `gold_star.*` to reclaim storage.

## PR-08 — Iceberg-backed Gold + maintenance (compaction, snapshot expiry)

**Type:** feat (lakehouse) · **Branch:** `pr-008-iceberg-gold` (stacks on PR-07)

### Context
PR-07 made Silver Iceberg but Gold was still `overwrite` Parquet, so the pipeline
was not end-to-end ACID; `MERGE`/streaming writes accumulate small files with no
compaction; and `gold_to_es.py` synced artists only, using a deprecated `/doc`
mapping type (findings C1/C2/J2).

### Changed
- **`common/spark.py`** — added `gold_table()` (mirrors `silver_table()`, new
  `GOLD_NAMESPACE`) and an `extra_packages` hook on `build_spark` so a job can
  declare extra JVM coordinates (e.g. the ES connector) alongside the Iceberg
  runtime without clobbering `spark.jars.packages`. Backward-compatible.
- **`spark_jobs/batch/silver_to_gold_all.py`** — feature-flagged
  (`GOLD_FORMAT=iceberg|parquet`, **default `parquet`**). The Iceberg path reads
  Silver from the catalog and writes Gold Iceberg tables via
  `writeTo(...).createOrReplace()` (idempotent full-recompute → one new
  snapshot). Dropped the pre-write global `orderBy` (query-time concern; the
  album top-track windowing is retained). Converted the star `functions` import
  to explicit `F.` references.
- **`spark_jobs/batch/gold_to_es.py`** — reads Gold the same dual-path way and
  **syncs both** `batch_artists` and `batch_albums` (completes the artists-only
  sync). Typeless indices (drops the deprecated `/doc` type, consistent with
  PR-04). Declares the ES connector via `ES_SPARK_RUNTIME`.
- **`spark_jobs/batch/maintenance.py`** (new) — `rewrite_data_files`,
  `expire_snapshots` (retain `SNAPSHOT_RETAIN_LAST`, default 5), and
  `remove_orphan_files` over the Silver + Gold tables (targets overridable via
  `MAINTENANCE_TABLES`). Pure, JVM-free SQL builders.
- **`tests/test_maintenance.py`** (new, 7) — maintenance SQL builders + retain
  guard + target-set coverage/override. **`tests/test_spark.py`** — `gold_table`
  coverage (+3).
- **Docs** — `docs/lakehouse.md` gains Gold, maintenance, and the `GOLD_FORMAT`
  flag/cutover.

### Design decisions
1. **`GOLD_FORMAT` mirrors `SILVER_FORMAT`, default parquet.** `main` stays
   byte-for-byte behaviour-preserving; the Iceberg Gold tables are additive in a
   separate namespace until an explicit cutover PR.
2. **`createOrReplace` for Gold, not MERGE.** Gold `*_stats` are a full
   aggregate recompute, so a transactional replace is the idempotent primitive
   (stable counts, one new snapshot, history retained). MERGE is for the keyed
   Silver upsert.
3. **Maintenance SQL builders are pure.** The `CALL … system.*` strings are
   generated by side-effect-free functions, so compaction/expiry/orphan logic is
   unit-tested with no JVM (same pattern as `build_merge_sql`).
4. **Typeless ES indices.** ES 7+/8 removed mapping types; `batch_artists/doc`
   would break, so both indices are typeless — consistent with PR-04's stream
   sink.

### Verification
- `python -m py_compile` on every edited/added job; `ruff (E,F)` clean.
- `python tests/test_maintenance.py` (7) + `python tests/test_spark.py` (11) +
  existing config/event-schema suites pass with no Spark/JVM.
- Live Iceberg Gold + compaction/expiry + ES sync need a Spark 3.5 + MinIO + ES
  environment (documented in `docs/lakehouse.md`, not runnable here).

### New environment variables
| Variable | Default | Notes |
| :--- | :--- | :--- |
| `GOLD_FORMAT` | `parquet` | `iceberg` reads Silver Iceberg → writes Gold Iceberg. |
| `GOLD_NAMESPACE` | `gold` | Iceberg namespace for Gold tables. |
| `SNAPSHOT_RETAIN_LAST` | `5` | Snapshots retained by `expire_snapshots`. |
| `MAINTENANCE_TABLES` | *(silver+gold)* | Comma-separated `namespace.table` override. |
| `ES_SPARK_RUNTIME` | `org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4` | ES connector coordinate. |

### Rollback
Config-only: set `GOLD_FORMAT=parquet` to return to the legacy Gold writer/ES
read, or `git revert`. The Iceberg Gold tables are additive (separate
namespace); dropping them does not touch legacy Parquet.

## PR-07 — Iceberg-backed Silver (catalog + MERGE upsert)

**Type:** feat (lakehouse) · **Branch:** `pr-007-iceberg-silver` (off `main`)

### Context
The "Lakehouse" claim was unbacked: live Silver was `write.mode("overwrite")`
Parquet, and the only Iceberg script (`bronze_to_silver_iceberg.py`) was
orphaned and pinned the **Spark 3.3** Iceberg runtime against a `pyspark==3.5`
project — a guaranteed jar mismatch (findings C1/C2/C3).

### Changed
- **`common/spark.py`** (new) — central `build_spark(app_name, iceberg=…)` with
  the MinIO S3A config and the **Iceberg 3.5** runtime + a Hadoop `lakehouse`
  catalog whose warehouse lives on MinIO. Plus pure, JVM-free helpers
  `silver_table()` and `build_merge_sql()`.
- **`spark_jobs/batch/bronze_to_silver_all.py`** — feature-flagged
  (`SILVER_FORMAT=iceberg|parquet`, **default `parquet`** so `main` is
  behaviour-preserving until the cutover). The Iceberg path creates each Silver
  table (empty CTAS to inherit schema + hidden `days(ingest_ts)` partitioning)
  then `MERGE INTO` upserts on the business key → idempotent re-runs.
- **`requirements.txt`** — documents the Iceberg Spark runtime coordinate
  aligned to Spark 3.5 (`iceberg-spark-runtime-3.5_2.12`).
- **Deleted** `spark_jobs/batch/bronze_to_silver_iceberg.py` (orphan, Spark 3.3).
- **`tests/test_spark.py`** (new, 8) — table naming, MERGE-SQL generation, empty-
  key guard, runtime-classifier check, per-dataset business-key coverage.
- **Docs** — `docs/lakehouse.md` (catalog, MERGE, partitioning, cutover, rollback).

## PR-06 — Fail-fast credentials + kill hardcoded paths

**Type:** fix (security) · **Branch:** `pr-006-fail-fast-config` (stacks on PR-03)

### Context
Most Spark/minIO jobs baked in insecure MinIO defaults
(`os.getenv("MINIO_SECRET_KEY", "miniopass123")`, or literal `minioadmin`), so a
missing secret silently became a well-known credential; several scripts also
hardcoded machine-specific `D:\...` absolute paths (findings G2/G3).

### Changed
- **`common/config.py`** — add `require_env(name)` (raises `RuntimeError` on
  unset/empty, no insecure fallback) and `DATA_DIR` (repo-relative default,
  override with `DATA_DIR`).
- **Credentials → `require_env`** in `minIO/minio_client.py` and 8
  `spark_jobs/batch/*.py` (`advanced_analytics`, `bronze_to_silver_all`,
  `bronze_to_silver_iceberg`, `get_data_gold_all`, `get_data_silver_all`,
  `gold_to_es`, `silver_to_gold_all`, `upload_data`). No `minioadmin` /
  `miniopass123` defaults remain.
- **Hardcoded paths → portable** — `minIO/get_data_gold.py`,
  `minIO/upload_minio.py`, `spark_jobs/batch/get_data_gold_all.py` now derive
  local dirs from `DATA_DIR` / `GOLD_EXPORT_DIR`; stray `D:\...` mentions in
  comments/print strings removed.
- **`tests/test_config.py`** — `require_env` (set / unset / empty) and `DATA_DIR`.
- **Docs** — `docs/CONFIGURATION.md` (require_env + DATA_DIR); README env vars.

### Design decisions
1. **Fail fast, no insecure default.** `require_env` turns a missing secret into
   an immediate, clear error instead of a silent `minioadmin` fallback.
2. **`DATA_DIR` over per-file absolute paths.** One repo-relative, env-overridable
   base makes the local scripts portable; the paths AC was otherwise unmet
   because `D:\...` literals remained (e.g. `get_data_gold_all.py`).
3. **Endpoints left as-is.** The strict scope here is *credentials* and *paths*;
   centralising non-secret endpoints is the roadmap's separate light-touch work.
4. **`sys.path` bootstrap per job** (repo root via `__file__`) so `common`
   imports under `spark-submit`, mirroring PR-03.

### Verification
- `git grep` → no `minioadmin` / `miniopass123` and no `D:\` literals remain.
- `python -m py_compile` on every edited job; `ruff (E,F)` clean on authored lines.
- `python tests/test_config.py` passes (require_env raises on unset/empty).

### New environment variables
| Variable | Default | Notes |
| :--- | :--- | :--- |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | *(required)* | Now mandatory — jobs fail fast if unset. |
| `DATA_DIR` | `<repo>/data` | Local data I/O base (replaces `D:\...`). |
| `GOLD_EXPORT_DIR` | `<DATA_DIR>/data_gold` | Local gold-export target. |

### Rollback
Revert restores the prior defaults; no state change. After revert, jobs again
fall back to insecure defaults, so prefer setting the env vars over reverting.

## PR-03 — Central config module + unified Kafka topic taxonomy

**Type:** refactor + fix · **Branch:** `pr-003-central-config` (stacks on PR-01)

### Context
Kafka topic names were defined as literals in each job and had **diverged**: the
producer wrote `spotify_playback_events`, but the Cassandra consumer subscribed
to `spotify-gold-events` (and the Flink job to `spotify-user-events`), leaving 2
of 3 real-time paths dead (architecture review, findings A2/B3).

### Changed
- **`common/config.py`, `common/__init__.py`** (new) — single source of truth
  for the Kafka topic taxonomy, medallion bucket names, service endpoints, and
  a `get_ingest_date()` helper. Every value is env-overridable; the module holds
  no secrets and has no import side effects.
- **`spark_jobs/stream/produce_to_kafka.py`**, **`stream_processing.py`**,
  **`stream_processing_mongo.py`**, **`stream_to_cassandra.py`** — import
  `TOPIC_PLAYBACK` from `common.config` instead of hardcoding the topic. This
  also **reconnects the Cassandra consumer**, which previously subscribed to the
  dead `spotify-gold-events` topic.
- **`tests/test_config.py`** (new) — unit tests for the config contract
  (non-empty constants, env-override precedence, `get_ingest_date`, and a
  subprocess test proving import performs no network I/O).
- **`docs/CONFIGURATION.md`** (new) — the module's contract and the incremental
  adoption plan.

### Design decisions
1. **Default topic value kept as `spotify_playback_events`.** The producer and
   the ES/Mongo consumers already agreed on it, so centralising on this value is
   behaviour-preserving; only the dead Cassandra subscription changes. Adopting a
   versioned taxonomy (e.g. `spotify.playback.events.v1`) would require
   coordinated Kafka/Flink/infra changes (PR-05/PR-10/PR-17) and is deliberately
   left as a future, env-only migration.
2. **Only the topic is wired in this PR.** Endpoint/bucket adoption across the
   batch layer is PR-06; crawl-topic adoption is PR-11. The module defines the
   full surface (single source of truth) with documented incremental adoption.
3. **`sys.path` bootstrap in each job.** With no packaging in place yet, jobs add
   the repo root (derived from `__file__`) to `sys.path` so `common` imports
   under `spark-submit` — portable, mirroring PR-01's path handling.

### Verification
- `python -m py_compile` passes for all edited jobs and the new module.
- `python tests/test_config.py` → all unit tests pass (no pytest dependency).
- `git grep` confirms no topic-name literals remain in the four stream jobs.

### New environment variables
| Variable | Default | Used by |
| :--- | :--- | :--- |
| `TOPIC_PLAYBACK` | `spotify_playback_events` | producer + all stream jobs |
| `TOPIC_ANOMALY` | `spotify_anomaly_events` | (Flink, wired in PR-10) |

### Rollback
Revert restores the per-file topic literals; no state change. New module is
additive. (The Cassandra consumer would return to its previously dead topic.)

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
