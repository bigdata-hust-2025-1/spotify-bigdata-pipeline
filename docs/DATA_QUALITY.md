# Data Quality

`spark_jobs/quality/checks.py` provides reusable data-quality gates between the
medallion layers, so bad data **fails the pipeline** instead of flowing silently
to Gold / serving (finding H2).

## Checks

| Check | Question | Executor |
| :--- | :--- | :--- |
| non-null keys | are any business-key columns null? | `key_checks` |
| key uniqueness | are there duplicate business keys? | `key_checks` |
| value range | do values (e.g. `popularity 0–100`) fall in bounds? | `range_check` |
| FK resolution | did every fact row resolve its dimension surrogate keys (Silver→Gold)? | `fk_check` (reuses `common.modeling.unresolved_fk_count_sql`) |
| freshness | is the newest partition within N days of the run date? | `freshness_check` |

## How a gate works

Each executor returns a `CheckResult` (`name`, `passed`, `severity`, `observed`,
`threshold`, `details`). A `DataQualityReport` aggregates results, logs each as a
structured `dq_check` metric (via `common.logging`), and `raise_if_failed()`
raises `DataQualityError` if any **BLOCK**-severity check failed.

Design: the evaluator/decision logic is pure Python (unit-tested without a JVM);
only the count-computing helpers touch Spark, and they import `pyspark` lazily.

## Severity (warn vs block)

`DATA_QUALITY_SEVERITY` controls the default:

- `block` (default) — a failed gate raises and fails the job.
- `warn` — failures are logged but do not raise (useful when first rolling gates
  out on historical data).

Per-check severity can also be passed explicitly.

## Where it is wired

`bronze_to_silver_all.py` runs the non-null + uniqueness gate on each dataset's
business key **before** the Silver write. On breach the write is skipped and the
error is recorded by the job's `FailureCollector`, so the job exits non-zero
(PR-13). Because the gate runs pre-write, bad rows never land in Silver.

```python
report = key_checks(df_clean, BUSINESS_KEYS[dataset_name], f"silver.{dataset_name}")
report.log(LOG)
report.raise_if_failed()   # DataQualityError on a blocking breach
```

The FK gate (`fk_check`) belongs after the star-schema fact build
(`build_facts.py`), and `freshness_check` / `range_check` can be added per layer
as needed — all share the same `DataQualityReport` shape.

## Validation

- **Unit (runs anywhere):** `python tests/test_quality.py` — seeded
  null-key / duplicate / broken-FK / out-of-range / stale fixtures each fail the
  gate; clean data passes; results log as `dq_check` metrics; `warn` never blocks.
- **Integration (needs Spark):** run `bronze_to_silver_all.py` against a Bronze
  partition seeded with a null/duplicate key and confirm the job exits non-zero
  and writes nothing for that dataset. Documented; not runnable without a cluster.
