# Disaster Recovery & Scalability

How the platform survives failure (RPO/RTO, backup, replication, recovery
procedures) and how each layer scales. Closes findings **F3** (no DR story) and
**F4** (single-region LRS, single node pool).

---

## 1. Recovery objectives (RPO / RTO)

- **RPO** (Recovery Point Objective) — how much data we can afford to lose.
- **RTO** (Recovery Time Objective) — how long recovery may take.

| Data store | What it holds | RPO | RTO | Recovery mechanism |
| :--- | :--- | :--- | :--- | :--- |
| **Iceberg lakehouse** (Silver/Gold on object store) | Source of truth for modelled data | **0** (immutable snapshots) | ~15 min | Snapshot roll-back / time-travel (§3) + object versioning |
| **Object store** (MinIO / ADLS Gen2) | Physical files behind Iceberg + raw Bronze | ≤ 5 min | ~30 min | Bucket/blob **versioning** + replication tier (§4) |
| **Kafka** | In-flight events (buffer, not system of record) | ≤ retention (default 7 d) | ~15 min | Replayable from Bronze; topic replication factor ≥ 3 in prod |
| **Cassandra** (`user_plays`) | Serving copy of playback | ≤ 5 min | ~30 min | Rebuildable from Bronze/Gold; multi-replica keyspace in prod |
| **Elasticsearch** | Search/serving index | **derived** (rebuildable) | ~1 h | Re-run `gold_to_es` from Gold |
| **Airflow metadata** (Postgres) | DAG/run history | ≤ 24 h | ~30 min | Managed Postgres PITR / nightly dump |
| **Spark checkpoints** | Streaming offsets | ≤ 1 min | ~10 min | Recomputed on restart; NOT versioned (churns) |

**Design principle — derive, don't back up everything.** Only **Bronze** (raw
ingest) and the **Iceberg lakehouse** are true systems of record. Silver/Gold
serving copies (Cassandra, ES) are deterministically rebuildable from them, so
their RPO is bounded by how fast we can re-run the batch DAG, not by a backup.

---

## 2. Backup & replication strategy

| Layer | Mechanism |
| :--- | :--- |
| **Cloud object store** | `azurerm_storage_account` with `versioning_enabled = true` + `delete_retention_policy` (30 d) and a **replicated tier** (`storage_replication_type`, default **ZRS**; **GRS/RA-GRS** for cross-region DR). See `azure_iac/`. |
| **MinIO (on-cluster/dev)** | `minio-enable-versioning` Job turns on bucket versioning + noncurrent-version retention on `spotify-bronze/silver/gold/lakehouse`. |
| **Iceberg** | Every write creates an immutable snapshot; `expire_snapshots` (maintenance job, PR-08) keeps a configurable window for roll-back. |
| **Kafka** | Replication factor ≥ 3 across brokers in prod (no data loss on single-broker failure). |
| **Airflow Postgres** | Point-in-time restore (managed) or scheduled `pg_dump` to the object store. |

Replication tiers, at a glance:

- **LRS** — 3 copies, one datacentre. *Dev only* — a DC/AZ loss loses data.
- **ZRS** (default) — 3 copies across availability zones, one region. Survives an AZ loss.
- **GRS / RA-GRS** — ZRS/LRS **plus** async copy to a paired region. Survives a
  regional outage; RA-GRS also allows reads from the secondary. **Use for prod DR.**

---

## 3. Recovery procedures

### 3.1 Iceberg snapshot roll-back (corruption / bad batch)

A bad job that wrote wrong data is undone by rolling back to the prior snapshot —
no restore from backup needed:

```sql
-- Inspect history
SELECT snapshot_id, committed_at, operation
FROM lakehouse.gold.artists_stats.snapshots ORDER BY committed_at DESC;

-- Roll back to a known-good snapshot
CALL lakehouse.system.rollback_to_snapshot('lakehouse.gold.artists_stats', <snapshot_id>);
```

Time-travel to *read* a prior version without mutating the table:

```sql
SELECT * FROM lakehouse.gold.artists_stats VERSION AS OF <snapshot_id>;
```

**Drill (spot-verified):** on a test table, write a bad batch, confirm the wrong
value, `rollback_to_snapshot`, confirm the value is restored, and confirm a new
snapshot did not overwrite history.

### 3.2 Object-version restore (accidental overwrite/delete)

With versioning enabled, a clobbered/deleted object is restored from its prior
version — `mc cp --version-id <id> …` (MinIO) or blob version restore (Azure).

### 3.3 Serving-layer rebuild (Cassandra / ES loss)

Re-run the batch DAG for the affected `ingest_date` window; `gold_to_es` and the
Cassandra sink are idempotent (MERGE / keyed upsert), so a full rebuild converges
to the same state with no duplicates.

### 3.4 Region loss (prod)

Fail over to the geo-replicated secondary (GRS/RA-GRS), stand up AKS from
`azure_iac/` in the paired region, and repoint the lakehouse warehouse at the
replicated storage account. RTO dominated by cluster provisioning (~30–45 min).

---

## 4. Scalability levers

| Layer | Bottleneck | Lever |
| :--- | :--- | :--- |
| **Kafka** | Consumer throughput | Add **partitions** and scale consumers 1:1 (parallelism is capped by partition count). |
| **Spark** | Shuffle / skew / small files | **AQE** + `coalescePartitions` (enabled in `common.spark`, PR-18); tune `spark.sql.shuffle.partitions`; broadcast small dims; Iceberg `write.target-file-size-bytes` (PR-18). |
| **Iceberg** | Small-file explosion | Target file size on write + `rewrite_data_files` compaction (PR-08 maintenance). |
| **Flink** | Keyed state size | Scale parallelism + a RocksDB state backend; keyed-window state partitions by `user_id`. |
| **Cassandra** | Write/read fan-out | Add nodes (linear scale); partition key sized to spread load. |
| **Elasticsearch** | Index size / query load | More primary shards + replicas. |
| **AKS** | Pod scheduling | **Cluster-autoscaler** on the node pool (`enable_auto_scaling`, min/max in `azure_iac/`); a separate spot pool for batch is the natural next step. |

**Kafka sizing rule of thumb:** target throughput ÷ per-partition throughput =
partition count; keep consumers ≤ partitions or they idle.

---

## 5. What is provisioned vs documented

- **Provisioned (code):** object versioning (Azure blob + MinIO Job), replicated
  storage tier (Terraform variable, default ZRS), delete-retention, AKS
  autoscaling, Iceberg snapshots/compaction.
- **Documented (runbook):** the recovery drills (§3), Kafka/Cassandra replication
  factors, Postgres PITR, and cross-region failover — these are operational
  procedures rather than a single applyable manifest.
