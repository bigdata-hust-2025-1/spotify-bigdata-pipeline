# Kubernetes deployment

The platform deploys in two layers with two tools, deliberately:

| Layer | Tool | Where |
| :--- | :--- | :--- |
| Raw infra (Kafka, MinIO, ELK, MongoDB, kafka-ui, Postgres, RBAC, namespaces) | **Kustomize** | `base/` + `overlays/{dev,prod}` |
| Off-the-shelf platforms (Airflow, Spark Operator) | **Helm** | `helm/airflow`, `helm/spark-operator` |

Everything is version-pinned and renders offline — no `:latest`, no captured
live-object dumps, one values source per component.

## Layout

```
kubernetes/
├── base/                       # Kustomize base — all raw infra manifests
│   ├── kustomization.yaml
│   ├── namespaces.yaml         # bigdata + airflow
│   ├── kafka.yaml  kafka-ui.yaml  minio.yaml  mongodb.yaml  elk.yaml
│   ├── airflow-postgres.yaml   # external metadata DB (namespace: airflow)
│   └── rbac-airflow-bigdata.yaml
├── overlays/
│   ├── dev/                    # single-node, laptop/minikube footprint
│   └── prod/                   # HA replicas + larger heaps (patches)
└── helm/
    ├── airflow/values.yaml         # the one canonical Airflow values file
    └── spark-operator/values.yaml  # replaces the old fix_operator.yaml dump
```

## Deploy

### 1. Infra (Kustomize)

```bash
# Preview (fully offline, no cluster needed):
kubectl kustomize kubernetes/overlays/dev

# Apply:
kubectl apply -k kubernetes/overlays/dev     # or overlays/prod
```

`dev` is the base as-is (single replicas, small heaps). `prod` layers patches:
two `kafka-ui` replicas and a larger Elasticsearch heap. Stateful singletons
(MinIO, Elasticsearch data, Postgres) stay single-replica by design — scaling
them needs a real HA storage story (tracked in `docs/DR_AND_SCALING.md`, PR-19).

### 2. Airflow (Helm)

```bash
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow --create-namespace \
  --version 1.15.0 \
  -f kubernetes/helm/airflow/values.yaml
```

Uses `KubernetesExecutor` against the external `airflow-postgres` StatefulSet
(the chart's bundled Postgres/Redis are disabled in the values file).

### 3. Spark Operator (Helm)

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace bigdata --create-namespace \
  --version 2.4.0 \
  -f kubernetes/helm/spark-operator/values.yaml
```

Watches the `bigdata` namespace, where the batch DAG (PR-12) submits
`SparkApplication`s.

## What changed in PR-17 (consolidation)

Removed as redundant / non-reproducible:
`airflow/airflow-values.yaml`, `airflow/airflow.values.yaml`,
`airflow/airflow.values.utf8.yaml`, `airflow/airflow.values.current.yaml`
(a `helm get values` UTF-16 dump), `airflow/airflow.manifest.yaml`
(a 2332-line rendered-manifest dump), and `fix_operator.yaml`
(a live `kubectl get -o yaml` of the operator). Kept the single well-documented
`helm/airflow/values.yaml`. Pinned `kafka-ui` and `minio` off `:latest`.
Introduced the `base/` + `overlays/{dev,prod}` structure and the missing
`airflow` namespace.
