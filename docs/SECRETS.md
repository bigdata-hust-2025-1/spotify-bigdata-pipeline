# Secrets Management & Rotation Runbook

This repository must never contain live credentials. This document explains
how secrets are configured locally, where they live at runtime, and the
**mandatory rotation procedure** for credentials that were previously exposed.

> **Scope of this document (PR-02).** This PR *stops the leak* (untracks the
> secret files, adds ignore rules, ships safe templates) and *documents the
> rotation*. The actual key rotation is an **owner action** that must be
> performed out-of-band — see [Rotation runbook](#4-rotation-runbook). Wiring
> runtime jobs to Kubernetes Secrets and adding a fail-fast `require_env`
> config helper is tracked separately (roadmap PR-06).

---

## 1. Golden rules

1. **Never commit real credentials.** Only `*.env.example` templates with
   placeholder values are tracked. Real `.env` files are gitignored.
2. **A secret that reached a public repo is compromised — forever.** Removing
   it from the latest commit does **not** undo the exposure (it stays in git
   history and in anyone's clone/fork). The only real fix is **rotation**.
3. **No insecure defaults in code.** Jobs must fail fast when a required
   secret is missing rather than silently falling back to a well-known default
   like `minioadmin` (enforced in PR-06).

---

## 2. Local development setup

Each service reads configuration from a local `.env` file (loaded via
`python-dotenv`). These files are **gitignored** and created from the tracked
templates:

```bash
# Producer (Spotify crawler)
cp ingestion/producer/.env.example ingestion/producer/.env
# Consumer (Kafka -> MinIO)
cp ingestion/consumer/.env.example ingestion/consumer/.env
```

Then edit each `.env` and fill in real values. Because `.env` is ignored,
`git status` will not show it and it cannot be committed by accident.

| File | Contains | Required secrets |
| :--- | :--- | :--- |
| `ingestion/producer/.env` | Spotify crawler config | `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET` |
| `ingestion/consumer/.env` | Kafka→MinIO consumer config | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |

The Spotipy OAuth token cache (`ingestion/producer/.cache`) is also gitignored:
it is generated at runtime and contains a live access/refresh token, so it must
never be committed.

---

## 3. Where secrets live at runtime (production)

Local `.env` files are a **development convenience only**. In the Kubernetes
deployment, secrets are injected as environment variables from **Kubernetes
Secrets**, not baked into images or ConfigMaps:

```bash
# Example: create the Spotify secret in the cluster (values from your rotated app)
kubectl create secret generic spotify-credentials \
  --from-literal=SPOTIFY_CLIENT_ID='<rotated-id>' \
  --from-literal=SPOTIFY_CLIENT_SECRET='<rotated-secret>'

# Example: MinIO root credentials
kubectl create secret generic minio-credentials \
  --from-literal=MINIO_ROOT_USER='<rotated-user>' \
  --from-literal=MINIO_ROOT_PASSWORD='<rotated-password>'
```

Pods reference these via `envFrom` / `secretKeyRef`. Wiring every job to
`require_env(...)` (fail-fast, no insecure default) is completed in PR-06.

---

## 4. Rotation runbook

The following credentials were tracked in this repository and are therefore
**considered compromised**. They MUST be rotated. Untracking the files (this
PR) does not neutralize the old values — only rotation does.

### 4.1 Spotify Client Secret

1. Sign in to the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).
2. Open the app, go to **Settings → Basic Information**.
3. Click **Reset** next to *Client Secret* to generate a new secret.
   (The Client ID does not change; only the secret is rotated.)
4. Update the new secret in:
   - your local `ingestion/producer/.env`, and
   - the `spotify-credentials` Kubernetes Secret (`kubectl create secret ... --dry-run=client -o yaml | kubectl apply -f -`).
5. Confirm the crawler authenticates with the new secret; the old secret is now
   permanently invalid.

### 4.2 MinIO root password

1. Rotate `MINIO_ROOT_PASSWORD` (and, ideally, stop using the root user for
   applications — create a scoped access key/secret per service via the MinIO
   console or `mc admin user add`).
2. Update the value in your local `ingestion/consumer/.env` and the
   `minio-credentials` Kubernetes Secret, then restart the dependent pods.
3. Verify the consumer can still write objects with the new credentials.

### 4.3 Spotify OAuth token cache

The leaked `.cache` token is short-lived (it expires ~1 hour after issue) and
is invalidated when the Client Secret is reset in §4.1. No separate action is
required beyond rotating the Client Secret. The file is now gitignored.

---

## 5. Follow-up: purging secrets from git history (optional)

Rotation makes the exposed values worthless, which is the real remediation.
Removing them from **history** is optional hygiene and is intentionally **not**
part of this PR because it rewrites history (a force-push that invalidates every
existing clone/fork and must be coordinated with all collaborators).

If desired, do it as a separate, coordinated task with
[`git filter-repo`](https://github.com/newren/git-filter-repo) or the BFG:

```bash
# Example (coordinate with the team first — this rewrites every commit hash):
git filter-repo --path ingestion/producer/.env \
                --path ingestion/consumer/.env \
                --path ingestion/producer/.cache \
                --invert-paths
```

---

## 6. Prevention

- A CI secret-scanning gate (`gitleaks`) that fails the build on any detected
  credential is added in roadmap PR-14 (CI pipeline). Until then, the
  `.gitignore` rules and these templates are the primary guard.
- Before every commit, sanity-check with `git status` that no `.env` (or other
  secret) is staged.
