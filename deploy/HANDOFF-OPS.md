# Ops handoff — gitoxide loader + dulwich fallback deployment

This document tells the SRE / ops team what to configure in the Helm
charts to deploy the gitoxide-based git loader with the dulwich
fallback safety net. It is self-contained — all references point to
files in this repository.

## 1. What changed in the loader

The git loader's network-fetch and pack-inflation engine was replaced
from dulwich (Python) to gitoxide (Rust via PyO3). The dulwich code
is preserved as a fallback for pathological repositories. No changes
to swh-scheduler, swh-lister, or the data model are required for the
initial deployment.

**Branches** on `swh-loader-git`: the change ships as four stacked MRs —
`mr/1-gix-engine` (gix engine + bindings + wire-into-loader),
`mr/2-size-classed-queues` (size-classed Celery tasks),
`mr/3-dulwich-fallback` (fallback dispatch), and
`mr/4-helm-overlay` (this document + Helm values overlay).

## 2. What ops needs to deploy (ordered)

### Step 1 — Deploy the gix-based loader workers (existing queues)

The existing `loader.git` worker pool runs the new engine
automatically once this branch is merged + released. No Helm change
is needed for the existing workers **unless** you want to activate
the size-based dispatch tiers, which is optional for the first
deployment.

**For the zero-refactor path** (recommended first deployment):
route all visits to the existing `loader.git` queue. The loader
self-promotes by wall-time internally.

### Step 2 — Set the dulwich fallback env var on gix workers

Add the following env var to the gix-based loader worker deployment:

```yaml
env:
  - name: SWH_LOADER_GIT_DULWICH_FALLBACK
    value: "1"
```

**When unset (default):** gix errors propagate as normal visit
failures. The fallback path is disabled.

**When set to any non-empty value:** the loader classifies gix
exceptions and re-dispatches eligible visits to the dulwich fallback
queue via `apply_async`.

**Staging:** set from day one.
**Production:** start unset. Enable on 1% of workers via a Helm
overlay targeting a specific replicaSet. Widen 1% → 10% → 50% →
100% over one week, gated by the metrics in §4 below.

### Step 3 — Deploy the dulwich fallback worker pools

Three new Celery worker deployments, one per tier. Reference values
are in `deploy/values-dulwich-fallback.yaml` in this repo; adapt to
your chart structure.

| Queue name | Task name | Pod spec | Replicas (prod) |
|---|---|---|---|
| `loader.git.dulwich_fallback_small` | `swh.loader.git.tasks.LoadGitDulwichFallbackSmall` | 2 vCPU / 4 GB | 2 |
| `loader.git.dulwich_fallback_large` | `swh.loader.git.tasks.LoadGitDulwichFallbackLarge` | 8 vCPU / 16 GB | 1 |
| `loader.git.dulwich_fallback_xl` | `swh.loader.git.tasks.LoadGitDulwichFallbackXl` | 16 vCPU / 64 GB | 1 |

These workers do NOT need the `SWH_LOADER_GIT_DULWICH_FALLBACK` env
var — they ARE the fallback. The env var controls the gix-side
dispatch decision.

Concurrency: **1** (one visit per worker, matching the gix-side
pattern). Each visit is a long-running pack-fetch + object-inflation
job; concurrency > 1 risks OOM.

### Step 4 — Configure Celery routing

Ensure the Celery broker routes each task name to its queue:

```python
task_routes = {
    # Existing gix-side queues (if size-dispatch is active)
    "swh.loader.git.tasks.UpdateGitRepositorySmall": {"queue": "loader.git.small"},
    "swh.loader.git.tasks.UpdateGitRepositoryLarge": {"queue": "loader.git.large"},
    "swh.loader.git.tasks.UpdateGitRepositoryXl":    {"queue": "loader.git.xl"},
    # Dulwich fallback queues (new)
    "swh.loader.git.tasks.LoadGitDulwichFallbackSmall": {"queue": "loader.git.dulwich_fallback_small"},
    "swh.loader.git.tasks.LoadGitDulwichFallbackLarge": {"queue": "loader.git.dulwich_fallback_large"},
    "swh.loader.git.tasks.LoadGitDulwichFallbackXl":    {"queue": "loader.git.dulwich_fallback_xl"},
}
```

If you are NOT deploying the size-dispatch tiers yet (zero-refactor
path), the first three rows are optional — the existing `loader.git`
queue handles everything.

## 3. Rollback plan

If the fallback mechanism causes operational problems:

1. **Unset `SWH_LOADER_GIT_DULWICH_FALLBACK`** on all gix workers.
   The fallback predicate is disabled; gix errors propagate as
   normal visit failures. No code change needed.
2. Scale the dulwich fallback worker pools to 0 replicas. The queues
   drain; no new work is routed to them.
3. Investigate using the metrics in §4.

If the gix engine itself needs to be rolled back entirely:

1. Revert `swh-loader-git` to the pre-rehaul release.
2. Scale down any size-dispatch + dulwich-fallback worker pools.
3. The existing `loader.git` queue resumes using the dulwich engine
   as before the rehaul.

## 4. Metrics to watch

All metrics are emitted by the **gix-side workers** via statsd.
Configure Prometheus scraping on the gix loader pods.

| Metric | Type | Alert threshold | What it means |
|---|---|---|---|
| `git_dulwich_fallback_total{reason}` | counter | > 0.1% of visits sustained 1h | Rate of visits re-dispatched to dulwich. `reason` label = `GixPackError` / `GixObjectParseError` / `GixTraverseError`. |
| `git_dulwich_fallback_dispatch_failed_total{reason}` | counter | any non-zero | The `apply_async` to the fallback queue failed (broker unreachable, queue misconfigured). Operational incident. |
| `git_safety_net_redispatch_total{from_queue,to_task}` | counter | > 5% of visits | Wall-time-triggered tier promotions. High rate = `T_class` thresholds need tuning or pack_size_kb routing should be enabled (Lane 2). |
| OOM-kill count on `loader.git.small` pods | k8s metric | any non-zero over 7 days | The wall-time promotion didn't fire fast enough → correctness bug in the loader. |

### Grafana dashboard suggestions

- **Panel 1:** `rate(git_dulwich_fallback_total[5m])` by `reason` —
  shows fallback rate broken down by exception class.
- **Panel 2:** `rate(git_safety_net_redispatch_total[5m])` by
  `from_queue` — shows tier-promotion activity.
- **Panel 3:** p95 wall-time per Celery queue — compare against the
  model prediction (mean absolute percentage error 33%).

## 5. Staging gating values

The staging → production transition happens when ALL FOUR gates pass
over the agreed measurement window (proposed: 7 days):

| Gate | Threshold | Source |
|---|---|---|
| Safety-net redispatch rate | < 5% | `git_safety_net_redispatch_total` / total visits |
| Dulwich fallback rate | < 0.1% | `git_dulwich_fallback_total` / total visits |
| Per-tier p95 wall-time fit | within 30% of the size-class throughput model | tracked via existing per-queue p95 dashboards |
| OOM-kill rate on small | 0 | k8s pod restart metrics |

These are starting proposals. The team negotiates final values during
the Phase 1 staging window.

## 6. Files in this repo that ops should read

| File | What's in it |
|---|---|
| `README.rst` (Deployment section) | Env var semantics, queue table, gating values |
| `deploy/values-dulwich-fallback.yaml` | Reference Helm values (adapt to your chart) |
| `swh/loader/git/tasks.py` | Task class names + queue routing expectations |
| `swh/loader/git/loader.py` | `_DULWICH_FALLBACK_TASK` mapping (queue → task name) |
| `deploy/HANDOFF-OPS.md` | This file |

## 7. Contact

For questions about the loader internals, the exception taxonomy that
drives the fallback dispatch, or the size-class throughput model, the
authors of the four MRs (`mr/1-gix-engine` through `mr/4-helm-overlay`)
are the primary contacts. See `swh/loader/git/dulwich_fallback.py` for
the classifier source of truth and `swh/loader/git/loader.py` for the
`_DULWICH_FALLBACK_TASK` mapping.
