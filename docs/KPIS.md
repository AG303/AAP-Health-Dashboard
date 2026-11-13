# KPI definitions

The overview page (`index.html`) is written for managers; each environment page (`env/<name>.html`)
is written for platform engineers and support. Every KPI below is computed from the daily snapshot
and stored in `history.json`, so the trend charts and the "last 30 days" figures are reproducible.
Targets come from `kpi_targets` in the config file (defaults shown).

## Management KPIs (overview page)

| KPI | Definition | Default target | Why it matters |
|---|---|---|---|
| Environment status (RAG) | Red: any critical check fails. Amber: any check fails, or a critical/major check warns. Green: otherwise. | Green | Single answer to "is the platform OK today" |
| Health score | Weighted share of evaluated checks passing: ok = 1, warn = 0.5, fail = 0; weights critical 3, major 2, minor 1. `unknown` checks are excluded. | >= 90 | Comparable day to day and across environments |
| Green days, last 30 | Share of the last 30 recorded days the environment was Green | >= 90% | Stability / service reliability trend |
| Job success rate, 24h and 30d | successful / (successful + failed + error); canceled jobs excluded | >= 95% | Automation reliability seen by consumers |
| Jobs run, 24h and 30d | Finished controller jobs (playbook runs) in the window | trend | Adoption and demand |
| Estimated hours saved, 30d | successful jobs x `minutes_saved_per_job` / 60 | trend | Business value. This is an estimate; tune `minutes_saved_per_job` (default 15) to your organization's measured manual effort, or set it per environment |
| Peak capacity used | Highest instance group consumed/total capacity | <= 80% | Early warning for scaling the task pods |
| Managed hosts / licensed | Subscription host usage | under license | Compliance and renewal planning |
| Subscription days left | Days until the AAP subscription expires | >= 60 | Renewal lead time |
| Active users, 30d | Gateway users with a login in the last 30 days | trend | Adoption |
| EDA activations running / enabled; rules fired 24h | Event-Driven Ansible usage and health | all enabled running | Event-driven automation coverage |
| Last good backup age | Hours since the newest successful AnsibleAutomationPlatformBackup | <= 26 h | Recoverability (RPO) |
| Min route certificate days | Days until the soonest-expiring AAP route certificate | >= 30 | Avoids certificate outages |
| Open critical issues | Critical-severity checks currently failing | 0 | Risk exposure |

## Engineering and support statistics (environment pages)

- Component tiles (gateway, controller, EDA, hub, AAP namespace, cluster) with pass/warn/fail counts.
- Every check with value, detail, remediation hint, troubleshooting-toolkit script reference and Red Hat reference link.
- Trend charts: health score, check status counts, job success, job volume, average and p95 job duration, pending jobs, capacity used, event lag and callback queue, DB connections, EDA activations and rules fired, hub tasks, pod restarts, PVC usage, memory vs limits, firing alerts and etcd latency.
- Detail tables: instances, instance groups, top failing templates, pods, PVCs, operator CSV, routes, nodes, ClusterOperators.
- Machine-readable exports: `data/latest.json`, `data/checks_latest.csv`, daily `data/snapshots/`.

## Reading the trends

- A falling health score with stable job success usually means infrastructure drift (certificates,
  backups, PVC growth, alerts) before users notice. Look at the open issues table.
- Rising p95 duration or pending jobs with flat volume points to capacity or execution-node problems;
  compare capacity used and CPU throttling.
- Pod restart deltas are computed against the previous day's snapshot, so a single spike indicates a
  new event, not an accumulated count.
