# KPI definitions

The site has three management pages and two sets of engineering pages:

| Page | Built by | Audience |
|---|---|---|
| `index.html` (landing) | both scripts | everyone: one panel for AAP, one for OpenShift |
| `aap/index.html` | `aap_health_dashboard.py` | managers: AAP status, KPIs vs target, trends, open AAP issues |
| `openshift/index.html` | `ocp_health_dashboard.py` | managers: OpenShift status, KPIs vs target, trends, open OpenShift issues |
| `aap/env/<name>.html` | `aap_health_dashboard.py` | platform engineers and support: AAP checks, trends, tables |
| `openshift/env/<name>.html` | `ocp_health_dashboard.py` | platform engineers and support: OpenShift checks, trends, tables |

Each script keeps its own `history.json`, so the AAP and OpenShift health scores are independent and
an OpenShift problem never lowers the AAP score (or the other way round). Targets come from
`kpi_targets` in the config file (defaults shown). Only Production is checked by default
(`settings.only_tiers: ["prod"]`).

## Shared definitions

| KPI | Definition | Default target |
|---|---|---|
| Status (RAG) | Red: any critical check fails. Amber: any check fails, or a critical/major check warns. Green: otherwise. | Green |
| Health score | Weighted share of evaluated checks passing: ok = 1, warn = 0.5, fail = 0; weights critical 3, major 2, minor 1. `unknown` checks are excluded. | >= 90 |
| Green days, last 30 | Share of the last 30 recorded days the dashboard was Green | >= 90% |
| Open critical issues | Critical-severity checks currently failing | 0 |

## Ansible Automation Platform KPIs (`aap/index.html`)

| KPI | Definition | Default target | Why it matters |
|---|---|---|---|
| Job success rate, 24h and 30d | successful / (successful + failed + error); canceled jobs excluded | >= 95% | Automation reliability seen by consumers |
| Jobs run, 24h and 30d | Finished controller jobs in the window | trend | Adoption and demand |
| Estimated hours saved, 30d | successful jobs x `minutes_saved_per_job` / 60 | trend | Business value (an estimate; tune `minutes_saved_per_job`, default 15) |
| Peak capacity used | Highest non-container instance group consumed/total capacity | <= 80% | Early warning for scaling task pods |
| Managed hosts / licensed | Subscription host usage | under license | Compliance and renewal planning |
| Subscription days left | Days until the AAP subscription expires | >= 60 | Renewal lead time |
| Active users, 30d | Gateway users with a login in the last 30 days | trend | Adoption |
| EDA activations running / enabled; rules fired 24h | Event-Driven Ansible usage and health | all enabled running | Event-driven coverage |
| Hub storage used | Pulp storage used | <= 80% | Content growth |

Engineering pages add: API latency per service, job duration (average, p95), pending jobs, job event
lag, Redis callback queue, controller database connections, EDA and hub task trends, instance and
instance group tables, top failing templates, launch types and EDA activations.

## OpenShift KPIs (`openshift/index.html`)

| KPI | Definition | Default target | Why it matters |
|---|---|---|---|
| AAP pods not ready | Pods in the AAP namespace not Ready | 0 | Service availability |
| AAP pod restarts since last run | Container restart delta against the previous snapshot | trend | Crash loops, OOM kills |
| Last good AAP backup | Hours since the newest successful AnsibleAutomationPlatformBackup | <= 26 h | Recoverability (RPO) |
| Min route certificate days | Days until the soonest-expiring AAP route certificate | >= 30 | Avoids certificate outages |
| Max PVC used | Fullest persistent volume claim in the AAP namespace | <= 80% | Database and hub storage headroom |
| Critical alerts firing | Prometheus alerts with severity critical (Watchdog excluded) | 0 | Cluster risk |
| Degraded ClusterOperators | ClusterOperators Degraded or not Available | 0 | Cluster health |
| Busiest node memory | Highest node memory utilization | <= 85% | Scheduling headroom |
| etcd WAL fsync p99 | etcd disk latency | < 10 ms (Red Hat) | Control plane stability |

Engineering pages add: operator custom resources, pods, container resource usage, PVCs, route
certificates, warning events, nodes, firing alerts, namespace CPU/memory, node headroom and etcd
latency trends.

## Reading the trends

- A falling health score with stable job success usually means infrastructure drift (certificates,
  backups, PVC growth, alerts) before users notice. Look at the open issues table.
- Rising p95 duration or pending jobs with flat volume points to capacity or execution-node problems;
  compare capacity used and CPU throttling.
- Pod restart deltas are computed against the previous day's snapshot, so a single spike indicates a
  new event, not an accumulated count.
