# Runbook - responding to dashboard findings

Each failing or warning check on an environment page has a hint and, where one exists, the matching
script from the AAP 2.6 Troubleshooting Toolkit. This runbook covers the common findings. All
investigation commands below are read-only; remediation steps that change the platform should go
through your normal change process.

## Daily routine (support / on-call)

1. Open the overview. Anything Red is triaged the same day; Amber within the week.
2. Open the environment page, sort by the Open issues table (failures first, then severity).
3. Attach `data/checks_latest.csv` to the incident or change record when escalating.

## Common findings

| Check | Likely cause | First steps |
|---|---|---|
| `gateway.status` / `gateway.token_valid` | Gateway pod down, expired token | `oc -n aap get pods -l app.kubernetes.io/component=platform-gateway`; rotate the read token (docs/RBAC.md) |
| `controller.heartbeat`, `controller.instances` | Task pod restart, DB or Redis issue | `oc -n aap get pods`, controller task pod logs; toolkit `python/01` and `python/03` |
| `controller.capacity` | Task pod limits too small for workload | Raise `task_resource_requirements` / replicas on the AutomationController (performance considerations guide) |
| `controller.event_lag`, `controller.callback_queue` | Callback receiver backlog, DB slow | Check DB latency and callback receiver pod CPU; consider `task_replicas` |
| `controller.job_success` | Failing templates (see top failing table) | Engage template owners; check credential or inventory sync failures first |
| `controller.pending`, `controller.long_running` | No capacity in instance group, stuck job | Instance group detail table; cancel stuck jobs via normal change process |
| `controller.cleanup_jobs` | Cleanup Job/Activity Stream schedules disabled | Re-enable the system job schedules; DB growth affects the operator-managed DB limits |
| `eda.*` | Activation restart loops, decision environment pull failures | Activation instance logs in the EDA UI; toolkit EDA script |
| `hub.status`, `hub.tasks_failed`, `hub.storage` | Pulp workers offline, storage full | Hub worker pods, PVC usage (`platform.pvc_usage`) |
| `platform.backup` | No recent successful AnsibleAutomationPlatformBackup | Create a scheduled backup (CronJob that creates an `AnsibleAutomationPlatformBackup` CR); verify the backup PVC has space |
| `platform.operator_db_limits` | Operator-managed PostgreSQL beyond its recommended envelope | Plan migration to an external, customer-managed database |
| `platform.pods_ready`, `platform.crashloop`, `platform.oomkilled` | CrashLoopBackOff or OOMKilled | `oc -n aap describe pod <pod>`; raise memory limits on the relevant CR |
| `platform.route_tls` | Route certificate close to expiry | Renew the route or ingress certificate |
| `cluster.version`, `cluster.operators` | Upgrade in progress, degraded operator | `oc get clusterversion`, `oc get co`; follow the OpenShift troubleshooting guide |
| `cluster.nodes_ready`, `cluster.node_pressure`, `cluster.mcp` | NotReady/pressure nodes, MCP degraded | `oc describe node`, `oc get mcp` |
| `cluster.csr` | Pending CSRs (kubelet certificate rotation) | Review and approve legitimate CSRs (cluster admin) |
| `cluster.etcd_*` | Slow disk or network for control plane | Follow the etcd recommended practices; check control plane disk latency |
| `cluster.alerts` | Firing Prometheus alerts | Console > Observe > Alerting |

## When the dashboard itself fails

- Exit code 1: one environment or section could not be collected. The page shows `unknown` checks
  with the HTTP error; usually an expired token or a network/firewall change on the runner.
- Exit code 2: nothing collected or invalid config; the workflow run goes red and the previous site
  remains published.
- 403 on cluster checks: expected when cluster-scope RBAC is not granted; see docs/RBAC.md.
