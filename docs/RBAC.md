# Read-only access model

The dashboards need nothing more than read-only tokens, and each script uses only its own:

| Script | Token | Never needs |
|---|---|---|
| `aap_health_dashboard.py` | AAP OAuth token (`aap_token_env`), scope read, Platform Auditor | any OpenShift credential |
| `ocp_health_dashboard.py` | ServiceAccount `aap-health-reader` token (`ocp_token_env`, optional `prometheus_token_env`) | any AAP credential |

Read-only access is enforced in three layers:

1. **Code.** The `Http` class in `healthdash_core.py`, shared by both scripts, can only send `GET` (the method is a class constant handed to
   `urllib.request.Request`); there is no POST/PUT/PATCH/DELETE code path, no basic auth and no
   password handling. The mock platform in `tests/` returns `405` for any other verb, and the test
   suite would fail if one were sent.
2. **AAP token.** An OAuth2 personal access token with **scope `read`**, owned by a dedicated user
   with the **Platform Auditor** role. A `read` scoped token cannot make changes even if the user
   could. The dashboard itself checks this (`gateway.least_privilege`) and warns when the token
   belongs to a superuser.
3. **OpenShift token.** A ServiceAccount whose roles grant only `get`, `list`, `watch`
   (`deploy/openshift/rbac.yaml`). Secrets and ConfigMaps are deliberately not readable, and
   `pods/exec`, `pods/log` and `pods/proxy` are not granted.

## Creating the AAP token (once per environment)

1. Platform gateway UI: Access Management > Users > Create user `svc-aap-health` (normal user, not
   superuser). Assign the **Platform Auditor** role (Access Management > Users > Roles), which
   gives read access across controller, EDA and hub.
2. Log in as that user (or have an admin create it): User details > API token > Create token,
   scope **Read**. Copy the token once.
3. Store it as the secret named by `aap_token_env`, for example `AAP_TOKEN_PROD`.

Token lifetime follows the gateway OAuth setting (`OAUTH2_PROVIDER` access token expiry); set a
rotation reminder for that interval.

## Creating the OpenShift token (once per cluster)

```bash
# edit namespace "aap" in rbac.yaml if your AAP namespace differs
oc apply -f deploy/openshift/rbac.yaml
oc -n aap-health get secret aap-health-reader-token -o jsonpath='{.data.token}' | base64 -d
# verify it is read-only
TOKEN=...; oc --token "$TOKEN" auth can-i --list -n aap | head -40
oc --token "$TOKEN" auth can-i delete pods -n aap          # expect: no
oc --token "$TOKEN" auth can-i get secrets -n aap          # expect: no
```

The same token works against the Thanos querier route (`prometheus_url`) because of the
`cluster-monitoring-view` binding, so `prometheus_token_env` can be omitted.

If the platform team will not grant the cluster-scope role, set `"cluster_checks": false` for that
environment (or skip the ClusterRoleBinding). Cluster checks then show `unknown` with a hint and are
not scored.

## Endpoint permission matrix

### AAP (bearer token, scope read, Platform Auditor)

| API | Endpoints (GET only) | Used for |
|---|---|---|
| Gateway `/api/gateway/v1` | `status/`, `me/`, `users/`, `organizations/` | service status, least privilege, active users, org count |
| Controller `/api/controller/v2` | `ping/`, `instances/`, `instance_groups/`, `metrics/`, `config/` | heartbeats, capacity, event lag, callback queue, subscription |
| Controller | `jobs/`, `workflow_jobs/`, `unified_jobs/`, `system_jobs/`, `system_job_templates/`, `schedules/` | job KPIs, stuck/long jobs, cleanup jobs, missed schedules |
| Controller | `projects/`, `inventory_sources/`, `inventories/`, `hosts/`, `job_templates/`, `workflow_job_templates/`, `credentials/`, `organizations/` | sync failures, object counts (credential lists never include secret values) |
| EDA `/api/eda/v1` | `status/`, `activations/`, `activation-instances/`, `projects/` | activation health, restarts, rules fired |
| Hub `/api/galaxy` | `pulp/api/v3/status/`, `pulp/api/v3/tasks/`, `v3/plugin/ansible/search/collection-versions/`, `v3/plugin/execution-environments/repositories/` | Pulp workers, storage, tasks, content counts |

### OpenShift (ServiceAccount aap-health-reader)

| Scope | API group / resource | Verbs |
|---|---|---|
| AAP namespace | core: pods, persistentvolumeclaims, events, resourcequotas, services, endpoints | get, list, watch |
| AAP namespace | apps: deployments, statefulsets, replicasets | get, list, watch |
| AAP namespace | policy: poddisruptionbudgets; route.openshift.io: routes | get, list, watch |
| AAP namespace | metrics.k8s.io: pods | get, list |
| AAP namespace | operators.coreos.com: clusterserviceversions, subscriptions, installplans | get, list, watch |
| AAP namespace | aap.ansible.com, automationcontroller.ansible.com, eda.ansible.com, automationhub.ansible.com, lightspeed.ansible.com CRs and backups | get, list, watch |
| openshift-etcd | core: pods | get, list, watch |
| Cluster | config.openshift.io: clusterversions, clusteroperators | get, list, watch |
| Cluster | core: nodes; metrics.k8s.io: nodes | get, list, watch |
| Cluster | machineconfiguration.openshift.io: machineconfigpools; certificates.k8s.io: certificatesigningrequests | get, list, watch |
| Cluster | `cluster-monitoring-view` (built-in) for Thanos querier `/api/v1/query` | read |

## GitHub side

The workflow uses `permissions: contents: write` only to push the generated site to `gh-pages`.
Platform tokens are passed to the script via `AAP_HEALTH_SECRETS_JSON` and never written to disk or
to the site. Use `--redact` (default on for scheduled runs) when the Pages site is public.
