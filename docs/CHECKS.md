# Health check catalog

Generated from the script's check definitions. Every check is a read-only GET. Severity drives scoring
(critical x3, major x2, minor x1) and the RAG status: Red when any critical check fails, Amber when any
check fails or a critical/major check warns, otherwise Green. Thresholds are the defaults in
`DEFAULT_THRESHOLDS` and can be overridden globally or per environment under `thresholds`.

Checks that cannot be evaluated (403/404, feature disabled, Prometheus not configured) report `unknown`
and are excluded from the score, so missing permissions never look like an outage.

## Platform gateway

| Check ID | Title | Severity | Reference |
|---|---|---|---|
| `gateway.latency` | Gateway API response time | minor | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `gateway.least_privilege` | Dashboard token is read-only (least privilege) | minor |  |
| `gateway.status` | Gateway status endpoint (all services good) | critical | [AAP 2.6 - Platform gateway OpenAPI (/api/gateway/v1/status/)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/configure-con_gw_open_api_specification) |
| `gateway.token` | (conditional - emitted only when the feature applies) | | |
| `gateway.token_valid` | API token valid | critical |  |

## Automation controller

| Check ID | Title | Severity | Reference |
|---|---|---|---|
| `controller.callback_queue` | Redis callback receiver queue | major | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `controller.capacity` | Instance group capacity headroom | major | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `controller.cleanup_jobs` | Cleanup management jobs scheduled and passing | minor | [AAP 2.6 - Operator growth topology (operator database limits)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/plan-ref_ocp_a_env_a) |
| `controller.event_lag` | Job event processing lag | major | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `controller.health_check_age` | Instance health checks recent | minor | [AAP 2.6 - Planning your cluster (ping, heartbeats, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/configure-planning_your_cluster) |
| `controller.heartbeat` | Instance heartbeats current (ping) | critical | [AAP 2.6 - Planning your cluster (ping, heartbeats, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/configure-planning_your_cluster) |
| `controller.host_usage` | Managed hosts within subscription | major | [Red Hat AAP life cycle](https://access.redhat.com/support/policy/updates/ansible-automation-platform) |
| `controller.instances` | Instances ready with no health-check errors | critical | [AAP 2.6 - Planning your cluster (ping, heartbeats, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/configure-planning_your_cluster) |
| `controller.inventory_sources` | Inventory source syncs healthy | minor | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `controller.job_success` | Job success rate (24h) | major | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `controller.long_running` | No runaway jobs (> 4h) | minor | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `controller.pending` | No stuck pending jobs | major | [AAP 2.6 - Planning your cluster (ping, heartbeats, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/configure-planning_your_cluster) |
| `controller.projects` | Project syncs healthy | minor | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `controller.schedules` | Schedules firing on time | major | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `controller.subscription` | Subscription valid and compliant | critical | [Red Hat AAP life cycle](https://access.redhat.com/support/policy/updates/ansible-automation-platform) |

## Event-Driven Ansible

| Check ID | Title | Severity | Reference |
|---|---|---|---|
| `eda.activations` | Enabled rulebook activations running | major | [AAP 2.6 - EDA API root (status, activations, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/administer-assembly_simplified_event_routing) |
| `eda.failed_instances` | No failed rulebook processes (24h) | minor | [AAP 2.6 - EDA API root (status, activations, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/administer-assembly_simplified_event_routing) |
| `eda.projects` | EDA project imports healthy | minor | [AAP 2.6 - EDA API root (status, activations, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/administer-assembly_simplified_event_routing) |
| `eda.restarts` | Activations not restart-looping | minor | [AAP 2.6 - EDA API root (status, activations, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/administer-assembly_simplified_event_routing) |
| `eda.status` | EDA status endpoint | critical | [AAP 2.6 - EDA API root (status, activations, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/administer-assembly_simplified_event_routing) |

## Automation hub

| Check ID | Title | Severity | Reference |
|---|---|---|---|
| `hub.status` | Hub (Pulp) status: workers, content apps, DB, Redis | critical | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `hub.storage` | Hub artifact storage free | major | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `hub.tasks_failed` | No failed hub tasks (24h) | minor | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `hub.tasks_waiting` | Hub task queue draining | minor | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |

## OpenShift - AAP namespace

| Check ID | Title | Severity | Reference |
|---|---|---|---|
| `platform.aap_cr` | AnsibleAutomationPlatform CR reconciled | critical | [AAP 2.6 - AnsibleAutomationPlatform custom resource](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_) |
| `platform.alerts` | No alerts firing in the AAP namespace | major | [Red Hat - OpenShift 4 cluster health pre-checks](https://access.redhat.com/solutions/7004992) |
| `platform.backup` | Recent successful platform backup | critical | [AAP 2.6 - Back up the platform on OpenShift](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/administer-assembly_aap_backup) |
| `platform.component_crs` | Component CRs reconciled (controller, EDA, hub) | major | [AAP 2.6 - AnsibleAutomationPlatform custom resource](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_) |
| `platform.cpu_throttling` | AAP pods not CPU throttled (1h) | minor | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `platform.crashloop` | No CrashLoopBackOff / image pull errors | critical | [AAP 2.6 - AnsibleAutomationPlatform custom resource](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_) |
| `platform.events` | Warning events in namespace (24h) | minor | [AAP 2.6 - AnsibleAutomationPlatform custom resource](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_) |
| `platform.hub_rwx` | Hub file storage is ReadWriteMany | minor | [AAP 2.6 - Operator growth topology (operator database limits)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/plan-ref_ocp_a_env_a) |
| `platform.mem_limits` | Containers below memory limit | major | [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring) |
| `platform.oomkilled` | No OOMKilled containers (24h) | major | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `platform.operator_csv` | AAP operator CSV Succeeded | critical | [AAP 2.6 - AnsibleAutomationPlatform custom resource](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_) |
| `platform.operator_db_limits` | Operator-managed database within supported limits | minor | [AAP 2.6 - Operator growth topology (operator database limits)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/plan-ref_ocp_a_env_a) |
| `platform.operator_upgrade` | Operator update pending approval | minor | [Red Hat AAP life cycle](https://access.redhat.com/support/policy/updates/ansible-automation-platform) |
| `platform.pdb` | PodDisruptionBudgets allow node drains | minor | [Red Hat - OpenShift 4 cluster health pre-checks](https://access.redhat.com/solutions/7004992) |
| `platform.pod_spread` | Replicas spread across worker nodes | minor | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `platform.pods_ready` | AAP pods Running and Ready | critical | [AAP 2.6 - AnsibleAutomationPlatform custom resource](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_) |
| `platform.pvc_bound` | PersistentVolumeClaims Bound | critical | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `platform.pvc_usage` | PVC free space (database, hub, redis) | major | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `platform.quota` | ResourceQuota headroom | major | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `platform.restarts` | Pod restarts since last run | major | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `platform.route_tls` | Route TLS certificates not expiring | critical | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `platform.routes` | Routes admitted by the ingress controller | major | [AAP 2.6 - AnsibleAutomationPlatform custom resource](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_) |
| `platform.task_limits` | Controller task containers have resource limits | minor | [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index) |
| `platform.token` | (conditional - emitted only when the feature applies) | | |
| `platform.workloads` | Deployments and StatefulSets at desired replicas | critical | [AAP 2.6 - AnsibleAutomationPlatform custom resource](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_) |

## OpenShift - cluster

| Check ID | Title | Severity | Reference |
|---|---|---|---|
| `cluster.alerts` | No critical alerts firing | major | [Red Hat - OpenShift 4 cluster health pre-checks](https://access.redhat.com/solutions/7004992) |
| `cluster.csr` | No pending certificate signing requests | minor | [Red Hat - OpenShift 4 cluster health pre-checks](https://access.redhat.com/solutions/7004992) |
| `cluster.etcd_commit` | etcd backend commit p99 | minor | [OpenShift cluster-etcd-operator - etcd performance metrics](https://github.com/openshift/cluster-etcd-operator/blob/master/docs/performance-metrics.md) |
| `cluster.etcd_fsync` | etcd WAL fsync p99 < 10 ms | major | [OpenShift 4.18 - Recommended performance practices (etcd)](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/scalability_and_performance/recommended-performance-and-scalability-practices-2) |
| `cluster.etcd_leader` | etcd leader stable (24h) | major | [OpenShift 4.18 - Recommended performance practices (etcd)](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/scalability_and_performance/recommended-performance-and-scalability-practices-2) |
| `cluster.etcd_members` | etcd members running | critical | [OpenShift 4.18 - Recommended performance practices (etcd)](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/scalability_and_performance/recommended-performance-and-scalability-practices-2) |
| `cluster.etcd_operator` | etcd operator healthy | critical | [OpenShift 4.18 - Recommended performance practices (etcd)](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/scalability_and_performance/recommended-performance-and-scalability-practices-2) |
| `cluster.etcd_rtt` | etcd peer round trip p99 < 50 ms | minor | [OpenShift 4.18 - Recommended performance practices (etcd)](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/scalability_and_performance/recommended-performance-and-scalability-practices-2) |
| `cluster.mcp` | MachineConfigPools updated, not Degraded | major | [Red Hat - OpenShift 4 cluster health pre-checks](https://access.redhat.com/solutions/7004992) |
| `cluster.node_headroom` | Node CPU and memory headroom | major | [OpenShift 4.20 - Validation and troubleshooting](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/pdf/validation_and_troubleshooting/index) |
| `cluster.node_pressure` | No Memory/Disk/PID pressure on nodes | major | [OpenShift 4.22 - Working with nodes (node conditions)](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/nodes/working-with-nodes) |
| `cluster.nodes_ready` | Nodes Ready | critical | [OpenShift 4.22 - Working with nodes (node conditions)](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/nodes/working-with-nodes) |
| `cluster.nodes_schedulable` | No nodes left cordoned | minor | [Red Hat - OpenShift 4 cluster health pre-checks](https://access.redhat.com/solutions/7004992) |
| `cluster.operators` | ClusterOperators Available and not Degraded | critical | [Red Hat - OpenShift 4 cluster health pre-checks](https://access.redhat.com/solutions/7004992) |
| `cluster.upgradeable` | Cluster Upgradeable | minor | [Red Hat - OpenShift 4 cluster health pre-checks](https://access.redhat.com/solutions/7004992) |
| `cluster.version` | ClusterVersion Available, not Failing | critical | [OpenShift 4.20 - Validation and troubleshooting](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/pdf/validation_and_troubleshooting/index) |

## Default thresholds

| Key | Default |
|---|---|
| `api_latency_warn_ms` | 1500 |
| `api_latency_fail_ms` | 5000 |
| `heartbeat_warn_s` | 300 |
| `heartbeat_fail_s` | 900 |
| `instance_health_check_stale_h` | 24 |
| `capacity_warn_pct` | 80 |
| `capacity_fail_pct` | 95 |
| `job_success_warn_pct` | 95 |
| `job_success_fail_pct` | 85 |
| `job_success_min_jobs` | 10 |
| `pending_warn_min` | 10 |
| `pending_fail_min` | 30 |
| `long_running_h` | 4 |
| `missed_schedule_grace_min` | 60 |
| `event_lag_warn_s` | 10 |
| `event_lag_fail_s` | 60 |
| `callback_queue_warn` | 10000 |
| `callback_queue_fail` | 100000 |
| `license_days_warn` | 60 |
| `license_days_fail` | 14 |
| `host_usage_warn_pct` | 90 |
| `host_usage_fail_pct` | 100 |
| `eda_restart_warn` | 5 |
| `hub_storage_warn_pct` | 80 |
| `hub_storage_fail_pct` | 90 |
| `hub_waiting_task_warn_min` | 30 |
| `pod_restarts_warn` | 3 |
| `pod_restarts_fail` | 10 |
| `pvc_warn_pct` | 80 |
| `pvc_fail_pct` | 90 |
| `mem_limit_warn_pct` | 85 |
| `mem_limit_fail_pct` | 95 |
| `cert_warn_days` | 30 |
| `cert_fail_days` | 7 |
| `backup_warn_h` | 26 |
| `backup_fail_h` | 72 |
| `warning_events_warn` | 20 |
| `db_connections_warn_pct` | 80 |
| `db_connections_fail_pct` | 95 |
| `operator_db_max_connections` | 100 |
| `operator_db_max_gb` | 100 |
| `operator_db_max_concurrent_jobs` | 100 |
| `etcd_fsync_warn_ms` | 10 |
| `etcd_fsync_fail_ms` | 20 |
| `etcd_commit_warn_ms` | 25 |
| `etcd_commit_fail_ms` | 100 |
| `etcd_peer_rtt_warn_ms` | 50 |
| `etcd_peer_rtt_fail_ms` | 100 |
| `etcd_leader_changes_warn` | 1 |
| `etcd_leader_changes_fail` | 5 |
| `node_cpu_warn_pct` | 80 |
| `node_cpu_fail_pct` | 95 |
| `node_mem_warn_pct` | 85 |
| `node_mem_fail_pct` | 95 |
| `cpu_throttle_warn_pct` | 25 |
| `cpu_throttle_fail_pct` | 50 |
| `quota_warn_pct` | 85 |
| `quota_fail_pct` | 95 |

## Red Hat references

- `aap_cluster`: [AAP 2.6 - Planning your cluster (ping, heartbeats, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/configure-planning_your_cluster)
- `aap_monitor`: [AAP 2.6 - Ensure system health through monitoring (metrics of interest)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-ensure_system_health_and_efficiency_through_monitoring)
- `aap_metrics`: [AAP 2.6 - Send metrics to system monitoring software](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/observe-assembly_controller_metrics)
- `aap_gw_status`: [AAP 2.6 - Platform gateway OpenAPI (/api/gateway/v1/status/)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/configure-con_gw_open_api_specification)
- `aap_eda_api`: [AAP 2.6 - EDA API root (status, activations, instances)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/administer-assembly_simplified_event_routing)
- `aap_topology`: [AAP 2.6 - Operator growth topology (operator database limits)](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/plan-ref_ocp_a_env_a)
- `aap_perf_ocp`: [AAP 2.6 - Performance considerations for operator environments](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/html-single/performance_considerations_for_operator_environments/index)
- `aap_backup`: [AAP 2.6 - Back up the platform on OpenShift](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/administer-assembly_aap_backup)
- `aap_cr`: [AAP 2.6 - AnsibleAutomationPlatform custom resource](https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_)
- `aap_lifecycle`: [Red Hat AAP life cycle](https://access.redhat.com/support/policy/updates/ansible-automation-platform)
- `ocp_upgrade_checks`: [Red Hat - OpenShift 4 cluster health pre-checks](https://access.redhat.com/solutions/7004992)
- `ocp_validate`: [OpenShift 4.20 - Validation and troubleshooting](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/pdf/validation_and_troubleshooting/index)
- `ocp_nodes`: [OpenShift 4.22 - Working with nodes (node conditions)](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/nodes/working-with-nodes)
- `ocp_etcd`: [OpenShift 4.18 - Recommended performance practices (etcd)](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/scalability_and_performance/recommended-performance-and-scalability-practices-2)
- `ocp_etcd_metrics`: [OpenShift cluster-etcd-operator - etcd performance metrics](https://github.com/openshift/cluster-etcd-operator/blob/master/docs/performance-metrics.md)
- `toolkit`: [AAP 2.6 Troubleshooting Toolkit (this project)]()
