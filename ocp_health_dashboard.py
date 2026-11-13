#!/usr/bin/env python3
"""
ocp_health_dashboard.py - daily OpenShift health dashboard for an AAP 2.6 operator deployment.

OpenShift data only: every check uses the OpenShift (Kubernetes) API and, optionally, the
cluster monitoring Prometheus/Thanos API. AAP application data (jobs, capacity, EDA, hub) is
collected separately by aap_health_dashboard.py, which has its own token, history and pages.

Output (static site, no JavaScript, no CDN):
  <out>/openshift/index.html        management view: status, KPIs vs target, trends, open issues
  <out>/openshift/env/<name>.html   engineering / support detail per environment
  <out>/openshift/history.json, data/latest.json, data/checks_latest.csv, data/snapshots/<date>.json
  <out>/index.html                  landing page linking the AAP and OpenShift dashboards

Checks (each cites the Red Hat guidance it implements; see docs/CHECKS.md)
  AAP namespace   operator custom resources (AnsibleAutomationPlatform, controller, EDA, hub),
                  operator Subscription / CSV, pods (not ready, crash loops, OOM kills, restarts),
                  deployments / statefulsets, PVCs (bound, usage), CPU and memory vs limits,
                  warning events, route certificates, resource quotas, backups (AutomationControllerBackup
                  / AnsibleAutomationPlatformBackup age), operator-managed database replica / size limits
  Cluster         ClusterVersion, ClusterOperators, nodes (Ready, pressure, unschedulable), node CPU /
  (optional)      memory headroom, MachineConfigPools, etcd members, certificate signing requests
  Prometheus      firing alerts (critical / warning / AAP namespace), etcd WAL fsync, backend commit,
  (optional)      peer round trip and leader changes

Read-only by design: HTTP GET only (enforced in the Http class), bearer token only. Use the
aap-health-reader ServiceAccount from deploy/openshift/rbac.yaml (get/list/watch Role in the
AAP namespace, optional cluster-reader-style ClusterRole and cluster-monitoring-view).
Cluster-scope and Prometheus checks report Unknown, never Fail, when that access is missing.

Which environments run: every environment with "enabled" not false and an ocp_api_url, limited
by settings.only_tiers (for example ["prod"]) or --env.

Usage
  python3 ocp_health_dashboard.py -c environments.json -o site
  python3 ocp_health_dashboard.py -c environments.json -o site --no-cluster

Exit codes: 0 all collected; 1 partial (dashboard still written); 2 nothing collected / bad
config; 3 --fail-on tripped.

Requirements: Python 3.9+, standard library only, healthdash_core.py next to this file.
"""
import math
import re
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from healthdash_core import (  # noqa: E402
    ApiError, BaseCollector, Dashboard, Http, card, common_kpis, cond, cond_true, cpu_cores, esc,
    gib, grade_high, grade_low, iso, kcell, log, lookup_secret, mem_bytes, num, parse_dt, pct, pill,
    run_dashboard, short, status_card, tls_expiry, vs_target, worst,
)

# --------------------------------------------------------------------------- #
# Pod classification (operator naming: <cr>-<component>-<role>-<hash>)
# --------------------------------------------------------------------------- #
POD_RULES = [
    ("operator", r"(operator-controller-manager|-operator-|^aap-operator|resource-operator)"),
    ("postgres", r"postgres"),
    ("redis", r"redis"),
    ("mcp", r"mcp"),
    ("lightspeed", r"lightspeed"),
    ("controller", r"controller"),
    ("eda", r"(^|-)eda(-|$)"),
    ("hub", r"(^|-)hub(-|$)"),
    ("gateway", r"gateway"),
]


def classify_pod(name):
    n = name.lower()
    for comp, rx in POD_RULES:
        if re.search(rx, n):
            return comp
    return "other"


def workload_of(pod):
    """Deployment/StatefulSet-ish name of a pod (strip the replica-set / ordinal suffix)."""
    name = pod["metadata"]["name"]
    for ref in pod["metadata"].get("ownerReferences") or []:
        if ref.get("kind") == "ReplicaSet":
            return re.sub(r"-[a-z0-9]{6,10}$", "", ref.get("name", name))
        if ref.get("kind") == "StatefulSet":
            return ref.get("name", name)
    return re.sub(r"(-[a-z0-9]{6,10})?-[a-z0-9]{5}$", "", name)


class OcpCollector(BaseCollector):
    def setup(self):
        env = self.env
        verify, ca = env.get("ocp_verify_tls", self.verify), env.get("ocp_ca_bundle", self.ca)
        self.ocp = Http("ocp", env["ocp_api_url"], lookup_secret(env.get("ocp_token_env")), verify, ca, self.timeout)
        self.prom = None
        if env.get("prometheus_url"):
            tok = lookup_secret(env.get("prometheus_token_env") or env.get("ocp_token_env"))
            self.prom = Http("prom", env["prometheus_url"], tok, verify, ca, self.timeout)

    def run(self):
        log("  [%s] collecting OpenShift" % self.env["name"], self.quiet)
        if not self.ocp.token:
            self.add("platform.token", "platform", "OpenShift token configured", "fail", "missing",
                     "Environment variable %s is empty" % self.env.get("ocp_token_env"),
                     "Create the aap-health-reader ServiceAccount token (deploy/openshift/)", "critical")
            return self
        self.section("platform", "operator custom resources", self.ocp_crs)
        self.section("platform", "operator subscription", self.ocp_operator)
        self.section("platform", "pods and workloads", self.ocp_workloads)
        self.section("platform", "storage", self.ocp_storage)
        self.section("platform", "resource usage", self.ocp_usage)
        self.section("platform", "events, routes and quotas", self.ocp_events_routes)
        self.section("platform", "backups", self.ocp_backups)
        self.section("platform", "operator database limits", self.ocp_database_limits)
        if self.s.get("cluster_checks", True):
            self.section("cluster", "cluster health", self.cluster_checks)
        if self.prom:
            self.section("cluster", "prometheus", self.prometheus_checks)
        return self

    # ======================================================================= #
    # OpenShift - AAP namespace
    # ======================================================================= #
    def _cr_state(self, obj):
        """Return (status, text) from operator CR conditions (Running / Successful / Failure)."""
        conds = ((obj or {}).get("status") or {}).get("conditions") or []
        by = {c.get("type"): c for c in conds}
        fail = by.get("Failure")
        if fail and str(fail.get("status")).lower() == "true":
            return "fail", "Failure: " + short(fail.get("message") or fail.get("reason"), 200)
        succ = by.get("Successful")
        run = by.get("Running")
        if succ and str(succ.get("status")).lower() == "true":
            return "ok", "Successful"
        if run and str(run.get("status")).lower() == "true":
            return "warn", "Reconciling (Running=True, not yet Successful)"
        if not conds:
            return "unknown", "no status conditions"
        return "warn", ", ".join("%s=%s" % (c.get("type"), c.get("status")) for c in conds)

    def ocp_crs(self):
        o, ns = self.ocp, self.ns
        aap = o.json("/apis/aap.ansible.com/v1alpha1/namespaces/%s/ansibleautomationplatforms/%s" % (ns, self.cr))
        self.info["aap_cr_spec"] = aap.get("spec") or {}
        st, txt = self._cr_state(aap)
        self.add("platform.aap_cr", "platform", "AnsibleAutomationPlatform CR reconciled", st, self.cr, txt,
                 "oc describe ansibleautomationplatform %s -n %s; check the operator logs" % (self.cr, ns),
                 "critical", "aap_cr")
        rows = []
        for comp, group, ver, plural in (("controller", "automationcontroller.ansible.com", "v1beta1",
                                          "automationcontrollers"),
                                         ("eda", "eda.ansible.com", "v1alpha1", "edas"),
                                         ("hub", "automationhub.ansible.com", "v1beta1", "automationhubs"),
                                         ("lightspeed", "lightspeed.ansible.com", "v1alpha1", "ansiblelightspeeds")):
            try:
                items = o.k8s_list("/apis/%s/%s/namespaces/%s/%s" % (group, ver, ns, plural))
            except ApiError as exc:
                if exc.code == 404:
                    continue
                raise
            for it in items:
                s2, t2 = self._cr_state(it)
                rows.append({"kind": plural, "name": it["metadata"]["name"], "status": s2, "detail": t2,
                             "version": (it.get("status") or {}).get("version")})
        self.details["component_crs"] = rows
        bad = [r for r in rows if r["status"] in ("fail", "warn")]
        self.add("platform.component_crs", "platform", "Component CRs reconciled (controller, EDA, hub)",
                 worst([r["status"] for r in rows]) if rows else "unknown",
                 "%d/%d ok" % (len(rows) - len(bad), len(rows)),
                 "; ".join("%s/%s: %s" % (r["kind"], r["name"], r["detail"]) for r in bad) or "all Successful",
                 "Check the component operator pod logs in %s" % self.op_ns, "major", "aap_cr")

    def ocp_operator(self):
        o = self.ocp
        csvs = o.k8s_list("/apis/operators.coreos.com/v1alpha1/namespaces/%s/clusterserviceversions" % self.op_ns)
        aap = [c for c in csvs if re.search(r"aap-operator|ansible-automation-platform", c["metadata"]["name"])]
        if not aap:
            self.add("platform.operator_csv", "platform", "AAP operator CSV Succeeded", "fail", "not found",
                     "No aap-operator ClusterServiceVersion in %s" % self.op_ns,
                     "Check operator_namespace in environments.json", "critical", "aap_cr")
        else:
            c = sorted(aap, key=lambda x: x["metadata"].get("creationTimestamp", ""))[-1]
            phase = (c.get("status") or {}).get("phase")
            self.info["operator_version"] = (c.get("spec") or {}).get("version") or c["metadata"]["name"]
            self.add("platform.operator_csv", "platform", "AAP operator CSV Succeeded",
                     "ok" if phase == "Succeeded" else "fail", phase or "unknown", c["metadata"]["name"],
                     "oc get csv -n %s; check the install plan" % self.op_ns, "critical", "aap_cr")
        try:
            subs = o.k8s_list("/apis/operators.coreos.com/v1alpha1/namespaces/%s/subscriptions" % self.op_ns)
        except ApiError:
            subs = []
        for s in subs:
            spec, stt = s.get("spec") or {}, s.get("status") or {}
            if spec.get("name") != "ansible-automation-platform-operator":
                continue
            self.info["operator_channel"] = spec.get("channel")
            pending = stt.get("currentCSV") and stt.get("installedCSV") and stt["currentCSV"] != stt["installedCSV"]
            self.add("platform.operator_upgrade", "platform", "Operator update pending approval",
                     "warn" if pending else "ok", stt.get("currentCSV") if pending else "up to date",
                     "installed %s, channel %s, approval %s" % (stt.get("installedCSV"), spec.get("channel"),
                                                              spec.get("installPlanApproval")),
                     "Review and approve the InstallPlan in a change window", "minor", "aap_lifecycle")

    def ocp_workloads(self):
        o, ns, t = self.ocp, self.ns, self.t
        pods = o.k8s_list("/api/v1/namespaces/%s/pods" % ns)
        prev_restarts = (self.prev.get("details") or {}).get("pod_restarts") or {}
        rows, restarts_now, not_ready, crash, oom = [], {}, [], [], []
        by_comp = Counter()
        spread = defaultdict(set)
        spread_n = Counter()
        no_task_limits = []
        restart_delta_total = 0
        for p in pods:
            name = p["metadata"]["name"]
            phase = (p.get("status") or {}).get("phase")
            if name.startswith("automation-job-") or phase == "Succeeded":
                continue
            comp = classify_pod(name)
            by_comp[comp] += 1
            cs = (p.get("status") or {}).get("containerStatuses") or []
            rcount = sum(int(c.get("restartCount") or 0) for c in cs)
            restarts_now[name] = rcount
            delta = max(0, rcount - int(prev_restarts.get(name, rcount if prev_restarts else 0)))
            if not prev_restarts:
                delta = 0
            restart_delta_total += delta
            ready = phase == "Running" and cs and all(c.get("ready") for c in cs)
            if not ready:
                not_ready.append("%s (%s)" % (name, phase))
            for c in cs:
                w = (c.get("state") or {}).get("waiting") or {}
                if w.get("reason") in ("CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull",
                                       "CreateContainerConfigError", "CreateContainerError"):
                    crash.append("%s/%s: %s" % (name, c.get("name"), w.get("reason")))
                lt = (c.get("lastState") or {}).get("terminated") or {}
                fin = parse_dt(lt.get("finishedAt"))
                if lt.get("reason") == "OOMKilled" and (fin is None or fin >= self.since24):
                    oom.append("%s/%s" % (name, c.get("name")))
            node = (p.get("spec") or {}).get("nodeName")
            wl = workload_of(p)
            if node:
                spread[wl].add(node)
            spread_n[wl] += 1
            if comp == "controller" and "task" in name:
                for c in (p.get("spec") or {}).get("containers") or []:
                    if "task" in c.get("name", "") and not ((c.get("resources") or {}).get("limits") or {}):
                        no_task_limits.append("%s/%s" % (name, c.get("name")))
            rows.append({"pod": name, "component": comp, "phase": phase, "ready": bool(ready),
                         "restarts": rcount, "restarts_delta": delta, "node": node})
        self.details["pods"] = sorted(rows, key=lambda r: (r["component"], r["pod"]))
        self.details["pod_restarts"] = restarts_now
        self.m("ocp.pods", len(rows))
        self.m("ocp.pods_not_ready", len(not_ready))
        self.m("ocp.pod_restarts_24h", restart_delta_total)
        self.m("ocp.oomkilled_24h", len(oom))
        self.add("platform.pods_ready", "platform", "AAP pods Running and Ready",
                 "fail" if not_ready else "ok", "%d/%d ready" % (len(rows) - len(not_ready), len(rows)),
                 "; ".join(not_ready[:10]) or "all ready", "oc describe pod <name> -n %s" % ns, "critical",
                 "aap_cr")
        self.add("platform.crashloop", "platform", "No CrashLoopBackOff / image pull errors",
                 "fail" if crash else "ok", "%d" % len(crash), "; ".join(crash[:10]) or "none",
                 "oc logs --previous <pod>; check image pull secrets", "critical", "aap_cr")
        self.add("platform.restarts", "platform", "Pod restarts since last run",
                 grade_high(restart_delta_total, t["pod_restarts_warn"], t["pod_restarts_fail"]),
                 "%d" % restart_delta_total, ", ".join("%s +%d" % (r["pod"], r["restarts_delta"])
                                                       for r in rows if r["restarts_delta"])[:500] or
                 ("baseline recorded" if not prev_restarts else "no new restarts"),
                 "Restarts are counted against the previous daily snapshot", "major", "aap_perf_ocp")
        self.add("platform.oomkilled", "platform", "No OOMKilled containers (24h)",
                 "warn" if oom else "ok", "%d" % len(oom), ", ".join(oom[:10]) or "none",
                 "Raise the memory limit in the CR (*_resource_requirements)", "major", "aap_perf_ocp")
        crowded = ["%s (%d pods on 1 node)" % (wl, spread_n[wl]) for wl, nodes in spread.items()
                   if spread_n[wl] > 1 and len(nodes) == 1]
        self.add("platform.pod_spread", "platform", "Replicas spread across worker nodes",
                 "warn" if crowded else "ok", "%d" % len(crowded), "; ".join(crowded[:10]) or
                 "multi-replica workloads span more than one node",
                 "Red Hat recommends topology_spread_constraints so one node loss does not take out a component",
                 "minor", "aap_perf_ocp")
        self.add("platform.task_limits", "platform", "Controller task containers have resource limits",
                 "warn" if no_task_limits else "ok", "%d without limits" % len(no_task_limits),
                 ", ".join(no_task_limits[:6]) or "limits set",
                 "Control capacity is derived from the task container limits; set task_resource_requirements",
                 "minor", "aap_perf_ocp")
        # Deployments / StatefulSets ready vs desired
        bad = []
        for kind, path in (("deploy", "/apis/apps/v1/namespaces/%s/deployments" % ns),
                           ("sts", "/apis/apps/v1/namespaces/%s/statefulsets" % ns)):
            for d in o.k8s_list(path):
                want = (d.get("spec") or {}).get("replicas", 1)
                have = (d.get("status") or {}).get("readyReplicas") or 0
                if want and have < want:
                    bad.append("%s/%s %d/%d" % (kind, d["metadata"]["name"], have, want))
        self.m("ocp.workloads_degraded", len(bad))
        self.add("platform.workloads", "platform", "Deployments and StatefulSets at desired replicas",
                 "fail" if bad else "ok", "%d degraded" % len(bad), "; ".join(bad[:10]) or "all at desired",
                 "oc get deploy,sts -n %s" % ns, "critical", "aap_cr")
        # PodDisruptionBudgets that block node drains (OpenShift upgrades)
        try:
            pdbs = o.k8s_list("/apis/policy/v1/namespaces/%s/poddisruptionbudgets" % ns)
            blocking = [p["metadata"]["name"] for p in pdbs
                        if (p.get("status") or {}).get("expectedPods") and
                        (p.get("status") or {}).get("disruptionsAllowed") == 0]
            self.add("platform.pdb", "platform", "PodDisruptionBudgets allow node drains",
                     "warn" if blocking else "ok", "%d blocking" % len(blocking), ", ".join(blocking) or "none",
                     "A PDB with 0 allowed disruptions stalls MachineConfigPool rollouts during upgrades",
                     "minor", "ocp_upgrade_checks")
        except ApiError:
            pass

    def ocp_storage(self):
        o, ns, t = self.ocp, self.ns, self.t
        pvcs = o.k8s_list("/api/v1/namespaces/%s/persistentvolumeclaims" % ns)
        unbound, rows, hub_rwo = [], [], []
        usage = self._prom_vector('kubelet_volume_stats_used_bytes{namespace="%s"} / '
                                  'kubelet_volume_stats_capacity_bytes{namespace="%s"}' % (ns, ns),
                                  "persistentvolumeclaim") if self.prom else {}
        worst_pct = None
        for p in pvcs:
            name = p["metadata"]["name"]
            phase = (p.get("status") or {}).get("phase")
            cap = mem_bytes(((p.get("status") or {}).get("capacity") or {}).get("storage"))
            modes = (p.get("spec") or {}).get("accessModes") or []
            if phase != "Bound":
                unbound.append("%s (%s)" % (name, phase))
            if re.search(r"hub", name) and re.search(r"file|storage", name) and "ReadWriteMany" not in modes:
                hub_rwo.append(name)
            used_pct = round(usage[name] * 100, 1) if name in usage else None
            if used_pct is not None:
                worst_pct = used_pct if worst_pct is None else max(worst_pct, used_pct)
            rows.append({"pvc": name, "phase": phase, "capacity_gib": gib(cap), "modes": ",".join(modes),
                         "storage_class": (p.get("spec") or {}).get("storageClassName"), "used_pct": used_pct})
            if "postgres" in name:
                self.info["db_pvc_gib"] = gib(cap)
                if used_pct is not None:
                    self.m("ocp.db_pvc_used_pct", used_pct)
        self.details["pvcs"] = rows
        self.m("ocp.pvc_max_used_pct", worst_pct)
        self.add("platform.pvc_bound", "platform", "PersistentVolumeClaims Bound", "fail" if unbound else "ok",
                 "%d/%d bound" % (len(pvcs) - len(unbound), len(pvcs)), "; ".join(unbound) or "all bound",
                 "oc describe pvc -n %s; check the storage class / provisioner" % ns, "critical", "aap_perf_ocp")
        self.add("platform.pvc_usage", "platform", "PVC free space (database, hub, redis)",
                 grade_high(worst_pct, t["pvc_warn_pct"], t["pvc_fail_pct"]),
                 "%s%% max used" % worst_pct if worst_pct is not None else "n/a",
                 "" if self.prom else "Set prometheus_url to read kubelet volume stats",
                 "Expand the PVC (storage class must allow expansion); run cleanup management jobs",
                 "major", "aap_perf_ocp")
        if hub_rwo:
            self.add("platform.hub_rwx", "platform", "Hub file storage is ReadWriteMany", "warn",
                     ", ".join(hub_rwo), "Automation hub needs RWX storage (or S3) to run more than one pod",
                     "Use an RWX storage class or object storage for hub", "minor", "aap_topology")

    def ocp_usage(self):
        o, ns, t = self.ocp, self.ns, self.t
        pods = {p["metadata"]["name"]: p for p in o.k8s_list("/api/v1/namespaces/%s/pods" % ns)}
        usage = o.k8s_list("/apis/metrics.k8s.io/v1beta1/namespaces/%s/pods" % ns)
        tot_cpu = tot_mem = 0.0
        hot, rows = [], []
        for u in usage:
            name = u["metadata"]["name"]
            if name.startswith("automation-job-"):
                continue
            spec = pods.get(name, {}).get("spec") or {}
            limits = {c["name"]: (c.get("resources") or {}).get("limits") or {} for c in spec.get("containers") or []}
            for c in u.get("containers") or []:
                cpu = cpu_cores((c.get("usage") or {}).get("cpu")) or 0.0
                mem = mem_bytes((c.get("usage") or {}).get("memory")) or 0.0
                tot_cpu += cpu
                tot_mem += mem
                lim = mem_bytes((limits.get(c["name"]) or {}).get("memory"))
                mpct = pct(mem, lim) if lim else None
                if mpct is not None and mpct >= t["mem_limit_warn_pct"]:
                    hot.append(("%s/%s %s%%" % (name, c["name"], mpct), mpct))
                rows.append({"pod": name, "container": c["name"], "cpu_m": round(cpu * 1000),
                             "mem_mib": round(mem / 1048576.0), "mem_limit_pct": mpct})
        self.details["usage"] = sorted(rows, key=lambda r: -r["mem_mib"])[:40]
        self.m("ocp.cpu_cores", round(tot_cpu, 2))
        self.m("ocp.mem_gib", gib(tot_mem))
        top = max([p for _, p in hot], default=None)
        self.add("platform.mem_limits", "platform", "Containers below memory limit",
                 grade_high(top, t["mem_limit_warn_pct"], t["mem_limit_fail_pct"]) if hot else "ok",
                 "%d near limit" % len(hot), ", ".join(h for h, _ in sorted(hot, key=lambda x: -x[1])[:8]) or
                 "all under %s%%" % t["mem_limit_warn_pct"],
                 "Red Hat: capacity does not introspect host resources - watch CPU/memory and raise limits",
                 "major", "aap_monitor")
        if self.prom:
            thr = self._prom_vector(
                'sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{namespace="%s",container!=""}[1h])) / '
                'sum by (pod) (rate(container_cpu_cfs_periods_total{namespace="%s",container!=""}[1h]))' % (ns, ns),
                "pod")
            thr = {k: v * 100 for k, v in thr.items() if not k.startswith("automation-job-")}
            worst_ = max(thr.values(), default=None)
            self.m("ocp.cpu_throttle_max_pct", round(worst_, 1) if worst_ is not None else None)
            bad = sorted(((k, v) for k, v in thr.items() if v >= t["cpu_throttle_warn_pct"]), key=lambda x: -x[1])
            self.add("platform.cpu_throttling", "platform", "AAP pods not CPU throttled (1h)",
                     grade_high(worst_, t["cpu_throttle_warn_pct"], t["cpu_throttle_fail_pct"]),
                     "%.0f%% max" % worst_ if worst_ is not None else "n/a",
                     ", ".join("%s %.0f%%" % kv for kv in bad[:6]) or "none above %s%%" % t["cpu_throttle_warn_pct"],
                     "Raise CPU requests/limits (Red Hat observed PostgreSQL throttling below 500m requests)",
                     "minor", "aap_perf_ocp")

    def ocp_events_routes(self):
        o, ns, t = self.ocp, self.ns, self.t
        events = o.k8s_list("/api/v1/namespaces/%s/events" % ns, {"fieldSelector": "type=Warning"})
        recent = []
        for ev in events:
            ts = parse_dt(ev.get("lastTimestamp") or ev.get("eventTime") or
                          (ev.get("metadata") or {}).get("creationTimestamp"))
            if ts and ts >= self.since24:
                recent.append(ev)
        reasons = Counter(ev.get("reason") for ev in recent)
        self.m("ocp.warning_events_24h", len(recent))
        self.details["event_reasons"] = reasons.most_common(10)
        self.add("platform.events", "platform", "Warning events in namespace (24h)",
                 "warn" if len(recent) >= t["warning_events_warn"] else "ok", "%d" % len(recent),
                 ", ".join("%s x%d" % kv for kv in reasons.most_common(6)) or "none",
                 "oc get events -n %s --field-selector type=Warning" % ns, "minor", "aap_cr")
        routes = o.k8s_list("/apis/route.openshift.io/v1/namespaces/%s/routes" % ns)
        not_admitted, certs = [], []
        for r in routes:
            host = (r.get("spec") or {}).get("host")
            ingress = ((r.get("status") or {}).get("ingress") or [{}])[0]
            adm = next((c for c in ingress.get("conditions") or [] if c.get("type") == "Admitted"), None)
            if not adm or str(adm.get("status")).lower() != "true":
                not_admitted.append(r["metadata"]["name"])
            if host and (r.get("spec") or {}).get("tls") and self.env.get("check_route_certs",
                                                                         self.s["check_route_certs"]):
                try:
                    na, issuer, _ = tls_expiry(host, int(self.env.get("route_tls_port", 443)))
                    certs.append({"route": r["metadata"]["name"], "host": host,
                                  "days_left": round((na - self.now).total_seconds() / 86400.0, 1),
                                  "not_after": iso(na), "issuer": issuer})
                except Exception as exc:  # noqa: BLE001
                    certs.append({"route": r["metadata"]["name"], "host": host, "days_left": None,
                                  "error": short(exc, 120)})
        self.add("platform.routes", "platform", "Routes admitted by the ingress controller",
                 "fail" if not_admitted else "ok", "%d/%d" % (len(routes) - len(not_admitted), len(routes)),
                 ", ".join(not_admitted) or "all admitted", "oc describe route -n %s" % ns, "major", "aap_cr")
        self.details["certs"] = certs
        days = [c["days_left"] for c in certs if c.get("days_left") is not None]
        mn = min(days) if days else None
        self.m("ocp.cert_min_days", mn)
        if certs:
            self.add("platform.route_tls", "platform", "Route TLS certificates not expiring",
                     grade_low(mn, t["cert_warn_days"], t["cert_fail_days"]),
                     "%s days" % mn if mn is not None else "n/a",
                     ", ".join("%s %sd" % (c["route"], c.get("days_left")) for c in certs[:6]),
                     "python/28_aap_ssl_certificate_scanner.py; rotate the route / ingress certificate",
                     "critical", "aap_perf_ocp")
        quotas = o.k8s_list("/api/v1/namespaces/%s/resourcequotas" % ns)
        tight = []
        top_q = None
        for q in quotas:
            hard = (q.get("status") or {}).get("hard") or {}
            used = (q.get("status") or {}).get("used") or {}
            for k, hv in hard.items():
                conv = cpu_cores if "cpu" in k else mem_bytes
                h_, u_ = conv(hv), conv(used.get(k, "0"))
                if h_:
                    p_ = pct(u_ or 0, h_)
                    top_q = p_ if top_q is None else max(top_q, p_)
                    if p_ >= t["quota_warn_pct"]:
                        tight.append("%s %s %s%%" % (q["metadata"]["name"], k, p_))
        if quotas:
            self.add("platform.quota", "platform", "ResourceQuota headroom",
                     grade_high(top_q, t["quota_warn_pct"], t["quota_fail_pct"]),
                     "%s%% max" % top_q, ", ".join(tight[:6]) or "headroom available",
                     "Job pods fail to schedule when the quota is exhausted", "major", "aap_perf_ocp")

    def ocp_backups(self):
        o, ns, t = self.ocp, self.ns, self.t
        items = o.k8s_list("/apis/aap.ansible.com/v1alpha1/namespaces/%s/ansibleautomationplatformbackups" % ns)
        good = []
        for b in items:
            if cond_true(b, "Successful"):
                c = cond(b, "Successful")
                ts = parse_dt(c.get("lastTransitionTime")) or parse_dt(b["metadata"].get("creationTimestamp"))
                if ts:
                    good.append((ts, b["metadata"]["name"]))
        if not good:
            self.m("ocp.backup_age_h", None)
            self.add("platform.backup", "platform", "Recent successful platform backup", "fail", "none",
                     "%d backup CRs, none Successful" % len(items),
                     "Schedule an AnsibleAutomationPlatformBackup (CronJob or AAP job); see docs/RUNBOOK.md",
                     "critical", "aap_backup")
            return
        ts, name = max(good)
        age = round((self.now - ts).total_seconds() / 3600.0, 1)
        self.m("ocp.backup_age_h", age)
        self.add("platform.backup", "platform", "Recent successful platform backup",
                 grade_high(age, t["backup_warn_h"], t["backup_fail_h"]), "%sh ago" % age, name,
                 "Backups older than a day mean the backup schedule is not running", "critical", "aap_backup")

    def ocp_database_limits(self):
        """Red Hat: do not use the operator-deployed PostgreSQL past these limits."""
        t = self.t
        spec = self.info.get("aap_cr_spec")
        if spec is None:
            return
        db = spec.get("database") or {}
        external = bool(db.get("database_secret") or spec.get("database_secret"))
        self.info["database"] = "external" if external else "operator-managed"
        if external:
            self.add("platform.operator_db_limits", "platform", "Operator-managed database within supported limits",
                     "ok", "external database", "Customer-provided PostgreSQL", "", "minor", "aap_topology")
            return
        reasons = []
        for comp in ("api", "controller", "hub", "eda"):
            reps = (spec.get(comp) or {}).get("replicas")
            if isinstance(reps, int) and reps > 1:
                reasons.append("%s replicas=%d" % (comp, reps))
        size = self.info.get("db_pvc_gib")
        if size and size > t["operator_db_max_gb"]:
            reasons.append("database PVC %s GiB > %s" % (size, t["operator_db_max_gb"]))
        self.add("platform.operator_db_limits", "platform", "Operator-managed database within supported limits",
                 "warn" if reasons else "ok", "operator-managed", "; ".join(reasons) or
                 "1 replica per component / 100 GB database not exceeded. Connection and concurrent-job "
                 "limits are checked by the AAP dashboard (controller.operator_db_load).",
                 "Red Hat: move to an external PostgreSQL 15-17 when any limit is exceeded", "major", "aap_topology")

    # ======================================================================= #
    # OpenShift - cluster (needs cluster-reader; optional)
    # ======================================================================= #
    def cluster_checks(self):
        o, t = self.ocp, self.t

        def clusterversion():
            cv = o.json("/apis/config.openshift.io/v1/clusterversions/version")
            st = cv.get("status") or {}
            ver = (st.get("desired") or {}).get("version")
            self.info["ocp_version"] = ver
            self.info["ocp_channel"] = (cv.get("spec") or {}).get("channel")
            failing = cond_true(cv, "Failing")
            available = cond_true(cv, "Available")
            progressing = cond_true(cv, "Progressing")
            upgradeable = cond_true(cv, "Upgradeable")
            updates = len(st.get("availableUpdates") or [])
            self.m("ocp.updates_available", updates)
            s_ = "fail" if failing or available is False else ("warn" if progressing else "ok")
            self.add("cluster.version", "cluster", "ClusterVersion Available, not Failing", s_, ver,
                     "Progressing=%s, %d updates available, channel %s" % (progressing, updates,
                                                                          self.info["ocp_channel"]),
                     "oc adm upgrade; oc describe clusterversion", "critical", "ocp_validate")
            if upgradeable is False:
                c = cond(cv, "Upgradeable") or {}
                self.add("cluster.upgradeable", "cluster", "Cluster Upgradeable", "warn", "False",
                         short(c.get("message"), 300), "Resolve Upgradeable=False before the next minor update",
                         "minor", "ocp_upgrade_checks")
        self.optional("cluster", "cluster.version", "ClusterVersion Available, not Failing", clusterversion,
                      "ocp_validate", "critical")

        def operators():
            cos = o.k8s_list("/apis/config.openshift.io/v1/clusteroperators")
            down, degraded, prog = [], [], []
            for co in cos:
                n = co["metadata"]["name"]
                if cond_true(co, "Available") is False:
                    down.append(n)
                if cond_true(co, "Degraded"):
                    degraded.append(n)
                if cond_true(co, "Progressing"):
                    prog.append(n)
            self.m("ocp.co_degraded", len(set(down + degraded)))
            etcd = next((co for co in cos if co["metadata"]["name"] == "etcd"), None)
            self.add("cluster.operators", "cluster", "ClusterOperators Available and not Degraded",
                     "fail" if (down or degraded) else ("warn" if prog else "ok"),
                     "%d/%d healthy" % (len(cos) - len(set(down + degraded)), len(cos)),
                     "; ".join(filter(None, ["unavailable: " + ", ".join(down) if down else "",
                                             "degraded: " + ", ".join(degraded) if degraded else "",
                                             "progressing: " + ", ".join(prog) if prog else ""])) or "all healthy",
                     "oc get co; oc describe co <name>", "critical", "ocp_upgrade_checks")
            if etcd:
                s_ = "fail" if cond_true(etcd, "Degraded") or cond_true(etcd, "Available") is False else "ok"
                self.add("cluster.etcd_operator", "cluster", "etcd operator healthy", s_,
                         "Degraded=%s" % cond_true(etcd, "Degraded"),
                         short((cond(etcd, "Degraded") or {}).get("message"), 200),
                         "oc get pods -n openshift-etcd; check member health", "critical", "ocp_etcd")
        self.optional("cluster", "cluster.operators", "ClusterOperators Available and not Degraded", operators,
                      "ocp_upgrade_checks", "critical")

        def nodes():
            items = o.k8s_list("/api/v1/nodes")
            aap_nodes = {r.get("node") for r in self.details.get("pods") or [] if r.get("node")}
            notready, pressure, cordoned, rows = [], [], [], []
            for n in items:
                nm = n["metadata"]["name"]
                roles = [k.split("/", 1)[1] for k in (n["metadata"].get("labels") or {})
                         if k.startswith("node-role.kubernetes.io/")]
                ready = cond_true(n, "Ready")
                pres = [ct for ct in ("MemoryPressure", "DiskPressure", "PIDPressure") if cond_true(n, ct)]
                if ready is not True:
                    notready.append(nm + (" (hosts AAP)" if nm in aap_nodes else ""))
                if pres:
                    pressure.append("%s: %s" % (nm, ",".join(pres)))
                if (n.get("spec") or {}).get("unschedulable"):
                    cordoned.append(nm)
                alloc = (n.get("status") or {}).get("allocatable") or {}
                rows.append({"node": nm, "roles": ",".join(sorted(roles)), "ready": ready, "pressure": ",".join(pres),
                             "unschedulable": bool((n.get("spec") or {}).get("unschedulable")),
                             "aap": nm in aap_nodes, "cpu": cpu_cores(alloc.get("cpu")),
                             "mem_gib": gib(mem_bytes(alloc.get("memory"))),
                             "kubelet": ((n.get("status") or {}).get("nodeInfo") or {}).get("kubeletVersion")})
            self.details["nodes"] = rows
            self.m("ocp.nodes", len(items))
            self.m("ocp.nodes_not_ready", len(notready))
            self.add("cluster.nodes_ready", "cluster", "Nodes Ready", "fail" if notready else "ok",
                     "%d/%d ready" % (len(items) - len(notready), len(items)), ", ".join(notready) or "all Ready",
                     "oc describe node <name>; oc adm top nodes", "critical", "ocp_nodes")
            self.add("cluster.node_pressure", "cluster", "No Memory/Disk/PID pressure on nodes",
                     "fail" if pressure else "ok",
                     "%d" % len(pressure), "; ".join(pressure) or "none",
                     "Pressure triggers pod eviction - free disk / memory on the node", "major", "ocp_nodes")
            self.add("cluster.nodes_schedulable", "cluster", "No nodes left cordoned",
                     "warn" if cordoned else "ok", "%d" % len(cordoned), ", ".join(cordoned) or "none",
                     "oc adm uncordon <node> after maintenance", "minor", "ocp_upgrade_checks")
        self.optional("cluster", "cluster.nodes_ready", "Nodes Ready", nodes, "ocp_nodes", "critical")

        def node_usage():
            usage = o.k8s_list("/apis/metrics.k8s.io/v1beta1/nodes")
            alloc = {r["node"]: r for r in self.details.get("nodes") or []}
            cpu_hi = mem_hi = None
            hot = []
            for u in usage:
                n = alloc.get(u["metadata"]["name"])
                if not n or not n.get("cpu") or not n.get("mem_gib"):
                    continue
                c = pct(cpu_cores(u["usage"].get("cpu")) or 0, n["cpu"])
                mm = pct(gib(mem_bytes(u["usage"].get("memory")) or 0), n["mem_gib"])
                n["cpu_pct"], n["mem_pct"] = c, mm
                cpu_hi = c if cpu_hi is None else max(cpu_hi, c)
                mem_hi = mm if mem_hi is None else max(mem_hi, mm)
                if c >= t["node_cpu_warn_pct"] or mm >= t["node_mem_warn_pct"]:
                    hot.append("%s cpu %s%% mem %s%%" % (n["node"], c, mm))
            self.m("ocp.node_cpu_max_pct", cpu_hi)
            self.m("ocp.node_mem_max_pct", mem_hi)
            self.add("cluster.node_headroom", "cluster", "Node CPU and memory headroom",
                     worst([grade_high(cpu_hi, t["node_cpu_warn_pct"], t["node_cpu_fail_pct"]),
                            grade_high(mem_hi, t["node_mem_warn_pct"], t["node_mem_fail_pct"])]),
                     "cpu %s%% / mem %s%% max" % (cpu_hi, mem_hi), "; ".join(hot[:6]) or "headroom available",
                     "Nodes need spare capacity for pod rescheduling during upgrades", "major", "ocp_validate")
        self.optional("cluster", "cluster.node_headroom", "Node CPU and memory headroom", node_usage,
                      "ocp_validate", "major")

        def mcps():
            pools = o.k8s_list("/apis/machineconfiguration.openshift.io/v1/machineconfigpools")
            bad, upd, paused = [], [], []
            for p in pools:
                n = p["metadata"]["name"]
                st = p.get("status") or {}
                if cond_true(p, "Degraded") or int(st.get("degradedMachineCount") or 0) > 0:
                    bad.append(n)
                elif cond_true(p, "Updating") or st.get("readyMachineCount") != st.get("machineCount"):
                    upd.append("%s %s/%s" % (n, st.get("readyMachineCount"), st.get("machineCount")))
                if (p.get("spec") or {}).get("paused"):
                    paused.append(n)
            self.add("cluster.mcp", "cluster", "MachineConfigPools updated, not Degraded",
                     "fail" if bad else ("warn" if upd or paused else "ok"), "%d pools" % len(pools),
                     "; ".join(filter(None, ["degraded: " + ", ".join(bad) if bad else "",
                                             "updating: " + ", ".join(upd) if upd else "",
                                             "paused: " + ", ".join(paused) if paused else ""])) or "all updated",
                     "oc get mcp; oc describe mcp <pool>", "major", "ocp_upgrade_checks")
        self.optional("cluster", "cluster.mcp", "MachineConfigPools updated, not Degraded", mcps,
                      "ocp_upgrade_checks", "major")

        def csrs():
            items = o.k8s_list("/apis/certificates.k8s.io/v1/certificatesigningrequests")
            pend = [c["metadata"]["name"] for c in items if not ((c.get("status") or {}).get("conditions"))]
            self.m("ocp.csr_pending", len(pend))
            self.add("cluster.csr", "cluster", "No pending certificate signing requests",
                     "warn" if pend else "ok", "%d pending" % len(pend), ", ".join(pend[:8]) or "none",
                     "oc get csr; approve legitimate node CSRs (oc adm certificate approve)", "minor",
                     "ocp_upgrade_checks")
        self.optional("cluster", "cluster.csr", "No pending certificate signing requests", csrs,
                      "ocp_upgrade_checks", "minor")

        def etcd_pods():
            pods = o.k8s_list("/api/v1/namespaces/openshift-etcd/pods", {"labelSelector": "app=etcd"})
            notready = [p["metadata"]["name"] for p in pods
                        if not all(c.get("ready") for c in (p.get("status") or {}).get("containerStatuses") or [{}])]
            self.add("cluster.etcd_members", "cluster", "etcd members running",
                     "fail" if notready or not pods else "ok", "%d/%d ready" % (len(pods) - len(notready), len(pods)),
                     ", ".join(notready) or "all members ready", "oc rsh -n openshift-etcd <pod> etcdctl endpoint health",
                     "critical", "ocp_etcd")
        self.optional("cluster", "cluster.etcd_members", "etcd members running", etcd_pods, "ocp_etcd", "critical")

    # ---- Prometheus / Thanos querier (needs cluster-monitoring-view) -------- #
    def _prom_vector(self, query, label):
        """Instant query -> {label value: float}. Returns {} on any error."""
        try:
            data = self.prom.json("/api/v1/query", {"query": query})
        except ApiError:
            return {}
        out = {}
        for r in ((data.get("data") or {}).get("result") or []):
            try:
                out[(r.get("metric") or {}).get(label, "value")] = float(r["value"][1])
            except (KeyError, ValueError, IndexError, TypeError):
                pass
        return out

    def _prom_scalar(self, query):
        data = self.prom.json("/api/v1/query", {"query": query})
        vals = []
        for r in ((data.get("data") or {}).get("result") or []):
            try:
                v = float(r["value"][1])
                if not math.isnan(v):
                    vals.append(v)
            except (KeyError, ValueError, IndexError, TypeError):
                pass
        return max(vals) if vals else None

    def prometheus_checks(self):
        t, ns = self.t, self.ns

        def alerts():
            data = self.prom.json("/api/v1/query", {"query": 'ALERTS{alertstate="firing"}'})
            excluded = set(self.s.get("excluded_alerts") or [])
            crit, warn_, mine = [], [], []
            for r in ((data.get("data") or {}).get("result") or []):
                lab = r.get("metric") or {}
                name = lab.get("alertname")
                if name in excluded:
                    continue
                sev = lab.get("severity", "")
                tag = name + ("@" + lab["namespace"] if lab.get("namespace") else "")
                if lab.get("namespace") == ns:
                    mine.append(tag)
                if sev == "critical":
                    crit.append(tag)
                elif sev == "warning":
                    warn_.append(tag)
            self.m("ocp.alerts_critical", len(crit))
            self.m("ocp.alerts_warning", len(warn_))
            self.m("ocp.alerts_aap_ns", len(mine))
            self.details["alerts"] = {"critical": sorted(set(crit)), "warning": sorted(set(warn_)),
                                      "aap_namespace": sorted(set(mine))}
            self.add("cluster.alerts", "cluster", "No critical alerts firing",
                     "fail" if crit else ("warn" if warn_ else "ok"),
                     "%d critical, %d warning" % (len(crit), len(warn_)),
                     ", ".join(sorted(set(crit))[:6] + sorted(set(warn_))[:6]) or "none",
                     "Observe > Alerting in the console; follow the alert runbook", "major", "ocp_upgrade_checks")
            self.add("platform.alerts", "platform", "No alerts firing in the AAP namespace",
                     "warn" if mine else "ok", "%d" % len(mine), ", ".join(sorted(set(mine))[:8]) or "none",
                     "Observe > Alerting filtered to namespace %s" % ns, "major", "ocp_upgrade_checks")
        self.optional("cluster", "cluster.alerts", "No critical alerts firing", alerts, "ocp_upgrade_checks", "major")

        def etcd_latency():
            q = 'histogram_quantile(0.99, sum by (instance, le) (rate(%s{job="etcd"}[5m])))'
            fsync = self._prom_scalar(q % "etcd_disk_wal_fsync_duration_seconds_bucket")
            commit = self._prom_scalar(q % "etcd_disk_backend_commit_duration_seconds_bucket")
            rtt = self._prom_scalar('histogram_quantile(0.99, sum by (instance, le) '
                                    '(rate(etcd_network_peer_round_trip_time_seconds_bucket[5m])))')
            leaders = self._prom_scalar('max(increase(etcd_server_leader_changes_seen_total[1d]))')
            ms = lambda v: round(v * 1000, 2) if v is not None else None  # noqa: E731
            fsync, commit, rtt = ms(fsync), ms(commit), ms(rtt)
            self.m("ocp.etcd_fsync_p99_ms", fsync)
            self.m("ocp.etcd_commit_p99_ms", commit)
            self.m("ocp.etcd_peer_rtt_p99_ms", rtt)
            self.m("ocp.etcd_leader_changes_24h", round(leaders) if leaders is not None else None)
            self.add("cluster.etcd_fsync", "cluster", "etcd WAL fsync p99 < 10 ms",
                     grade_high(fsync, t["etcd_fsync_warn_ms"], t["etcd_fsync_fail_ms"]),
                     "%s ms" % fsync if fsync is not None else "n/a",
                     "Red Hat: p99 of etcd_disk_wal_fsync_duration_seconds should be below 10 ms",
                     "Move etcd to faster (NVMe/SSD) dedicated disks", "major", "ocp_etcd")
            self.add("cluster.etcd_commit", "cluster", "etcd backend commit p99",
                     grade_high(commit, t["etcd_commit_warn_ms"], t["etcd_commit_fail_ms"]),
                     "%s ms" % commit if commit is not None else "n/a",
                     "etcd_disk_backend_commit_duration_seconds p99", "Check disk bandwidth on control plane nodes",
                     "minor", "ocp_etcd_metrics")
            self.add("cluster.etcd_rtt", "cluster", "etcd peer round trip p99 < 50 ms",
                     grade_high(rtt, t["etcd_peer_rtt_warn_ms"], t["etcd_peer_rtt_fail_ms"]),
                     "%s ms" % rtt if rtt is not None else "n/a",
                     "Red Hat: etcd_network_peer_round_trip_time_seconds p99 should be below 50 ms",
                     "Check control plane network latency", "minor", "ocp_etcd")
            self.add("cluster.etcd_leader", "cluster", "etcd leader stable (24h)",
                     grade_high(leaders, t["etcd_leader_changes_warn"], t["etcd_leader_changes_fail"]),
                     "%s changes" % (round(leaders) if leaders is not None else "n/a"),
                     "etcd_server_leader_changes_seen_total; frequent elections point at disk or network latency",
                     "Investigate etcd disk and network latency", "major", "ocp_etcd")
        self.optional("cluster", "cluster.etcd_fsync", "etcd WAL fsync p99 < 10 ms", etcd_latency, "ocp_etcd")


# --------------------------------------------------------------------------- #
# Dashboard definition
# --------------------------------------------------------------------------- #
class OcpDashboard(Dashboard):
    key = "openshift"
    label = "OpenShift"
    short = "OpenShift"
    script = "ocp_health_dashboard.py"
    blurb = "AAP namespace, operator, storage, backups, routes, nodes, etcd and alerts (OpenShift API only)"
    components = ("platform", "cluster")
    requires = ("ocp_api_url",)
    collector = OcpCollector
    extra_settings = ("cluster_checks", "check_route_certs")
    env_chips = (("OpenShift", "ocp_version"), ("Channel", "ocp_channel"), ("AAP operator", "operator_version"),
                 ("Database", "database"))

    def add_arguments(self, ap):
        ap.add_argument("--no-cluster", action="store_true",
                        help="skip cluster-scope checks (nodes, ClusterOperators, etcd) for namespace-only tokens")

    def apply_arguments(self, args, cfg):
        if args.no_cluster:
            cfg["settings"]["cluster_checks"] = False

    def headline(self, col, m, settings):
        return [("Critical alerts", num(m.get("ocp.alerts_critical"))),
                ("AAP pods not ready", num(m.get("ocp.pods_not_ready"))),
                ("Pod restarts", num(m.get("ocp.pod_restarts_24h"))),
                ("Last backup", num(m.get("ocp.backup_age_h"), " h")),
                ("Min cert days", num(m.get("ocp.cert_min_days"))),
                ("OpenShift", col.info.get("ocp_version") or "n/a")]

    def hero_cards(self, snaps, cfg):
        t = cfg["kpi_targets"]
        S = lambda k: sum(s["metrics"].get(k) or 0 for s in snaps)  # noqa: E731
        scores = [s["score"] for s in snaps if s["score"] is not None]
        avg = round(statistics.mean(scores), 1) if scores else None
        alerts = S("ocp.alerts_critical")
        backups = [s["metrics"].get("ocp.backup_age_h") for s in snaps if s["metrics"].get("ocp.backup_age_h") is not None]
        bk = max(backups) if backups else None
        crit = S("open_critical")
        one = len(snaps) == 1
        return [status_card(snaps, "OpenShift status"),
                card(num(avg), "OpenShift health score" if one else "Average OpenShift health score",
                     vs_target(avg, t["health_score"]), "target %s" % t["health_score"]),
                card(num(alerts), "Critical alerts firing", "bad" if alerts else "good", "target 0"),
                card(num(S("ocp.pods_not_ready")), "AAP pods not ready", "warn" if S("ocp.pods_not_ready") else "good"),
                card(num(bk, " h"), "Last good AAP backup", vs_target(bk, t["backup_age_h"], False),
                     "target <= %s h" % t["backup_age_h"]),
                card(num(crit), "Open critical OpenShift issues", "bad" if crit else "good", "target 0")]

    scorecard_headers = ("Pods not ready", "Restarts", "Critical alerts", "Max PVC", "OpenShift", "AAP operator")
    scorecard_nums = (0, 1, 2, 3)

    def scorecard_cells(self, s, cfg):
        M = s["metrics"]
        return [num(M.get("ocp.pods_not_ready")), num(M.get("ocp.pod_restarts_24h")),
                num(M.get("ocp.alerts_critical")), num(M.get("ocp.pvc_max_used_pct"), "%"),
                esc(s["info"].get("ocp_version") or "n/a"), esc(s["info"].get("operator_version") or "n/a")]

    def kpi_rows(self, snaps, hist, dates30, cfg):
        t, th = cfg["kpi_targets"], cfg["thresholds"]
        M = lambda s, k: s["metrics"].get(k)  # noqa: E731
        return common_kpis(self, snaps, hist, dates30, cfg) + [
            ("AAP pods not ready", "= 0", [kcell(M(s, "ocp.pods_not_ready"), 0, False) for s in snaps]),
            ("AAP pod restarts since last run", "", [kcell(M(s, "ocp.pod_restarts_24h")) for s in snaps]),
            ("Last good AAP backup (hours)", "<= %s" % t["backup_age_h"],
             [kcell(M(s, "ocp.backup_age_h"), t["backup_age_h"], False) for s in snaps]),
            ("Min route certificate days", ">= %s" % t["cert_min_days"],
             [kcell(M(s, "ocp.cert_min_days"), t["cert_min_days"]) for s in snaps]),
            ("Max PVC used", "<= %s%%" % th["pvc_warn_pct"],
             [kcell(M(s, "ocp.pvc_max_used_pct"), th["pvc_warn_pct"], False, "%") for s in snaps]),
            ("Critical alerts firing", "= 0", [kcell(M(s, "ocp.alerts_critical"), 0, False) for s in snaps]),
            ("Degraded ClusterOperators", "= 0", [kcell(M(s, "ocp.co_degraded"), 0, False) for s in snaps]),
            ("Busiest node memory", "<= %s%%" % th["node_mem_warn_pct"],
             [kcell(M(s, "ocp.node_mem_max_pct"), th["node_mem_warn_pct"], False, "%") for s in snaps]),
            ("etcd WAL fsync p99", "< %s ms" % th["etcd_fsync_warn_ms"],
             [kcell(M(s, "ocp.etcd_fsync_p99_ms"), th["etcd_fsync_warn_ms"], False, " ms") for s in snaps]),
            ("Open critical issues", "= %s" % t["open_critical"],
             [kcell(M(s, "open_critical"), t["open_critical"], False) for s in snaps]),
        ]

    def charts(self, cfg):
        t, th = cfg["kpi_targets"], cfg["thresholds"]
        return [
            ("OpenShift health score", "score", "", 100, t["health_score"],
             "Weighted share of OpenShift checks passing; dashed = target", "auto"),
            ("Critical alerts firing", "ocp.alerts_critical", "", None, None,
             "Prometheus alerts with severity critical (Watchdog excluded)", 0),
            ("AAP pod restarts since last run", "ocp.pod_restarts_24h", "", None, None,
             "Container restarts in the AAP namespace", 0),
            ("Max PVC used", "ocp.pvc_max_used_pct", "%", 100, th["pvc_warn_pct"],
             "Fullest persistent volume claim in the AAP namespace", 0),
            ("Busiest node memory", "ocp.node_mem_max_pct", "%", 100, th["node_mem_warn_pct"],
             "Highest node memory utilization", 0),
            ("etcd WAL fsync p99", "ocp.etcd_fsync_p99_ms", "ms", None, th["etcd_fsync_warn_ms"],
             "Red Hat recommends under %s ms" % th["etcd_fsync_warn_ms"], 0),
        ]

    def env_cards(self, s, cfg):
        t, th = cfg["kpi_targets"], cfg["thresholds"]
        M = s["metrics"]
        return [
            card(num(M.get("ocp.pods_not_ready")), "AAP pods not ready", "warn" if M.get("ocp.pods_not_ready") else "good",
                 "of %s pods" % num(M.get("ocp.pods"))),
            card(num(M.get("ocp.pod_restarts_24h")), "Pod restarts since last run",
                 "warn" if M.get("ocp.pod_restarts_24h") else "good"),
            card(num(M.get("ocp.backup_age_h"), "h"), "Last good AAP backup",
                 vs_target(M.get("ocp.backup_age_h"), t["backup_age_h"], False)),
            card(num(M.get("ocp.alerts_critical")), "Critical alerts firing",
                 "bad" if M.get("ocp.alerts_critical") else "good", "%s warning" % num(M.get("ocp.alerts_warning"))),
            card(num(M.get("ocp.pvc_max_used_pct"), "%"), "Max PVC used",
                 vs_target(M.get("ocp.pvc_max_used_pct"), th["pvc_warn_pct"], False)),
            card(num(M.get("ocp.cert_min_days")), "Min route cert days",
                 vs_target(M.get("ocp.cert_min_days"), t["cert_min_days"])),
        ]

    def env_charts(self, cfg):
        t = cfg["kpi_targets"]
        return [
            ("OpenShift health score", [("score", "score")], "", 100, t["health_score"], "auto"),
            ("AAP pods", [("restarts", "ocp.pod_restarts_24h"), ("not ready", "ocp.pods_not_ready"),
                          ("warning events", "ocp.warning_events_24h")], "", None, None, 0),
            ("Storage used (%)", [("max PVC", "ocp.pvc_max_used_pct"), ("database PVC", "ocp.db_pvc_used_pct")],
             "%", 100, None, 0),
            ("AAP namespace usage", [("CPU cores", "ocp.cpu_cores"), ("memory GiB", "ocp.mem_gib")], "", None, None, 0),
            ("Node headroom (%)", [("max CPU", "ocp.node_cpu_max_pct"), ("max memory", "ocp.node_mem_max_pct")],
             "%", 100, None, 0),
            ("etcd p99 latency (ms)", [("WAL fsync", "ocp.etcd_fsync_p99_ms"), ("backend commit", "ocp.etcd_commit_p99_ms"),
                                       ("peer RTT", "ocp.etcd_peer_rtt_p99_ms")], "ms", None, None, 0),
            ("Cluster alerts", [("critical", "ocp.alerts_critical"), ("warning", "ocp.alerts_warning"),
                                ("AAP namespace", "ocp.alerts_aap_ns")], "", None, None, 0),
            ("Backup and certificates", [("backup age h", "ocp.backup_age_h"), ("min cert days", "ocp.cert_min_days")],
             "", None, None, 0),
        ]

    def env_details(self, s):
        D = s["details"]
        al = D.get("alerts") or {}
        return [
            ("Operator custom resources", ["Kind", "Name", "Status", "Detail", "Version"],
             [[esc(r["kind"]), esc(r["name"]), pill(r["status"]), esc(r["detail"]), esc(r["version"])]
              for r in D.get("component_crs") or []], ()),
            ("Pods", ["Pod", "Component", "Phase", "Ready", "Restarts", "New restarts", "Node"],
             [[esc(r["pod"]), esc(r["component"]), esc(r["phase"]), esc(r["ready"]), num(r["restarts"]),
               num(r["restarts_delta"]), esc(r["node"])] for r in D.get("pods") or []], (4, 5)),
            ("Container resource usage (top 40 by memory)", ["Pod", "Container", "CPU (m)", "Memory (MiB)",
                                                             "% of memory limit"],
             [[esc(r["pod"]), esc(r["container"]), num(r["cpu_m"]), num(r["mem_mib"]), num(r["mem_limit_pct"], "%")]
              for r in D.get("usage") or []], (2, 3, 4)),
            ("Persistent volume claims", ["PVC", "Phase", "Capacity (GiB)", "Access modes", "Storage class", "Used %"],
             [[esc(r["pvc"]), esc(r["phase"]), num(r["capacity_gib"]), esc(r["modes"]), esc(r["storage_class"]),
               num(r["used_pct"], "%")] for r in D.get("pvcs") or []], (2, 5)),
            ("Route certificates", ["Route", "Host", "Days left", "Expires", "Issuer"],
             [[esc(r["route"]), esc(r["host"]), num(r.get("days_left")), esc(r.get("not_after") or r.get("error")),
               esc(r.get("issuer"))] for r in D.get("certs") or []], (2,)),
            ("Warning event reasons, 24h", ["Reason", "Count"],
             [[esc(k_), num(v)] for k_, v in D.get("event_reasons") or []], (1,)),
            ("Cluster nodes", ["Node", "Roles", "Ready", "Pressure", "Unschedulable", "Hosts AAP", "CPU %", "Memory %",
                               "Kubelet"],
             [[esc(r["node"]), esc(r["roles"]), esc(r["ready"]), esc(r["pressure"]), esc(r["unschedulable"]),
               esc(r["aap"]), num(r.get("cpu_pct"), "%"), num(r.get("mem_pct"), "%"), esc(r["kubelet"])]
              for r in D.get("nodes") or []], (6, 7)),
            ("Firing alerts", ["Severity", "Alert"],
             [["critical", esc(a)] for a in al.get("critical") or []] +
             [["warning", esc(a)] for a in al.get("warning") or []], ()),
        ]


def main(argv=None):
    return run_dashboard(OcpDashboard(), argv)


if __name__ == "__main__":
    sys.exit(main())
