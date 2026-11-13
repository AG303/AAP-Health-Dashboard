#!/usr/bin/env python3
"""
aap_health_dashboard.py - Daily health dashboard for Ansible Automation Platform 2.6
running on OpenShift Container Platform (operator install), across several environments.

Every run collects a health snapshot from each environment, scores it, appends the day
to history.json and writes a static site (no JavaScript, no CDN) that GitHub Pages or
any web server can host:

  <out>/index.html                   management overview: KPIs vs targets, trends, open issues
  <out>/env/<name>.html              engineering / support detail for one environment
  <out>/history.json                 one entry per day per environment (drives every chart)
  <out>/data/latest.json             today's full snapshot, all environments
  <out>/data/checks_latest.csv       every check, one row each (for support tickets / Excel)
  <out>/data/snapshots/<date>.json   archived daily snapshots (pruned after --keep-days)
  <out>/.nojekyll                    so GitHub Pages serves files as-is

What is checked (each check cites the Red Hat guidance it implements; see docs/CHECKS.md)
  Platform gateway  status endpoint (controller/hub/eda/redis), API latency, users, orgs
  Controller        ping + heartbeats, instance health/errors/capacity, instance groups,
                    metrics endpoint (job event lag, Redis callback queue, remaining capacity),
                    24h job success / duration / volume, pending + long-running jobs,
                    project and inventory sync failures, missed schedules, cleanup management
                    jobs, subscription compliance / host usage / days left
  Event-Driven      status endpoint, activation health and restarts, failed rulebook
  Ansible           processes, rules fired, project import failures
  Automation hub    Pulp status (workers, content apps, DB, Redis, storage), failed and
                    waiting tasks, content counts
  OpenShift (AAP    AnsibleAutomationPlatform + component CR conditions, operator CSV and
  namespace)        subscription, pods (ready, restarts, CrashLoop, OOMKilled), deployments and
                    statefulsets, PVCs (bound, usage), CPU/memory vs limits, task container
                    limits, pod spread across nodes, warning events, routes and TLS expiry,
                    backup freshness, operator-managed PostgreSQL limits, quota usage
  OpenShift         ClusterVersion (Available/Failing/Upgradeable, updates), ClusterOperators,
  (cluster)         node Ready and pressure conditions, MachineConfigPools, pending CSRs,
                    firing alerts, etcd WAL fsync / backend commit / peer RTT / leader changes,
                    node CPU and memory headroom, CPU throttling of AAP pods
  Cluster-scope and Prometheus checks are optional and report "unknown" (never fail) when
  the service account lacks cluster-reader / cluster-monitoring-view.

Requirements: Python 3.8+ (3.9+ for --report-tz), standard library only.

Configuration: a JSON file (default environments.json) - see environments.example.json.
Read-only by design: the script only ever sends HTTP GET requests (enforced in the Http
class) and authenticates with bearer tokens only - a read-scoped AAP OAuth token for a
user with the Platform Auditor role, and an OpenShift ServiceAccount token bound to
get/list/watch-only roles plus cluster-monitoring-view (deploy/openshift/rbac.yaml).
Secrets are never in the file. Each environment names the environment variables that hold
its tokens; the script reads them from the process environment or, in GitHub Actions,
from AAP_HEALTH_SECRETS_JSON (set it to ${{ toJSON(secrets) }}).

Usage
  python3 aap_health_dashboard.py -c environments.json -o site
  python3 aap_health_dashboard.py -c environments.json -o site --serve 8080
  # cron, every day at 23:00 local time:
  # 0 23 * * * cd /opt/aap-health && . ./.env && python3 aap_health_dashboard.py -c environments.json -o /var/www/html/aap-health -q

Exit codes: 0 = every environment collected; 1 = partial (an environment or section could
not be collected - the dashboard is still written); 2 = nothing collected / bad config.
--fail-on fail|warn makes the exit code 3 when any environment is at that status or worse,
so a CI job can go red when the platform is unhealthy.
"""
import argparse
import csv
import html
import io
import json
import math
import os
import re
import socket
import ssl
import statistics
import sys
import tempfile
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import error, parse, request

VERSION = "1.0.0"
UTC = timezone.utc

# --------------------------------------------------------------------------- #
# Red Hat guidance each check implements (rendered as "Ref" links)
# --------------------------------------------------------------------------- #
DOC = "https://docs.redhat.com/en/documentation/red_hat_ansible_automation_platform/2.6/"
SOURCES = {
    "aap_cluster": ("AAP 2.6 - Planning your cluster (ping, heartbeats, instances)",
                    DOC + "configure-planning_your_cluster"),
    "aap_monitor": ("AAP 2.6 - Ensure system health through monitoring (metrics of interest)",
                    DOC + "observe-ensure_system_health_and_efficiency_through_monitoring"),
    "aap_metrics": ("AAP 2.6 - Send metrics to system monitoring software",
                    DOC + "observe-assembly_controller_metrics"),
    "aap_gw_status": ("AAP 2.6 - Platform gateway OpenAPI (/api/gateway/v1/status/)",
                      DOC + "configure-con_gw_open_api_specification"),
    "aap_eda_api": ("AAP 2.6 - EDA API root (status, activations, instances)",
                    DOC + "administer-assembly_simplified_event_routing"),
    "aap_topology": ("AAP 2.6 - Operator growth topology (operator database limits)",
                     DOC + "plan-ref_ocp_a_env_a"),
    "aap_perf_ocp": ("AAP 2.6 - Performance considerations for operator environments",
                     DOC + "html-single/performance_considerations_for_operator_environments/index"),
    "aap_backup": ("AAP 2.6 - Back up the platform on OpenShift",
                   DOC + "administer-assembly_aap_backup"),
    "aap_cr": ("AAP 2.6 - AnsibleAutomationPlatform custom resource",
               DOC + "reference-ansibleautomationplatform__aap_ansible_com_v1alpha1_"),
    "aap_lifecycle": ("Red Hat AAP life cycle",
                      "https://access.redhat.com/support/policy/updates/ansible-automation-platform"),
    "ocp_upgrade_checks": ("Red Hat - OpenShift 4 cluster health pre-checks",
                           "https://access.redhat.com/solutions/7004992"),
    "ocp_validate": ("OpenShift 4.20 - Validation and troubleshooting",
                     "https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/pdf/validation_and_troubleshooting/index"),
    "ocp_nodes": ("OpenShift 4.22 - Working with nodes (node conditions)",
                  "https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/nodes/working-with-nodes"),
    "ocp_etcd": ("OpenShift 4.18 - Recommended performance practices (etcd)",
                 "https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/scalability_and_performance/recommended-performance-and-scalability-practices-2"),
    "ocp_etcd_metrics": ("OpenShift cluster-etcd-operator - etcd performance metrics",
                         "https://github.com/openshift/cluster-etcd-operator/blob/master/docs/performance-metrics.md"),
    "toolkit": ("AAP 2.6 Troubleshooting Toolkit (this project)", ""),
}

# --------------------------------------------------------------------------- #
# Defaults (override in the config file: "thresholds", "kpi_targets", per env)
# --------------------------------------------------------------------------- #
DEFAULT_THRESHOLDS = {
    "api_latency_warn_ms": 1500, "api_latency_fail_ms": 5000,
    "heartbeat_warn_s": 300, "heartbeat_fail_s": 900,
    "instance_health_check_stale_h": 24,
    "capacity_warn_pct": 80, "capacity_fail_pct": 95,
    "job_success_warn_pct": 95, "job_success_fail_pct": 85, "job_success_min_jobs": 10,
    "pending_warn_min": 10, "pending_fail_min": 30,
    "long_running_h": 4,
    "missed_schedule_grace_min": 60,
    "event_lag_warn_s": 10, "event_lag_fail_s": 60,
    "callback_queue_warn": 10000, "callback_queue_fail": 100000,
    "license_days_warn": 60, "license_days_fail": 14,
    "host_usage_warn_pct": 90, "host_usage_fail_pct": 100,
    "eda_restart_warn": 5,
    "hub_storage_warn_pct": 80, "hub_storage_fail_pct": 90,
    "hub_waiting_task_warn_min": 30,
    "pod_restarts_warn": 3, "pod_restarts_fail": 10,
    "pvc_warn_pct": 80, "pvc_fail_pct": 90,
    "mem_limit_warn_pct": 85, "mem_limit_fail_pct": 95,
    "cert_warn_days": 30, "cert_fail_days": 7,
    "backup_warn_h": 26, "backup_fail_h": 72,
    "warning_events_warn": 20,
    "db_connections_warn_pct": 80, "db_connections_fail_pct": 95,
    "operator_db_max_connections": 100, "operator_db_max_gb": 100,
    "operator_db_max_concurrent_jobs": 100,
    "etcd_fsync_warn_ms": 10, "etcd_fsync_fail_ms": 20,
    "etcd_commit_warn_ms": 25, "etcd_commit_fail_ms": 100,
    "etcd_peer_rtt_warn_ms": 50, "etcd_peer_rtt_fail_ms": 100,
    "etcd_leader_changes_warn": 1, "etcd_leader_changes_fail": 5,
    "node_cpu_warn_pct": 80, "node_cpu_fail_pct": 95,
    "node_mem_warn_pct": 85, "node_mem_fail_pct": 95,
    "cpu_throttle_warn_pct": 25, "cpu_throttle_fail_pct": 50,
    "quota_warn_pct": 85, "quota_fail_pct": 95,
}
DEFAULT_KPI_TARGETS = {
    "health_score": 90,           # % weighted checks passing
    "job_success_pct": 95,        # 24h and 30d
    "green_days_pct": 90,         # share of the last 30 days the environment was green
    "capacity_used_pct": 80,      # max control capacity used (lower is better)
    "license_days_left": 60,
    "backup_age_h": 26,
    "cert_min_days": 30,
    "open_critical": 0,
}
DEFAULT_SETTINGS = {
    "minutes_saved_per_job": 15,  # for the "estimated hours saved" KPI; tune per org
    "active_user_days": 30,
    "max_jobs": 20000,
    "max_objects": 20000,
    "check_route_certs": True,
    "cluster_checks": True,
    "excluded_alerts": ["Watchdog", "AlertmanagerReceiversNotConfigured", "InfoInhibitor"],
}

SEVERITY_WEIGHT = {"critical": 3, "major": 2, "minor": 1}
STATUS_RANK = {"fail": 0, "warn": 1, "unknown": 2, "ok": 3}
STATUS_VALUE = {"ok": 1.0, "warn": 0.5, "fail": 0.0}
COMPONENTS = [
    ("gateway", "Platform gateway"), ("controller", "Automation controller"),
    ("eda", "Event-Driven Ansible"), ("hub", "Automation hub"),
    ("platform", "OpenShift - AAP namespace"), ("cluster", "OpenShift - cluster"),
]
COMPONENT_LABEL = dict(COMPONENTS)


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def log(msg, quiet=False):
    if not quiet:
        print(msg, file=sys.stderr, flush=True)


def parse_dt(value):
    if not value or not isinstance(value, str):
        return None
    v = value.strip().replace("Z", "+00:00")
    m = re.match(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d+)?(.*)$", v)
    if not m:
        try:
            return datetime.strptime(v[:10], "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            return None
    frac = (m.group(3) or "")[:7]
    tz = m.group(4) or "+00:00"
    try:
        dt = datetime.fromisoformat("%sT%s%s%s" % (m.group(1), m.group(2), frac, tz))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def iso(dt):
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def pct(part, whole):
    return round(part * 100.0 / whole, 1) if whole else None


def percentile(values, q):
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    k = (len(vals) - 1) * q
    lo, hi = int(math.floor(k)), int(math.ceil(k))
    return vals[lo] if lo == hi else vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def worst(statuses):
    known = [s for s in statuses if s]
    return min(known, key=lambda s: STATUS_RANK[s]) if known else "unknown"


def grade_high(value, warn, fail):
    """Higher is worse."""
    if value is None:
        return "unknown"
    return "fail" if value >= fail else ("warn" if value >= warn else "ok")


def grade_low(value, warn, fail):
    """Lower is worse."""
    if value is None:
        return "unknown"
    return "fail" if value <= fail else ("warn" if value <= warn else "ok")


def cpu_cores(q):
    """Kubernetes CPU quantity -> cores."""
    if q in (None, ""):
        return None
    q = str(q)
    try:
        if q.endswith("n"):
            return float(q[:-1]) / 1e9
        if q.endswith("u"):
            return float(q[:-1]) / 1e6
        if q.endswith("m"):
            return float(q[:-1]) / 1000.0
        return float(q)
    except ValueError:
        return None


_MEM = {"Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4, "Pi": 1024 ** 5,
        "K": 1000, "k": 1000, "M": 1000 ** 2, "G": 1000 ** 3, "T": 1000 ** 4, "P": 1000 ** 5, "E": 1000 ** 6}


def mem_bytes(q):
    if q in (None, ""):
        return None
    m = re.match(r"^([0-9.eE+-]+)([A-Za-z]*)$", str(q))
    if not m:
        return None
    try:
        return float(m.group(1)) * _MEM.get(m.group(2), 1)
    except ValueError:
        return None


def gib(b):
    return round(b / 1024.0 ** 3, 2) if b is not None else None


def cond(obj, ctype):
    for c in ((obj or {}).get("status") or {}).get("conditions") or []:
        if c.get("type") == ctype:
            return c
    return None


def cond_true(obj, ctype):
    c = cond(obj, ctype)
    return None if c is None else str(c.get("status")).lower() == "true"


def short(text, n=240):
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    return text if len(text) <= n else text[:n - 1] + "..."


# --------------------------------------------------------------------------- #
# HTTP client (AAP, OpenShift and Prometheus all speak JSON over HTTPS)
# --------------------------------------------------------------------------- #
class ApiError(Exception):
    def __init__(self, msg, code=None):
        super().__init__(msg)
        self.code = code


class Http:
    """Read-only HTTP client.

    Only GET is implemented - there is deliberately no post/put/patch/delete method, and
    urlopen() is always given a Request with method="GET". Authentication is a bearer token
    only (no passwords), so a read-scoped AAP token for an Auditor user and an OpenShift
    ServiceAccount token bound to view / cluster-reader / cluster-monitoring-view are enough.
    """
    METHOD = "GET"

    def __init__(self, label, base, token=None, verify=True, ca_bundle=None, timeout=30, retries=2):
        base = (base or "").strip().rstrip("/")
        if base and not re.match(r"^https?://", base, flags=re.I):
            base = "https://" + base
        self.label, self.base = label, base
        self.token = token
        self.timeout, self.retries = timeout, retries
        if verify:
            self.ctx = ssl.create_default_context(cafile=ca_bundle) if ca_bundle else ssl.create_default_context()
        else:
            self.ctx = ssl.create_default_context()
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE
        self.requests = 0
        self.truncated = set()

    def _headers(self, accept):
        h = {"Accept": accept, "User-Agent": "aap-health-dashboard/" + VERSION}
        if self.token:
            h["Authorization"] = "Bearer " + self.token
        return h

    def url(self, path, params=None):
        u = path if re.match(r"^https?://", path) else self.base + path
        if params:
            u += ("&" if "?" in u else "?") + parse.urlencode(params, doseq=True)
        return u

    def get(self, path, params=None, accept="application/json", raw=False):
        """Returns (body, latency_ms). Raises ApiError."""
        u = self.url(path, params)
        last, code = None, None
        for attempt in range(self.retries + 1):
            t0 = time.monotonic()
            try:
                self.requests += 1
                req = request.Request(u, headers=self._headers(accept), method=self.METHOD)
                with request.urlopen(req, timeout=self.timeout, context=self.ctx) as resp:
                    body = resp.read().decode("utf-8", "replace")
                    ms = round((time.monotonic() - t0) * 1000.0, 1)
                    return (body if raw else json.loads(body)), ms
            except error.HTTPError as exc:
                code = exc.code
                if exc.code in (401, 403, 404):
                    hint = {401: "token rejected", 403: "permission denied",
                            404: "not found (component not installed or wrong path)"}[exc.code]
                    raise ApiError("HTTP %d %s: %s" % (exc.code, hint, u), exc.code)
                last = "HTTP %d" % exc.code
                if exc.code == 500 and attempt == self.retries:
                    try:
                        body = exc.read().decode("utf-8", "replace")
                        return (body if raw else json.loads(body)), None
                    except Exception:  # noqa: BLE001
                        pass
            except (error.URLError, OSError, ValueError) as exc:
                last = str(getattr(exc, "reason", exc))
            if attempt < self.retries:
                time.sleep(min(2 ** attempt, 6))
        raise ApiError("%s (%s after %d attempts)" % (u, last, self.retries + 1), code)

    def json(self, path, params=None):
        return self.get(path, params)[0]

    def text(self, path, params=None, accept="text/plain"):
        return self.get(path, params, accept=accept, raw=True)[0]

    def count(self, path, params=None, style="drf"):
        q = dict(params or {})
        if style == "pulp":
            q["limit"] = 1
        else:
            q["page_size"] = 1
        data = self.json(path, q)
        if "meta" in data and isinstance(data["meta"], dict):
            return int(data["meta"].get("count", 0))
        return int(data.get("count", 0))

    def iter(self, path, params=None, page_size=200, max_items=None, style="drf"):
        """DRF (controller/gateway/eda: page_size + next) or pulp (limit/offset + next)."""
        q = dict(params or {})
        if style == "pulp":
            q["limit"] = page_size
        else:
            q["page_size"] = page_size
        u, seen = self.url(path, q), 0
        while u:
            data = self.json(u)
            items = data.get("results")
            if items is None:
                items = data.get("data", [])
            for item in items:
                yield item
                seen += 1
                if max_items and seen >= max_items:
                    self.truncated.add(path)
                    return
            nxt = data.get("next") or ((data.get("links") or {}).get("next"))
            if nxt and not re.match(r"^https?://", nxt):
                nxt = self.base + (nxt if nxt.startswith("/") else "/" + nxt)
            u = nxt

    def k8s_list(self, path, params=None, max_items=20000):
        """Kubernetes list with continue tokens."""
        q = dict(params or {})
        q["limit"] = 500
        out = []
        while True:
            data = self.json(path, q)
            out.extend(data.get("items") or [])
            cont = (data.get("metadata") or {}).get("continue")
            if not cont or len(out) >= max_items:
                return out
            q["continue"] = cont


def tls_expiry(host, port=443, timeout=10):
    """Return (not_after datetime, issuer, subject) for host:port - works for untrusted certs."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=timeout) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as tls:
            der = tls.getpeercert(binary_form=True)
    pem = ssl.DER_cert_to_PEM_cert(der)
    fd, path = tempfile.mkstemp(suffix=".pem")
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(pem)
        info = ssl._ssl._test_decode_cert(path)  # noqa: SLF001 - stdlib only, no cryptography lib
    finally:
        os.unlink(path)
    not_after = datetime.fromtimestamp(ssl.cert_time_to_seconds(info["notAfter"]), UTC)

    def name(seq):
        return ", ".join("%s=%s" % kv for rdn in seq or () for kv in rdn)
    return not_after, name(info.get("issuer")), name(info.get("subject"))


def prom_metrics(text):
    """Parse Prometheus text exposition -> {metric: [(labels dict, value)]}."""
    out = defaultdict(list)
    for line in (text or "").splitlines():
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+([-+0-9.eEInfNa]+)", line)
        if not m:
            continue
        labels = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', m.group(3) or ""))
        try:
            out[m.group(1)].append((labels, float(m.group(4))))
        except ValueError:
            pass
    return out


# --------------------------------------------------------------------------- #
# Secrets
# --------------------------------------------------------------------------- #
def lookup_secret(name):
    """Environment variable first, then the JSON blob in AAP_HEALTH_SECRETS_JSON."""
    if not name:
        return None
    if os.environ.get(name):
        return os.environ[name].strip()
    blob = os.environ.get("AAP_HEALTH_SECRETS_JSON")
    if blob:
        try:
            val = json.loads(blob).get(name)
            return val.strip() if isinstance(val, str) and val.strip() else None
        except ValueError:
            return None
    return None


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


# --------------------------------------------------------------------------- #
# Collector - one per environment
# --------------------------------------------------------------------------- #
class Collector:
    def __init__(self, env, settings, thresholds, now, previous, quiet=False):
        self.env, self.s, self.t, self.now = env, settings, thresholds, now
        self.prev = previous or {}
        self.quiet = quiet
        self.checks, self.metrics, self.details, self.errors = [], {}, {}, []
        self.info = {}
        verify = env.get("verify_tls", True)
        ca = env.get("ca_bundle")
        timeout = int(env.get("timeout", 30))
        self.aap = None
        if env.get("aap_url"):
            self.aap = Http("aap", env["aap_url"], lookup_secret(env.get("aap_token_env")), verify, ca, timeout)
        self.ocp = None
        if env.get("ocp_api_url"):
            self.ocp = Http("ocp", env["ocp_api_url"], lookup_secret(env.get("ocp_token_env")),
                            env.get("ocp_verify_tls", verify), env.get("ocp_ca_bundle", ca), timeout)
        self.prom = None
        if env.get("prometheus_url"):
            tok = lookup_secret(env.get("prometheus_token_env") or env.get("ocp_token_env"))
            self.prom = Http("prom", env["prometheus_url"], tok, env.get("ocp_verify_tls", verify),
                             env.get("ocp_ca_bundle", ca), timeout)
        self.ns = env.get("namespace", "aap")
        self.cr = env.get("aap_cr_name", "aap")
        self.op_ns = env.get("operator_namespace", self.ns)
        self.ctl = env.get("controller_prefix", "/api/controller/v2")
        self.eda = env.get("eda_prefix", "/api/eda/v1")
        self.hub = env.get("hub_prefix", "/api/galaxy")
        self.gw = env.get("gateway_prefix", "/api/gateway/v1")
        self.since24 = now - timedelta(hours=24)
        self.since30 = now - timedelta(days=30)

    # ---- bookkeeping -------------------------------------------------------- #
    def add(self, cid, comp, title, status, value="", detail="", hint="", sev="major", src=""):
        self.checks.append({"id": cid, "component": comp, "title": title, "status": status,
                            "severity": sev, "value": "" if value is None else str(value),
                            "detail": short(detail, 600), "hint": hint, "source": src})

    def m(self, key, value):
        if value is not None:
            self.metrics[key] = round(value, 3) if isinstance(value, float) else value

    def section(self, comp, title, fn):
        t0 = time.monotonic()
        try:
            fn()
        except ApiError as exc:
            self._section_error(comp, title, exc, exc.code)
        except Exception as exc:  # noqa: BLE001 - one broken section must not kill the run
            self._section_error(comp, title, exc, None)
        log("    %-10s %-38s %5.1fs" % (comp, title, time.monotonic() - t0), self.quiet)

    def _section_error(self, comp, title, exc, code):
        msg = short(exc, 300)
        rbac = code in (401, 403)
        self.errors.append({"component": comp, "section": title, "error": msg})
        hint = ("Grant the read-only role described in docs/RBAC.md (" +
                ("AAP Platform Auditor" if comp in ("gateway", "controller", "eda", "hub")
                 else "view / cluster-reader / cluster-monitoring-view") + ")") if rbac else \
            "Check connectivity and the URL in environments.json; run python/01_aap_api_health_check.py"
        self.add("%s.collect.%s" % (comp, re.sub(r"\W+", "_", title.lower())), comp,
                 "Collect: " + title, "unknown", "error", msg, hint, "minor")

    def optional(self, comp, cid, title, fn, src, sev="minor"):
        """Cluster-scope / Prometheus checks: a 403 or 404 becomes 'unknown', never a failure."""
        try:
            fn()
        except ApiError as exc:
            if exc.code in (401, 403):
                self.add(cid, comp, title, "unknown", "no access", short(exc, 200),
                         "Optional check: bind the reader ServiceAccount to cluster-reader / "
                         "cluster-monitoring-view (deploy/openshift/rbac.yaml)", sev, src)
            elif exc.code == 404:
                self.add(cid, comp, title, "unknown", "not available", short(exc, 200),
                         "API not present on this cluster", sev, src)
            else:
                self.errors.append({"component": comp, "section": title, "error": short(exc, 300)})
                self.add(cid, comp, title, "unknown", "error", short(exc, 200), "", sev, src)
        except Exception as exc:  # noqa: BLE001
            self.errors.append({"component": comp, "section": title, "error": short(exc, 300)})
            self.add(cid, comp, title, "unknown", "error", short(exc, 200), "", sev, src)

    # ---- run ---------------------------------------------------------------- #
    def run(self):
        log("  [%s] collecting" % self.env["name"], self.quiet)
        if self.aap:
            if not self.aap.token:
                self.add("gateway.token", "gateway", "AAP API token configured", "fail", "missing",
                         "Environment variable %s is empty" % self.env.get("aap_token_env"),
                         "Create a read-scoped token for an Auditor user and store it as a secret", "critical")
            else:
                self.section("gateway", "gateway status and identity", self.gateway)
                self.section("controller", "controller cluster and capacity", self.controller_cluster)
                self.section("controller", "controller metrics endpoint", self.controller_metrics)
                self.section("controller", "controller jobs", self.controller_jobs)
                self.section("controller", "controller content and schedules", self.controller_content)
                self.section("controller", "controller subscription", self.controller_license)
                if self.env.get("eda_enabled", True):
                    self.section("eda", "event-driven ansible", self.eda_checks)
                if self.env.get("hub_enabled", True):
                    self.section("hub", "automation hub", self.hub_checks)
        if self.ocp:
            if not self.ocp.token:
                self.add("platform.token", "platform", "OpenShift token configured", "fail", "missing",
                         "Environment variable %s is empty" % self.env.get("ocp_token_env"),
                         "Create the aap-health-reader ServiceAccount token (deploy/openshift/)", "critical")
            else:
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
    # Platform gateway
    # ======================================================================= #
    def gateway(self):
        t, a = self.t, self.aap
        data, ms = a.get(self.gw + "/status/")
        self.m("gw.latency_ms", ms)
        services = data.get("services") or []
        if isinstance(services, dict):
            services = [dict(v, service_name=k) if isinstance(v, dict) else {"service_name": k, "status": v}
                        for k, v in services.items()]
        bad = []
        for svc in services:
            name = svc.get("service_name") or svc.get("name") or "service"
            st = str(svc.get("status", "")).lower()
            if name.lower() == "redis":
                mode = (svc.get("response") or {}).get("mode") or svc.get("mode")
                if mode:
                    self.info["redis_mode"] = mode
            if st not in ("good", "ok", "healthy", "online"):
                bad.append("%s=%s" % (name, st or "unknown"))
        top = str(data.get("status", "good" if not bad else "degraded")).lower()
        status = "ok" if not bad and top in ("good", "ok", "healthy") else "fail"
        self.add("gateway.status", "gateway", "Gateway status endpoint (all services good)", status,
                 top, "; ".join(bad) or "%d services reporting good" % len(services),
                 "Run python/01_aap_api_health_check.py and check the failing service pods", "critical",
                 "aap_gw_status")
        self.m("gw.services_down", len(bad))
        self.add("gateway.latency", "gateway", "Gateway API response time",
                 grade_high(ms, t["api_latency_warn_ms"], t["api_latency_fail_ms"]), "%s ms" % ms,
                 "warn >= %s ms, fail >= %s ms" % (t["api_latency_warn_ms"], t["api_latency_fail_ms"]),
                 "Check gateway pod CPU/memory and the database; see Performance considerations", "minor",
                 "aap_perf_ocp")
        me = a.json(self.gw + "/me/")
        user = (me.get("results") or [me])[0] if isinstance(me, dict) else {}
        self.info["api_user"] = user.get("username")
        self.add("gateway.token_valid", "gateway", "API token valid", "ok", user.get("username") or "valid",
                 "", "", "critical")
        if user.get("is_superuser"):
            self.add("gateway.least_privilege", "gateway", "Dashboard token is read-only (least privilege)",
                     "warn", "superuser",
                     "The dashboard token belongs to a superuser. Every call is a GET, so a read-scoped "
                     "token for a Platform Auditor user is enough.",
                     "Create an Auditor user and a token with scope=read (docs/RBAC.md)", "minor")
        else:
            self.add("gateway.least_privilege", "gateway", "Dashboard token is read-only (least privilege)",
                     "ok", "auditor" if user.get("is_system_auditor") else "non-admin", "", "", "minor")
        total = active = 0
        cutoff = self.now - timedelta(days=int(self.s["active_user_days"]))
        for u in a.iter(self.gw + "/users/", max_items=self.s["max_objects"]):
            total += 1
            ll = parse_dt(u.get("last_login"))
            if ll and ll >= cutoff:
                active += 1
        self.m("gw.users_total", total)
        self.m("gw.users_active_30d", active)
        try:
            self.m("gw.orgs", a.count(self.gw + "/organizations/"))
        except ApiError:
            pass

    # ======================================================================= #
    # Automation controller
    # ======================================================================= #
    def controller_cluster(self):
        t, a, c = self.t, self.aap, self.ctl
        ping, ms = a.get(c + "/ping/")
        self.m("ctl.latency_ms", ms)
        self.info["controller_version"] = ping.get("version")
        self.info["controller_ha"] = ping.get("ha")
        hb_bad, ages = [], []
        for inst in ping.get("instances") or []:
            hb = parse_dt(inst.get("heartbeat"))
            age = (self.now - hb).total_seconds() if hb else None
            if age is not None:
                ages.append(age)
            if inst.get("node_type") in ("control", "hybrid", "execution") and (
                    age is None or age >= t["heartbeat_warn_s"]):
                hb_bad.append("%s (%s)" % (inst.get("node"), "no heartbeat" if age is None else "%ds" % age))
        oldest = max(ages) if ages else None
        st = "ok" if not hb_bad else grade_high(oldest or 10 ** 9, t["heartbeat_warn_s"], t["heartbeat_fail_s"])
        self.add("controller.heartbeat", "controller", "Instance heartbeats current (ping)", st,
                 "%d instances" % len(ping.get("instances") or []), "; ".join(hb_bad) or
                 "oldest heartbeat %ss" % (int(oldest) if oldest is not None else "n/a"),
                 "Stale heartbeat = instance lost; check the controller task pod logs", "critical", "aap_cluster")

        insts = list(a.iter(c + "/instances/", max_items=2000))
        bad, stale, errors_ = [], [], []
        enabled = [i for i in insts if i.get("enabled", True) and i.get("node_type") != "hop"]
        for i in enabled:
            state = i.get("node_state") or "ready"
            if state not in ("ready", "installed", "provisioning"):
                bad.append("%s=%s" % (i.get("hostname"), state))
            if i.get("errors"):
                errors_.append("%s: %s" % (i.get("hostname"), short(i.get("errors"), 120)))
            lhc = parse_dt(i.get("last_health_check"))
            if lhc and (self.now - lhc).total_seconds() > t["instance_health_check_stale_h"] * 3600:
                stale.append(i.get("hostname"))
        self.m("ctl.instances", len(enabled))
        self.m("ctl.instances_unhealthy", len(bad) + len(errors_))
        self.add("controller.instances", "controller", "Instances ready with no health-check errors",
                 "fail" if (bad or errors_) else "ok", "%d/%d ready" % (len(enabled) - len(bad), len(enabled)),
                 "; ".join(bad + errors_) or "all enabled instances ready",
                 "Run python/03_aap_instance_capacity_report.py; check instance errors in the UI",
                 "critical", "aap_cluster")
        self.add("controller.health_check_age", "controller", "Instance health checks recent",
                 "warn" if stale else "ok", "%d stale" % len(stale), ", ".join(stale[:10]) or
                 "all checked within %sh" % t["instance_health_check_stale_h"],
                 "Health checks run automatically; a stale one points at a stuck dispatcher", "minor",
                 "aap_cluster")
        jobs_running = sum(int(i.get("jobs_running") or 0) for i in insts)
        self.m("ctl.jobs_running", jobs_running)
        self.details["instances"] = [
            {"hostname": i.get("hostname"), "type": i.get("node_type"), "state": i.get("node_state"),
             "enabled": i.get("enabled"), "capacity": i.get("capacity"),
             "used": i.get("consumed_capacity"), "remaining_pct": i.get("percent_capacity_remaining"),
             "jobs_running": i.get("jobs_running"), "version": i.get("version"),
             "last_health_check": i.get("last_health_check")} for i in insts[:200]]

        groups = list(a.iter(c + "/instance_groups/", max_items=500))
        worst_pct, rows = None, []
        for g in groups:
            cap = g.get("capacity") or 0
            used = g.get("consumed_capacity") or 0
            used_pct = pct(used, cap) if cap else None
            if g.get("percent_capacity_remaining") is not None and cap:
                used_pct = round(100 - float(g["percent_capacity_remaining"]), 1)
            rows.append({"name": g.get("name"), "container_group": g.get("is_container_group"),
                         "capacity": cap, "used": used, "used_pct": used_pct,
                         "running": g.get("jobs_running"), "instances": g.get("instances")})
            if not g.get("is_container_group") and used_pct is not None:
                worst_pct = used_pct if worst_pct is None else max(worst_pct, used_pct)
        self.details["instance_groups"] = rows
        self.m("ctl.capacity_used_pct", worst_pct)
        self.add("controller.capacity", "controller", "Instance group capacity headroom",
                 grade_high(worst_pct, t["capacity_warn_pct"], t["capacity_fail_pct"]),
                 "%s%% used" % worst_pct if worst_pct is not None else "n/a",
                 "Highest non-container instance group. Container groups are not capacity bound.",
                 "Scale task replicas or raise task_resource_requirements limits; "
                 "python/03_aap_instance_capacity_report.py", "major", "aap_perf_ocp")

    def controller_metrics(self):
        t = self.t
        text = self.aap.text(self.ctl + "/metrics/")
        pm = prom_metrics(text)

        def mx(name):
            vals = [v for _, v in pm.get(name, []) if not math.isnan(v)]
            return max(vals) if vals else None

        def sm(name):
            vals = [v for _, v in pm.get(name, []) if not math.isnan(v)]
            return sum(vals) if vals else None
        lag = mx("callback_receiver_event_processing_avg_seconds")
        queue = mx("callback_receiver_events_queue_size_redis")
        dbc = sm("awx_database_connections_total")
        self.m("ctl.event_lag_s", lag)
        self.m("ctl.callback_queue", queue)
        self.m("ctl.db_connections", dbc)
        self.m("ctl.sessions", sm("awx_sessions_total"))
        self.m("ctl.remaining_capacity", sm("awx_instance_remaining_capacity"))
        self.m("ctl.pending_metric", sm("awx_pending_jobs_total"))
        self.add("controller.event_lag", "controller", "Job event processing lag",
                 grade_high(lag, t["event_lag_warn_s"], t["event_lag_fail_s"]),
                 "%.2f s" % lag if lag is not None else "n/a",
                 "callback_receiver_event_processing_avg_seconds. Red Hat: a large number means high "
                 "lag; job output lags behind the job.",
                 "Add task capacity / callback receiver workers; reduce verbosity", "major", "aap_monitor")
        self.add("controller.callback_queue", "controller", "Redis callback receiver queue",
                 grade_high(queue, t["callback_queue_warn"], t["callback_queue_fail"]),
                 "%d events" % queue if queue is not None else "n/a",
                 "callback_receiver_events_queue_size_redis. Red Hat: a large queue risks Redis "
                 "running out of memory.",
                 "Check Redis memory limits and the callback receiver; python/26_* dispatcher checker",
                 "major", "aap_monitor")
        self.info["metrics_series"] = len(pm)

    def controller_jobs(self):
        t, a, c = self.t, self.aap, self.ctl
        s24, s30 = iso(self.since24), iso(self.since30)
        statuses = Counter()
        durations, launch = [], Counter()
        fail_by_tpl = Counter()
        for j in a.iter(c + "/jobs/", {"finished__gt": s24, "order_by": "-finished"},
                        max_items=self.s["max_jobs"]):
            statuses[j.get("status")] += 1
            if j.get("elapsed") is not None:
                durations.append(float(j["elapsed"]))
            launch[j.get("launch_type") or "unknown"] += 1
            if j.get("status") in ("failed", "error"):
                fail_by_tpl[j.get("name") or "unknown"] += 1
        ok_, failed = statuses["successful"], statuses["failed"] + statuses["error"]
        finished = ok_ + failed
        rate = pct(ok_, finished)
        self.m("ctl.jobs_24h", sum(statuses.values()))
        self.m("ctl.jobs_success_24h", ok_)
        self.m("ctl.jobs_failed_24h", failed)
        self.m("ctl.jobs_canceled_24h", statuses["canceled"])
        self.m("ctl.success_rate_24h", rate)
        self.m("ctl.job_avg_s", round(statistics.mean(durations), 1) if durations else None)
        self.m("ctl.job_p95_s", round(percentile(durations, 0.95), 1) if durations else None)
        self.details["launch_types"] = dict(launch)
        self.details["top_failing_templates"] = fail_by_tpl.most_common(10)
        if finished < t["job_success_min_jobs"]:
            st = "ok" if failed == 0 else "warn"
        else:
            st = ("ok" if rate >= t["job_success_warn_pct"] else
                  "warn" if rate >= t["job_success_fail_pct"] else "fail")
        self.add("controller.job_success", "controller", "Job success rate (24h)", st,
                 "%s%% of %d" % (rate, finished) if rate is not None else "no jobs",
                 "Top failing: " + ", ".join("%s (%d)" % kv for kv in fail_by_tpl.most_common(5))
                 if fail_by_tpl else "no failed jobs",
                 "Run python/05_aap_stuck_job_finder.py and the job failure analyzer in the toolkit",
                 "major", "aap_monitor")
        # 30-day volume via counts (cheap)
        try:
            j30 = a.count(c + "/jobs/", {"finished__gt": s30})
            ok30 = a.count(c + "/jobs/", {"finished__gt": s30, "status": "successful"})
            f30 = a.count(c + "/jobs/", {"finished__gt": s30, "status__in": "failed,error"})
            self.m("ctl.jobs_30d", j30)
            self.m("ctl.success_rate_30d", pct(ok30, ok30 + f30))
            self.m("ctl.jobs_success_30d", ok30)
        except ApiError:
            pass
        try:
            self.m("ctl.workflow_jobs_24h", a.count(c + "/workflow_jobs/", {"finished__gt": s24}))
        except ApiError:
            pass
        # Pending / waiting now (stuck jobs)
        pend = list(a.iter(c + "/unified_jobs/", {"status__in": "pending,waiting", "order_by": "created"},
                           max_items=2000))
        oldest_min = None
        if pend:
            created = parse_dt(pend[0].get("created"))
            oldest_min = round((self.now - created).total_seconds() / 60.0, 1) if created else None
        self.m("ctl.pending", len(pend))
        self.m("ctl.oldest_pending_min", oldest_min)
        self.add("controller.pending", "controller", "No stuck pending jobs",
                 "ok" if not pend else grade_high(oldest_min or 0, t["pending_warn_min"], t["pending_fail_min"]),
                 "%d pending" % len(pend),
                 "oldest waiting %s min" % oldest_min if pend else "queue empty",
                 "Pending jobs mean no capacity or a blocked dependency: python/05_aap_stuck_job_finder.py",
                 "major", "aap_cluster")
        running = list(a.iter(c + "/unified_jobs/", {"status": "running"}, max_items=2000))
        long_ = []
        for j in running:
            st_ = parse_dt(j.get("started"))
            if st_ and (self.now - st_).total_seconds() > t["long_running_h"] * 3600:
                long_.append("%s #%s (%.1fh)" % (j.get("name"), j.get("id"),
                                                  (self.now - st_).total_seconds() / 3600))
        self.m("ctl.long_running", len(long_))
        self.add("controller.long_running", "controller", "No runaway jobs (> %sh)" % t["long_running_h"],
                 "warn" if long_ else "ok", "%d" % len(long_), "; ".join(long_[:8]) or "none",
                 "Set job template timeouts; python/05_aap_stuck_job_finder.py", "minor", "aap_monitor")

    def controller_content(self):
        t, a, c = self.t, self.aap, self.ctl
        bad_proj = [p.get("name") for p in a.iter(c + "/projects/", {"status__in": "failed,error"}, max_items=500)]
        self.m("ctl.projects_failed", len(bad_proj))
        self.add("controller.projects", "controller", "Project syncs healthy",
                 "warn" if bad_proj else "ok", "%d failed" % len(bad_proj), ", ".join(bad_proj[:10]) or "none",
                 "Check SCM credentials and reachability from the task pods", "minor", "aap_monitor")
        bad_inv = [i.get("name") for i in a.iter(c + "/inventory_sources/", {"status__in": "failed,error"},
                                                  max_items=500)]
        self.m("ctl.inventory_sources_failed", len(bad_inv))
        self.add("controller.inventory_sources", "controller", "Inventory source syncs healthy",
                 "warn" if bad_inv else "ok", "%d failed" % len(bad_inv), ", ".join(bad_inv[:10]) or "none",
                 "Check cloud credentials / source plugins", "minor", "aap_monitor")
        missed, enabled = [], 0
        grace = timedelta(minutes=t["missed_schedule_grace_min"])
        sched_by_ujt = defaultdict(list)
        for sc in a.iter(c + "/schedules/", {"enabled": "true"}, max_items=self.s["max_objects"]):
            enabled += 1
            sched_by_ujt[sc.get("unified_job_template")].append(sc)
            nr = parse_dt(sc.get("next_run"))
            if nr and nr < self.now - grace:
                missed.append(sc.get("name"))
        self.m("ctl.schedules_enabled", enabled)
        self.m("ctl.schedules_missed", len(missed))
        self.add("controller.schedules", "controller", "Schedules firing on time",
                 "warn" if missed else "ok", "%d missed of %d" % (len(missed), enabled),
                 ", ".join(missed[:10]) or "no enabled schedule has a next_run in the past",
                 "A next_run in the past means the dispatcher/scheduler is not running them", "major",
                 "aap_monitor")
        # Built-in cleanup management jobs keep the database small (Red Hat default schedules)
        sjt = list(a.iter(c + "/system_job_templates/", max_items=50))
        unscheduled = [s.get("name") for s in sjt
                       if s.get("job_type") in ("cleanup_jobs", "cleanup_activitystream", "cleanup_sessions",
                                                "cleanup_tokens")
                       and not sched_by_ujt.get(s.get("id"))]
        failed_sys = a.count(c + "/system_jobs/", {"finished__gt": iso(self.now - timedelta(days=7)),
                                                    "status__in": "failed,error"})
        st = "warn" if (unscheduled or failed_sys) else ("ok" if sjt else "unknown")
        self.add("controller.cleanup_jobs", "controller", "Cleanup management jobs scheduled and passing", st,
                 "%d unscheduled, %d failed (7d)" % (len(unscheduled), failed_sys),
                 ", ".join(unscheduled) or "job, activity stream, session and token cleanup scheduled",
                 "Re-enable the default management job schedules; unbounded job events grow the database",
                 "minor", "aap_topology")
        for key, path in (("job_templates", "/job_templates/"), ("workflow_templates", "/workflow_job_templates/"),
                          ("inventories", "/inventories/"), ("hosts", "/hosts/"), ("projects", "/projects/"),
                          ("credentials", "/credentials/"), ("execution_environments", "/execution_environments/")):
            try:
                self.m("ctl." + key, a.count(c + path))
            except ApiError:
                pass

    def controller_license(self):
        t = self.t
        cfg = self.aap.json(self.ctl + "/config/")
        li = cfg.get("license_info") or {}
        self.info["subscription"] = li.get("subscription_name") or li.get("product_name")
        self.info["controller_version"] = self.info.get("controller_version") or cfg.get("version")
        remaining = li.get("time_remaining")
        days = round(float(remaining) / 86400.0, 1) if remaining not in (None, "") else None
        self.m("ctl.license_days_left", days)
        compliant = li.get("compliant")
        st = grade_low(days, t["license_days_warn"], t["license_days_fail"])
        if compliant is False or li.get("date_expired"):
            st = "fail"
        self.add("controller.subscription", "controller", "Subscription valid and compliant", st,
                 "%s days left" % days if days is not None else "n/a",
                 "compliant=%s, %s" % (compliant, li.get("subscription_name") or "subscription"),
                 "Renew or refresh the subscription manifest under Settings > Subscription", "critical",
                 "aap_lifecycle")
        licensed = li.get("instance_count")
        used = li.get("current_instances") if li.get("current_instances") is not None else li.get("automated_instances")
        self.m("ctl.hosts_licensed", licensed)
        self.m("ctl.hosts_used", used)
        usage = pct(used or 0, licensed) if licensed else None
        self.m("ctl.host_usage_pct", usage)
        self.add("controller.host_usage", "controller", "Managed hosts within subscription",
                 grade_high(usage, t["host_usage_warn_pct"], t["host_usage_fail_pct"]),
                 "%s / %s" % (used, licensed) if licensed else "n/a", "%s%% of licensed nodes used" % usage
                 if usage is not None else "", "Delete stale hosts (host metrics) or expand the subscription",
                 "major", "aap_lifecycle")

    # ======================================================================= #
    # Event-Driven Ansible
    # ======================================================================= #
    def eda_checks(self):
        t, a, e = self.t, self.aap, self.eda
        try:
            data, ms = a.get(e + "/status/")
            good = str((data or {}).get("status", "")).lower() == "good"
            st = "ok" if good else "fail"
            if good:
                st = grade_high(ms, t["api_latency_warn_ms"], t["api_latency_fail_ms"])
            self.m("eda.latency_ms", ms)
            self.add("eda.status", "eda", "EDA status endpoint", st, (data or {}).get("status", "error"),
                     "%s ms" % ms if ms else "HTTP 500 - database or Redis connection failure",
                     "A slow or 500 response points at the EDA database/Redis; "
                     "python/26_aap_eda_dispatcher_health_checker.py", "critical", "aap_eda_api")
        except ApiError as exc:
            self.add("eda.status", "eda", "EDA status endpoint", "fail", "error", short(exc),
                     "python/26_aap_eda_dispatcher_health_checker.py", "critical", "aap_eda_api")
        acts = list(a.iter(e + "/activations/", max_items=self.s["max_objects"]))
        enabled = [x for x in acts if x.get("is_enabled")]
        running = [x for x in enabled if x.get("status") == "running"]
        broken, degraded, restarts = [], [], []
        for x in enabled:
            stx = x.get("status")
            if stx in ("failed", "error", "unresponsive", "workers offline"):
                broken.append("%s=%s" % (x.get("name"), stx))
            elif stx not in ("running", "starting", "pending"):
                degraded.append("%s=%s" % (x.get("name"), stx))
            if int(x.get("restart_count") or 0) >= t["eda_restart_warn"]:
                restarts.append("%s (%s)" % (x.get("name"), x.get("restart_count")))
        self.m("eda.activations", len(acts))
        self.m("eda.activations_enabled", len(enabled))
        self.m("eda.activations_running", len(running))
        self.m("eda.activations_unhealthy", len(broken) + len(degraded))
        self.add("eda.activations", "eda", "Enabled rulebook activations running",
                 "fail" if broken else ("warn" if degraded else "ok"),
                 "%d/%d running" % (len(running), len(enabled)), "; ".join(broken + degraded) or "all running",
                 "python/24_aap_eda_activation_status_checker.py; check the activation-worker pods",
                 "major", "aap_eda_api")
        self.add("eda.restarts", "eda", "Activations not restart-looping", "warn" if restarts else "ok",
                 "%d" % len(restarts), ", ".join(restarts[:10]) or "none",
                 "Review the rulebook process logs for the activation", "minor", "aap_eda_api")
        failed24 = 0
        for inst in a.iter(e + "/activation-instances/", {"status": "failed"}, max_items=5000):
            ts = parse_dt(inst.get("ended_at") or inst.get("started_at"))
            if ts and ts >= self.since24:
                failed24 += 1
        self.m("eda.failed_instances_24h", failed24)
        self.add("eda.failed_instances", "eda", "No failed rulebook processes (24h)",
                 "warn" if failed24 else "ok", "%d" % failed24, "", "Inspect activation-instance logs",
                 "minor", "aap_eda_api")
        fired = 0
        try:
            for r in a.iter(e + "/audit-rules/", max_items=20000):
                ts = parse_dt(r.get("fired_at"))
                if ts and ts >= self.since24:
                    fired += 1
        except ApiError:
            fired = None
        self.m("eda.rules_fired_24h", fired)
        bad_proj = [p.get("name") for p in a.iter(e + "/projects/", max_items=1000)
                    if p.get("import_state") == "failed"]
        self.m("eda.projects_failed", len(bad_proj))
        self.add("eda.projects", "eda", "EDA project imports healthy", "warn" if bad_proj else "ok",
                 "%d failed" % len(bad_proj), ", ".join(bad_proj[:10]) or "none",
                 "Re-sync the project; check the SCM credential", "minor", "aap_eda_api")
        for key, path in (("decision_environments", "/decision-environments/"),
                          ("event_streams", "/event-streams/")):
            try:
                self.m("eda." + key, a.count(e + path))
            except ApiError:
                pass
        self.details["eda_activations"] = [
            {"name": x.get("name"), "enabled": x.get("is_enabled"), "status": x.get("status"),
             "restarts": x.get("restart_count"), "message": short(x.get("status_message"), 120)}
            for x in acts[:200]]

    # ======================================================================= #
    # Automation hub
    # ======================================================================= #
    def hub_checks(self):
        t, a, h = self.t, self.aap, self.hub
        st_, ms = a.get(h + "/pulp/api/v3/status/")
        self.m("hub.latency_ms", ms)
        workers = st_.get("online_workers") or []
        apps = st_.get("online_content_apps") or []
        db = (st_.get("database_connection") or {}).get("connected")
        redis = (st_.get("redis_connection") or {}).get("connected")
        problems = []
        if not workers:
            problems.append("no online workers")
        if not apps:
            problems.append("no online content apps")
        if db is False:
            problems.append("database disconnected")
        if redis is False:
            problems.append("redis disconnected")
        self.m("hub.workers", len(workers))
        self.m("hub.content_apps", len(apps))
        self.add("hub.status", "hub", "Hub (Pulp) status: workers, content apps, DB, Redis",
                 "fail" if problems else "ok", "%d workers, %d content apps" % (len(workers), len(apps)),
                 "; ".join(problems) or "database and redis connected",
                 "Check the hub-worker / hub-content pods; python/21_aap_hub_sync_task_checker.py",
                 "critical", "aap_monitor")
        storage = st_.get("storage") or {}
        if storage.get("total"):
            used_pct = pct(storage.get("used") or 0, storage["total"])
            self.m("hub.storage_used_pct", used_pct)
            self.m("hub.storage_free_gb", gib(storage.get("free")))
            self.add("hub.storage", "hub", "Hub artifact storage free",
                     grade_high(used_pct, t["hub_storage_warn_pct"], t["hub_storage_fail_pct"]),
                     "%s%% used" % used_pct, "%s GiB free" % gib(storage.get("free")),
                     "Expand the hub file-storage PVC or run orphan cleanup", "major", "aap_perf_ocp")
        tasks = h + "/pulp/api/v3/tasks/"
        failed = a.count(tasks, {"state": "failed", "finished_at__gte": iso(self.since24)}, style="pulp")
        self.m("hub.tasks_failed_24h", failed)
        self.add("hub.tasks_failed", "hub", "No failed hub tasks (24h)", "warn" if failed else "ok",
                 "%d" % failed, "sync, import and signing tasks",
                 "python/21_aap_hub_sync_task_checker.py", "minor", "aap_monitor")
        waiting = list(a.iter(tasks, {"state": "waiting", "ordering": "pulp_created"}, page_size=50,
                              max_items=500, style="pulp"))
        oldest = None
        if waiting:
            created = parse_dt(waiting[0].get("pulp_created"))
            oldest = round((self.now - created).total_seconds() / 60.0, 1) if created else None
        self.m("hub.tasks_waiting", len(waiting))
        self.add("hub.tasks_waiting", "hub", "Hub task queue draining",
                 "warn" if oldest and oldest >= t["hub_waiting_task_warn_min"] else "ok",
                 "%d waiting" % len(waiting), "oldest %s min" % oldest if oldest else "",
                 "Waiting tasks with workers online usually mean a stuck worker - restart hub-worker",
                 "minor", "aap_monitor")
        for key, path in (("collections", "/v3/plugin/ansible/search/collection-versions/"),
                          ("ee_repositories", "/v3/plugin/execution-environments/repositories/")):
            try:
                self.m("hub." + key, a.count(h + path, style="pulp"))
            except ApiError:
                pass

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
        conns = self.metrics.get("ctl.db_connections")
        if conns is not None:
            p_ = pct(conns, t["operator_db_max_connections"])
            self.m("ctl.db_connections_pct", p_)
            if p_ >= t["db_connections_warn_pct"]:
                reasons.append("controller DB connections %d of %d" % (conns, t["operator_db_max_connections"]))
        cap = sum(int(r.get("capacity") or 0) for r in self.details.get("instance_groups") or []
                  if not r.get("container_group"))
        if cap > t["operator_db_max_concurrent_jobs"]:
            reasons.append("controller capacity %d > %d" % (cap, t["operator_db_max_concurrent_jobs"]))
        size = self.info.get("db_pvc_gib")
        if size and size > t["operator_db_max_gb"]:
            reasons.append("database PVC %s GiB > %s" % (size, t["operator_db_max_gb"]))
        self.add("platform.operator_db_limits", "platform", "Operator-managed database within supported limits",
                 "warn" if reasons else "ok", "operator-managed", "; ".join(reasons) or
                 "max_connections 100 / 100 GB / 1 replica / 100 concurrent jobs not exceeded",
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
# Scoring, snapshot, history
# --------------------------------------------------------------------------- #
def score_checks(checks):
    num_, den = 0.0, 0.0
    for c in checks:
        if c["status"] in STATUS_VALUE:
            w = SEVERITY_WEIGHT.get(c["severity"], 1)
            num_ += STATUS_VALUE[c["status"]] * w
            den += w
    return round(num_ * 100.0 / den, 1) if den else None


def rag(checks, collected):
    if not collected:
        return "unknown"
    if any(c["status"] == "fail" and c["severity"] == "critical" for c in checks):
        return "red"
    # Amber: any failure, or a warning on a critical/major check. Minor warnings alone stay green
    # (they are still listed as open issues for engineers).
    if any(c["status"] == "fail" or (c["status"] == "warn" and c["severity"] != "minor") for c in checks):
        return "amber"
    return "green"


def build_snapshot(env, col, now, settings):
    checks = sorted(col.checks, key=lambda c: (STATUS_RANK[c["status"]], -SEVERITY_WEIGHT[c["severity"]],
                                               [k for k, _ in COMPONENTS].index(c["component"])
                                               if c["component"] in COMPONENT_LABEL else 9, c["id"]))
    counts = Counter(c["status"] for c in checks)
    collected = any(c["status"] != "unknown" for c in checks)
    score = score_checks(checks)
    m = dict(col.metrics)
    m["health_score"] = score
    for s_ in ("ok", "warn", "fail", "unknown"):
        m["checks_" + s_] = counts.get(s_, 0)
    m["open_critical"] = sum(1 for c in checks if c["status"] == "fail" and c["severity"] == "critical")
    if m.get("ctl.jobs_success_30d") is not None:
        m["est_hours_saved_30d"] = round(m["ctl.jobs_success_30d"] * settings["minutes_saved_per_job"] / 60.0, 1)
    if m.get("ctl.jobs_success_24h") is not None:
        m["est_hours_saved_24h"] = round(m["ctl.jobs_success_24h"] * settings["minutes_saved_per_job"] / 60.0, 1)
    comp_status = {}
    for key, _ in COMPONENTS:
        cs = [c["status"] for c in checks if c["component"] == key]
        if cs:
            comp_status[key] = {"status": worst([s for s in cs if s != "unknown"]) if any(
                s != "unknown" for s in cs) else "unknown", "ok": cs.count("ok"), "warn": cs.count("warn"),
                "fail": cs.count("fail"), "unknown": cs.count("unknown")}
    return {"name": env["name"], "label": env.get("label", env["name"]), "tier": env.get("tier", ""),
            "collected_at": iso(now), "status": rag(checks, collected), "score": score,
            "components": comp_status, "checks": checks, "metrics": m, "details": col.details,
            "info": col.info, "errors": col.errors}


_RX_URL = re.compile(r"https?://[^\s\"'<>]+")
_RX_IP = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_RX_FQDN = re.compile(r"\b(?:[a-zA-Z0-9-]+\.){2,}[a-zA-Z]{2,}\b")


def redact_value(v):
    if isinstance(v, str):
        v = _RX_URL.sub("<url>", v)
        v = _RX_IP.sub("<ip>", v)
        v = _RX_FQDN.sub(lambda mm: mm.group(0).split(".")[0] + ".<domain>", v)
        return v
    if isinstance(v, list):
        return [redact_value(x) for x in v]
    if isinstance(v, tuple):
        return tuple(redact_value(x) for x in v)
    if isinstance(v, dict):
        return {k: redact_value(x) for k, x in v.items()}
    return v


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1, sort_keys=True, default=str)
    os.replace(tmp, path)


def previous_snapshot(out, today):
    snaps = sorted(p for p in (Path(out) / "data" / "snapshots").glob("*.json") if p.stem < today)
    return load_json(snaps[-1], {}) if snaps else {}


def update_history(out, today, snapshots, keep_days):
    hist = load_json(Path(out) / "history.json", {"version": 1, "days": {}})
    hist.setdefault("days", {})
    day = hist["days"].setdefault(today, {})
    for s in snapshots:
        day[s["name"]] = {"status": s["status"], "score": s["score"], "metrics": s["metrics"],
                          "label": s["label"], "tier": s["tier"]}
    cutoff = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    hist["days"] = {d: v for d, v in sorted(hist["days"].items()) if d >= cutoff}
    hist["updated"] = today
    write_json(Path(out) / "history.json", hist)
    return hist


def series(hist, env, key, dates):
    out = []
    for d in dates:
        e = (hist["days"].get(d) or {}).get(env)
        if not e:
            out.append(None)
        elif key in ("score", "status"):
            out.append(e.get(key))
        else:
            out.append((e.get("metrics") or {}).get(key))
    return out


def green_days(hist, env, dates):
    vals = [s for s in series(hist, env, "status", dates) if s]
    return (sum(1 for s in vals if s == "green"), len(vals))


# --------------------------------------------------------------------------- #
# HTML rendering (static, no JavaScript)
# --------------------------------------------------------------------------- #
CSS = """
:root{--bg:#f6f7f9;--card:#fff;--ink:#1c2230;--muted:#5d6678;--line:#e1e5ec;
--good:#17794a;--warn:#a96a00;--bad:#b83227;--neutral:#5d6678;--goodbg:#e4f4ea;--warnbg:#fdf0d5;--badbg:#fbe6e3;--neutralbg:#eceff3;
--s0:#1b66c9;--s1:#c4600c;--s2:#7a43b6;--s3:#0f8a8a;--s4:#b8327a;--s5:#5b6b00;--link:#1b66c9}
@media (prefers-color-scheme:dark){:root{--bg:#10141c;--card:#181e29;--ink:#e8ecf3;--muted:#9aa4b6;--line:#2a3243;
--good:#5fd29a;--warn:#f0b950;--bad:#ff8b7d;--neutral:#9aa4b6;--goodbg:#173626;--warnbg:#3b2f12;--badbg:#401e1a;--neutralbg:#232b3a;
--s0:#6aa6f5;--s1:#f0944a;--s2:#b48cf0;--s3:#4fd1d1;--s4:#f07ab8;--s5:#c3d64a;--link:#8ab8ff}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
a{color:var(--link)}.wrap{max-width:1280px;margin:0 auto;padding:20px 20px 48px}
header{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:flex-end;gap:12px;margin-bottom:16px}
h1{font-size:22px;margin:0}h2{font-size:16px;margin:0 0 4px}h3{font-size:14px;margin:12px 0 2px}.sub{color:var(--muted);font-size:12.5px}
.chips{display:flex;gap:8px;flex-wrap:wrap}.chip{border:1px solid var(--line);background:var(--card);border-radius:999px;padding:4px 12px;font-size:12.5px}
.chip b{font-weight:600}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px;background:var(--neutral)}
.dot.green{background:var(--good)}.dot.amber{background:var(--warn)}.dot.red{background:var(--bad)}
.banner{border-radius:8px;padding:10px 14px;margin-bottom:14px;border:1px solid var(--line)}
.banner.bad{background:var(--badbg);border-color:var(--bad)}.banner.warn{background:var(--warnbg);border-color:var(--warn)}
.banner ul{margin:4px 0 0 18px;padding:0}
.hero{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.hero .card .v{font-size:27px;font-weight:650;line-height:1.1}.hero .card .l{color:var(--muted);font-size:12.5px;margin-top:4px}
.hero .card .t{color:var(--muted);font-size:11.5px;margin-top:2px}
.v.good{color:var(--good)}.v.warn{color:var(--warn)}.v.bad{color:var(--bad)}
section.card{margin-bottom:16px}.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(400px,1fr));gap:16px}
.grid3{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin-top:8px}
.tile{border:1px solid var(--line);border-radius:8px;padding:10px 12px;border-left:5px solid var(--neutral)}
.tile.ok{border-left-color:var(--good)}.tile.warn{border-left-color:var(--warn)}.tile.fail{border-left-color:var(--bad)}
.tile b{display:block;margin-bottom:2px}
table{width:100%;border-collapse:collapse;margin-top:8px}th,td{padding:6px 8px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:12px;color:var(--muted);font-weight:600}td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:0}td:first-child{white-space:nowrap}td.small{font-size:12.5px;color:var(--muted);max-width:420px}
.pill{display:inline-block;border-radius:999px;padding:1px 9px;font-size:12px;font-weight:600;white-space:nowrap}
.pill.ok,.pill.green{background:var(--goodbg);color:var(--good)}.pill.warn,.pill.amber{background:var(--warnbg);color:var(--warn)}
.pill.fail,.pill.red{background:var(--badbg);color:var(--bad)}.pill.unknown{background:var(--neutralbg);color:var(--neutral)}
.sev{font-size:11.5px;color:var(--muted);text-transform:uppercase;letter-spacing:.03em}
details{border:1px solid var(--line);border-radius:8px;margin-top:10px;padding:2px 12px;background:var(--card)}
summary{cursor:pointer;padding:8px 0;font-weight:600}.muted{color:var(--muted)}
svg text{fill:var(--muted);font-size:11px}svg .grid{stroke:var(--line)}svg .tgt{stroke:var(--muted);stroke-dasharray:4 4;fill:none}
""" + "".join("svg .l%d{stroke:var(--s%d);fill:none;stroke-width:2}svg .p%d{fill:var(--s%d)}.sw%d{background:var(--s%d)}\n"
              % (i, i, i, i, i, i) for i in range(6)) + """
.key{display:flex;flex-wrap:wrap;gap:6px 14px;font-size:12.5px;margin-top:4px}.sw{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}
footer{color:var(--muted);font-size:12px;margin-top:20px}footer p{margin:4px 0}
.scroll{overflow-x:auto}.scroll.tall{max-height:380px;overflow:auto;border-top:1px solid var(--line)}
.scroll.tall th{position:sticky;top:0;background:var(--card);box-shadow:0 1px 0 var(--line)}
.spark polyline{stroke:var(--s0);fill:none;stroke-width:1.6}
"""

STATUS_LABEL = {"ok": "OK", "warn": "Warning", "fail": "Fail", "unknown": "Unknown",
                "green": "Green", "amber": "Amber", "red": "Red"}


def esc(x):
    return html.escape("" if x is None else str(x))


def num(x, suffix=""):
    if x is None:
        return "&mdash;"
    if isinstance(x, float):
        if math.isnan(x):
            return "&mdash;"
        if x == int(x) and abs(x) < 1e15:
            return "{:,}{}".format(int(x), suffix)
        return "{:,.1f}{}".format(x, suffix)
    return "{:,}{}".format(x, suffix)


def pill(status):
    return '<span class="pill %s">%s</span>' % (esc(status), STATUS_LABEL.get(status, esc(status)))


def nice_max(v):
    if v <= 5:
        return 5
    mag = 10 ** (len(str(int(v))) - 1)
    for mm in (1, 2, 2.5, 5, 10):
        if v <= mm * mag:
            return mm * mag
    return 10 * mag


def svg_lines(labels, series_, width=620, height=220, ymax=None, ymin=0, unit="", target=None):
    """ymin="auto" zooms the y axis to the data (in steps of 10) - used for percentage KPIs."""
    """series_: list of (name, index, values). Gaps (None) are skipped."""
    pl, pr, pt, pb = 50, 14, 12, 28
    vals = [v for _, _, vs in series_ for v in vs if v is not None]
    top = ymax if ymax else nice_max(max(vals + [1] + ([target] if target else [])))
    if ymin == "auto":
        lo = min(vals + ([target] if target is not None else [])) if vals else 0
        bot = max(0, int(math.floor((lo - 5) / 10.0)) * 10)
    else:
        bot = ymin
    rng = (top - bot) or 1
    iw, ih = width - pl - pr, height - pt - pb
    n = max(len(labels), 2)
    X = lambda i: pl + iw * i / (n - 1)  # noqa: E731
    Y = lambda v: pt + ih - ih * (min(max(v, bot), top) - bot) / rng  # noqa: E731
    out = ['<svg viewBox="0 0 %d %d" width="100%%" role="img" aria-label="trend chart">' % (width, height)]
    for frac in (0, 0.5, 1):
        y = pt + ih - ih * frac
        out.append('<line class="grid" x1="%d" x2="%d" y1="%.1f" y2="%.1f"/>' % (pl, width - pr, y, y))
        out.append('<text x="%d" y="%.1f" text-anchor="end">%s%s</text>' % (pl - 6, y + 4, num(bot + rng * frac), unit))
    if target is not None:
        out.append('<line class="tgt" x1="%d" x2="%d" y1="%.1f" y2="%.1f"><title>target %s%s</title></line>'
                   % (pl, width - pr, Y(target), Y(target), target, unit))
    if labels:
        for i in sorted({0, len(labels) // 2, len(labels) - 1}):
            out.append('<text x="%.1f" y="%d" text-anchor="middle">%s</text>' % (X(i), height - 8, esc(labels[i][5:])))
    for name, idx, vs in series_:
        segs, cur = [], []
        for i, v in enumerate(vs):
            if v is None:
                if cur:
                    segs.append(cur)
                cur = []
            else:
                cur.append((X(i), Y(v)))
        if cur:
            segs.append(cur)
        for seg in segs:
            if len(seg) > 1:
                out.append('<polyline class="l%d" points="%s"/>' % (idx % 6, " ".join("%.1f,%.1f" % p for p in seg)))
        r = 3 if len(labels) <= 45 else 1.8
        for i, v in enumerate(vs):
            if v is not None:
                out.append('<circle class="p%d" cx="%.1f" cy="%.1f" r="%s"><title>%s %s: %s%s</title></circle>'
                           % (idx % 6, X(i), Y(v), r, esc(name), esc(labels[i]), num(v), unit))
    out.append("</svg>")
    return "".join(out)


def sparkline(values, width=110, height=26, top=100):
    pts = [(i, v) for i, v in enumerate(values) if v is not None]
    if len(pts) < 2:
        return '<span class="muted">&mdash;</span>'
    n = max(len(values) - 1, 1)
    p = " ".join("%.1f,%.1f" % (2 + (width - 4) * i / n, height - 2 - (height - 4) * min(v, top) / top) for i, v in pts)
    return '<svg class="spark" viewBox="0 0 %d %d" width="%d" height="%d"><polyline points="%s"/></svg>' % (
        width, height, width, height, p)


def key(names):
    return '<div class="key">%s</div>' % "".join(
        '<span><span class="sw sw%d"></span>%s</span>' % (i % 6, esc(n)) for i, n in enumerate(names))


def table(headers, rows, num_cols=(), tall=False):
    h = "".join('<th class="%s">%s</th>' % ("n" if i in num_cols else "", t) for i, t in enumerate(headers))
    body = "".join("<tr>%s</tr>" % "".join(
        '<td class="%s">%s</td>' % ("n" if i in num_cols else "", c) for i, c in enumerate(r)) for r in rows)
    if not rows:
        body = '<tr><td colspan="%d" class="muted">No data</td></tr>' % len(headers)
    return '<div class="scroll%s"><table><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div>' % (
        " tall" if tall else "", h, body)


def card(value, label, cls="", target=""):
    return '<div class="card"><div class="v %s">%s</div><div class="l">%s</div>%s</div>' % (
        cls, value, label, '<div class="t">%s</div>' % target if target else "")


def vs_target(value, target, higher_is_better=True):
    if value is None or target is None:
        return ""
    ok = value >= target if higher_is_better else value <= target
    return "good" if ok else "bad"


def src_link(key_):
    title, url = SOURCES.get(key_, ("", ""))
    if not url:
        return ""
    return '<a href="%s" title="%s" rel="noopener">Ref</a>' % (esc(url), esc(title))


def page(title, body, root=""):
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" '
            'content="width=device-width,initial-scale=1"><title>%s</title><style>%s</style></head>'
            '<body><div class="wrap">%s</div></body></html>' % (esc(title), CSS, body))


def kpi_rows(snaps, hist, dates30, targets):
    """Management KPI matrix: (label, target text, [cell per env])."""
    def cell(v, tgt=None, hib=True, suffix=""):
        if v is None:
            return '<span class="muted">&mdash;</span>'
        cls = vs_target(v, tgt, hib) if tgt is not None else ""
        cls = {"good": "ok", "bad": "fail"}.get(cls, "")
        return '<span class="pill %s">%s</span>' % (cls, num(v, suffix)) if cls else num(v, suffix)
    rows = []
    M = lambda s, k: s["metrics"].get(k)  # noqa: E731
    rows.append(("Health score", ">= %s" % targets["health_score"],
                 [cell(s["score"], targets["health_score"]) for s in snaps]))
    rows.append(("Job success rate, 24h", ">= %s%%" % targets["job_success_pct"],
                 [cell(M(s, "ctl.success_rate_24h"), targets["job_success_pct"], True, "%") for s in snaps]))
    rows.append(("Job success rate, 30d", ">= %s%%" % targets["job_success_pct"],
                 [cell(M(s, "ctl.success_rate_30d"), targets["job_success_pct"], True, "%") for s in snaps]))
    rows.append(("Jobs run, 30d", "", [cell(M(s, "ctl.jobs_30d")) for s in snaps]))
    rows.append(("Est. hours saved, 30d", "estimate", [cell(M(s, "est_hours_saved_30d")) for s in snaps]))
    gd = []
    for s in snaps:
        g, n = green_days(hist, s["name"], dates30)
        gd.append(cell(pct(g, n), targets["green_days_pct"], True, "%") + (' <span class="muted">(%d/%d)</span>' % (g, n)
                                                                          if n else ""))
    rows.append(("Green days, last 30", ">= %s%%" % targets["green_days_pct"], gd))
    rows.append(("Peak capacity used", "<= %s%%" % targets["capacity_used_pct"],
                 [cell(M(s, "ctl.capacity_used_pct"), targets["capacity_used_pct"], False, "%") for s in snaps]))
    rows.append(("Managed hosts / licensed", "",
                 ["%s / %s" % (num(M(s, "ctl.hosts_used")), num(M(s, "ctl.hosts_licensed"))) for s in snaps]))
    rows.append(("Subscription days left", ">= %s" % targets["license_days_left"],
                 [cell(M(s, "ctl.license_days_left"), targets["license_days_left"]) for s in snaps]))
    rows.append(("Active users, 30d", "", [cell(M(s, "gw.users_active_30d")) for s in snaps]))
    rows.append(("EDA activations running / enabled", "",
                 ["%s / %s" % (num(M(s, "eda.activations_running")), num(M(s, "eda.activations_enabled")))
                  for s in snaps]))
    rows.append(("EDA rules fired, 24h", "", [cell(M(s, "eda.rules_fired_24h")) for s in snaps]))
    rows.append(("Last good backup age (h)", "<= %s" % targets["backup_age_h"],
                 [cell(M(s, "ocp.backup_age_h"), targets["backup_age_h"], False) for s in snaps]))
    rows.append(("Min route certificate days", ">= %s" % targets["cert_min_days"],
                 [cell(M(s, "ocp.cert_min_days"), targets["cert_min_days"]) for s in snaps]))
    rows.append(("Open critical issues", "= %s" % targets["open_critical"],
                 [cell(M(s, "open_critical"), targets["open_critical"], False) for s in snaps]))
    return rows


def render_index(snaps, hist, cfg, now_local, args):
    targets, settings = cfg["kpi_targets"], cfg["settings"]
    all_dates = sorted(hist["days"])
    dates = all_dates[-args.chart_days:]
    dates30 = all_dates[-30:]
    title = cfg.get("dashboard_title", "AAP 2.6 Platform Health")
    P = []
    chips = "".join('<span class="chip"><span class="dot %s"></span><b>%s</b> %s</span>' % (
        s["status"], esc(s["label"]), esc(s["tier"])) for s in snaps)
    P.append('<header><div><h1>%s</h1><div class="sub">Daily health of Ansible Automation Platform 2.6 on OpenShift '
             '&middot; generated %s &middot; %d environments &middot; %d days of history</div></div>'
             '<div class="chips">%s</div></header>' % (esc(title), esc(now_local.strftime("%Y-%m-%d %H:%M %Z")),
                                                         len(snaps), len(all_dates), chips))
    problems = ["<b>%s</b> %s: %s" % (esc(s["label"]), esc(e["section"]), esc(e["error"])) for s in snaps
                for e in s["errors"]]
    if problems:
        P.append('<div class="banner warn"><b>Data collection problems</b> - some checks are reported as Unknown.'
                 '<ul>%s</ul></div>' % "".join("<li>%s</li>" % p for p in problems[:12]))
    st = Counter(s["status"] for s in snaps)
    scores = [s["score"] for s in snaps if s["score"] is not None]
    avg = round(statistics.mean(scores), 1) if scores else None
    jobs24 = sum(s["metrics"].get("ctl.jobs_24h") or 0 for s in snaps)
    succ = sum(s["metrics"].get("ctl.jobs_success_24h") or 0 for s in snaps)
    failed = sum(s["metrics"].get("ctl.jobs_failed_24h") or 0 for s in snaps)
    fleet_rate = pct(succ, succ + failed)
    hours = sum(s["metrics"].get("est_hours_saved_30d") or 0 for s in snaps)
    crit = sum(s["metrics"].get("open_critical") or 0 for s in snaps)
    P.append('<div class="hero">%s</div>' % "".join([
        card("%d / %d / %d" % (st["green"], st["amber"], st["red"]), "Environments green / amber / red",
             "bad" if st["red"] else ("warn" if st["amber"] else "good")),
        card(num(avg), "Average health score", vs_target(avg, targets["health_score"]),
             "target %s" % targets["health_score"]),
        card(num(fleet_rate, "%"), "Job success rate, 24h (all environments)",
             vs_target(fleet_rate, targets["job_success_pct"]), "target %s%%" % targets["job_success_pct"]),
        card(num(jobs24), "Jobs run in the last 24h"),
        card(num(round(hours)), "Estimated hours saved, 30d", "",
             "%s min per successful job (estimate)" % settings["minutes_saved_per_job"]),
        card(num(crit), "Open critical issues", "bad" if crit else "good", "target 0"),
    ]))
    # Scorecard
    rows = []
    for s in snaps:
        g, n = green_days(hist, s["name"], dates30)
        issues = sum(1 for c in s["checks"] if c["status"] in ("fail", "warn"))
        rows.append(['<a href="env/%s.html"><b>%s</b></a>' % (esc(s["name"]), esc(s["label"])), esc(s["tier"]),
                     pill(s["status"]), num(s["score"]), sparkline(series(hist, s["name"], "score", dates30)),
                     num(s["metrics"].get("ctl.success_rate_24h"), "%"), num(s["metrics"].get("ctl.jobs_24h")),
                     num(s["metrics"].get("ctl.capacity_used_pct"), "%"),
                     "%s / %s" % (num(g), num(n)), num(issues),
                     esc(s["info"].get("controller_version") or "n/a"), esc(s["info"].get("ocp_version") or "n/a")])
    P.append('<section class="card"><h2>Environment scorecard</h2><div class="sub">Status is Red when any critical check '
             'fails, Amber when any check fails or a critical/major check warns, otherwise Green. Select an environment for engineering detail.'
             '</div>%s</section>' % table(["Environment", "Tier", "Status", "Score", "Score, 30d", "Success 24h",
                                            "Jobs 24h", "Capacity used", "Green days", "Open issues",
                                            "Controller", "OpenShift"], rows, (3, 5, 6, 7, 8, 9)))
    # KPI matrix
    names = [s["label"] for s in snaps]
    krows = [[esc(lbl), '<span class="muted">%s</span>' % esc(tgt)] + cells
             for lbl, tgt, cells in kpi_rows(snaps, hist, dates30, targets)]
    P.append('<section class="card"><h2>Key performance indicators vs target</h2><div class="sub">For management review. '
             'Green pill = target met, red pill = target missed.</div>%s</section>' % table(
                 ["KPI", "Target"] + [esc(n) for n in names], krows))
    # Trends
    def chart(title_, key_, unit="", ymax=None, target=None, sub="", ymin=0):
        sers = [(s["label"], i, series(hist, s["name"], key_, dates)) for i, s in enumerate(snaps)]
        return '<section class="card"><h2>%s</h2><div class="sub">%s</div>%s%s</section>' % (
            esc(title_), esc(sub), svg_lines(dates, sers, ymax=ymax, ymin=ymin, unit=unit, target=target), key(names))
    P.append('<div class="grid2">%s%s%s%s%s%s</div>' % (
        chart("Health score", "score", "", 100, targets["health_score"], "Weighted share of checks passing; dashed = target", "auto"),
        chart("Job success rate, 24h", "ctl.success_rate_24h", "%", 100, targets["job_success_pct"],
              "Successful / (successful + failed + error)", "auto"),
        chart("Jobs per day", "ctl.jobs_24h", "", None, None, "Jobs finished in the 24h before each run"),
        chart("Estimated hours saved per day", "est_hours_saved_24h", "h", None, None,
              "Successful jobs x %s min" % settings["minutes_saved_per_job"]),
        chart("Active users, 30d", "gw.users_active_30d", "", None, None, "Users who logged in within 30 days"),
        chart("Peak instance group capacity used", "ctl.capacity_used_pct", "%", 100, targets["capacity_used_pct"],
              "Highest non-container instance group"),
    ))
    # Fleet issues
    issues = []
    for s in snaps:
        for c in s["checks"]:
            if c["status"] in ("fail", "warn"):
                issues.append((STATUS_RANK[c["status"]], -SEVERITY_WEIGHT[c["severity"]], s, c))
    issues.sort(key=lambda x: (x[0], x[1], x[2]["label"]))
    P.append('<section class="card"><h2>Open issues across environments</h2><div class="sub">Failures first, then '
             'warnings, by severity.</div>%s</section>' % table(
                 ["Environment", "Status", "Severity", "Component", "Check", "Value", "What to do"],
                 [['<a href="env/%s.html">%s</a>' % (esc(s["name"]), esc(s["label"])), pill(c["status"]),
                   '<span class="sev">%s</span>' % esc(c["severity"]), esc(COMPONENT_LABEL.get(c["component"])),
                   esc(c["title"]), esc(c["value"]), '<span class="muted">%s</span>' % esc(c["hint"] or c["detail"])]
                  for _, _, s, c in issues[:80]], tall=True))
    P.append(footer(cfg, settings, args))
    return page(title, "".join(P))


def footer(cfg, settings, args):
    srcs = " &middot; ".join('<a href="%s" rel="noopener">%s</a>' % (esc(u), esc(t))
                            for k, (t, u) in SOURCES.items() if u)
    return ('<footer><p>aap_health_dashboard.py v%s. Read-only: every call is an HTTP GET with a read-only token. '
            'Data: <a href="%sdata/latest.json">latest.json</a> &middot; <a href="%sdata/checks_latest.csv">checks CSV</a> '
            '&middot; <a href="%shistory.json">history.json</a>.%s</p><p>Hours saved is an estimate: successful jobs x %s '
            'minutes. Health checks follow Red Hat guidance: %s</p></footer>' % (
                VERSION, args._root, args._root, args._root,
                " Hostnames and URLs are redacted." if args.redact else "", settings["minutes_saved_per_job"], srcs))


def render_env(s, hist, cfg, now_local, args):
    t = cfg["kpi_targets"]
    dates = sorted(hist["days"])[-args.chart_days:]
    M = s["metrics"]
    info = s["info"]
    P = ['<div class="sub"><a href="../index.html">&larr; All environments</a></div>']
    chips = []
    for lbl, k in (("Controller", "controller_version"), ("Operator", "operator_version"),
                   ("OpenShift", "ocp_version"), ("Database", "database"), ("Redis", "redis_mode"),
                   ("API user", "api_user")):
        if info.get(k):
            chips.append('<span class="chip"><b>%s</b> %s</span>' % (lbl, esc(info[k])))
    P.append('<header><div><h1><span class="dot %s"></span>%s</h1><div class="sub">%s &middot; collected %s</div></div>'
             '<div class="chips">%s</div></header>' % (s["status"], esc(s["label"]), esc(s["tier"]),
                                                         esc(now_local.strftime("%Y-%m-%d %H:%M %Z")), "".join(chips)))
    if s["errors"]:
        P.append('<div class="banner warn"><b>Collection problems</b><ul>%s</ul></div>' % "".join(
            "<li>%s: %s</li>" % (esc(e["section"]), esc(e["error"])) for e in s["errors"]))
    P.append('<div class="hero">%s</div>' % "".join([
        card(num(s["score"]), "Health score", vs_target(s["score"], t["health_score"]), "target %s" % t["health_score"]),
        card("%d / %d / %d" % (M.get("checks_ok", 0), M.get("checks_warn", 0), M.get("checks_fail", 0)),
             "Checks ok / warn / fail", "", "%d unknown" % M.get("checks_unknown", 0)),
        card(num(M.get("ctl.success_rate_24h"), "%"), "Job success, 24h",
             vs_target(M.get("ctl.success_rate_24h"), t["job_success_pct"]), "%s jobs" % num(M.get("ctl.jobs_24h"))),
        card(num(M.get("ctl.capacity_used_pct"), "%"), "Peak capacity used",
             vs_target(M.get("ctl.capacity_used_pct"), t["capacity_used_pct"], False)),
        card(num(M.get("ctl.pending")), "Jobs pending now", "warn" if M.get("ctl.pending") else "good",
             "oldest %s min" % num(M.get("ctl.oldest_pending_min")) if M.get("ctl.pending") else ""),
        card(num(M.get("ctl.event_lag_s"), "s"), "Job event lag"),
        card(num(M.get("ocp.pod_restarts_24h")), "Pod restarts since last run",
             "warn" if M.get("ocp.pod_restarts_24h") else "good"),
        card(num(M.get("ocp.backup_age_h"), "h"), "Last good backup", vs_target(M.get("ocp.backup_age_h"),
                                                                              t["backup_age_h"], False)),
    ]))
    tiles = []
    for k, lbl in COMPONENTS:
        c = s["components"].get(k)
        if not c:
            continue
        tiles.append('<div class="tile %s"><b>%s</b>%s <span class="muted">%d ok, %d warn, %d fail, %d unknown</span></div>'
                     % (c["status"], esc(lbl), pill(c["status"]), c["ok"], c["warn"], c["fail"], c["unknown"]))
    P.append('<section class="card"><h2>Components</h2><div class="grid3">%s</div></section>' % "".join(tiles))
    for k, lbl in COMPONENTS:
        rows = [[pill(c["status"]), '<span class="sev">%s</span>' % esc(c["severity"]), esc(c["title"]), esc(c["value"]),
                 '<span class="muted">%s</span>' % esc(c["detail"]), '<span class="muted">%s</span>' % esc(c["hint"]),
                 src_link(c.get("source"))] for c in s["checks"] if c["component"] == k]
        if rows:
            P.append('<section class="card"><h2>%s checks</h2>%s</section>' % (esc(lbl), table(
                ["Status", "Severity", "Check", "Value", "Detail", "What to do", "Guidance"], rows)))

    def chart(title_, keys, unit="", ymax=None, target=None, ymin=0):
        sers = [(lbl, i, series(hist, s["name"], k_, dates)) for i, (lbl, k_) in enumerate(keys)]
        if not any(v is not None for _, _, vs in sers for v in vs):
            return ""
        return '<section class="card"><h2>%s</h2>%s%s</section>' % (
            esc(title_), svg_lines(dates, sers, ymax=ymax, ymin=ymin, unit=unit, target=target),
            key([lbl for lbl, _ in keys]))
    charts = [
        chart("Health score", [("score", "score")], "", 100, t["health_score"], "auto"),
        chart("Jobs per day", [("finished", "ctl.jobs_24h"), ("failed", "ctl.jobs_failed_24h")]),
        chart("Job success rate", [("24h", "ctl.success_rate_24h"), ("30d", "ctl.success_rate_30d")], "%", 100,
              t["job_success_pct"], "auto"),
        chart("Capacity and queue", [("capacity used %", "ctl.capacity_used_pct"), ("pending jobs", "ctl.pending")]),
        chart("Job duration (s)", [("average", "ctl.job_avg_s"), ("p95", "ctl.job_p95_s")], "s"),
        chart("API latency (ms)", [("gateway", "gw.latency_ms"), ("controller", "ctl.latency_ms"),
                                   ("EDA", "eda.latency_ms"), ("hub", "hub.latency_ms")], "ms"),
        chart("Job event lag (s)", [("callback lag", "ctl.event_lag_s")], "s"),
        chart("Redis callback queue", [("events queued", "ctl.callback_queue")]),
        chart("Pods", [("restarts", "ocp.pod_restarts_24h"), ("not ready", "ocp.pods_not_ready"),
                       ("warning events", "ocp.warning_events_24h")]),
        chart("Storage used (%)", [("hub storage", "hub.storage_used_pct"), ("max PVC", "ocp.pvc_max_used_pct")],
              "%", 100),
        chart("Event-Driven Ansible", [("running activations", "eda.activations_running"),
                                       ("failed processes", "eda.failed_instances_24h"),
                                       ("rules fired", "eda.rules_fired_24h")]),
        chart("AAP namespace usage", [("CPU cores", "ocp.cpu_cores"), ("memory GiB", "ocp.mem_gib")]),
        chart("Node headroom (%)", [("max CPU", "ocp.node_cpu_max_pct"), ("max memory", "ocp.node_mem_max_pct")],
              "%", 100),
        chart("etcd p99 latency (ms)", [("WAL fsync", "ocp.etcd_fsync_p99_ms"), ("backend commit",
                                                                                  "ocp.etcd_commit_p99_ms"),
                                        ("peer RTT", "ocp.etcd_peer_rtt_p99_ms")], "ms"),
        chart("Cluster alerts", [("critical", "ocp.alerts_critical"), ("warning", "ocp.alerts_warning")]),
    ]
    P.append('<h2 style="margin:18px 0 8px">Trends (%d days)</h2><div class="grid2">%s</div>' % (
        len(dates), "".join(c for c in charts if c)))
    D = s["details"]

    def det(title_, headers, rows, nums=()):
        return '<details><summary>%s (%d)</summary>%s</details>' % (esc(title_), len(rows), table(
            headers, rows, nums, tall=True))
    parts = []
    parts.append(det("Instance groups", ["Name", "Container group", "Capacity", "Used", "Used %", "Running"],
                     [[esc(r["name"]), esc(r["container_group"]), num(r["capacity"]), num(r["used"]),
                       num(r["used_pct"], "%"), num(r["running"])] for r in D.get("instance_groups") or []], (2, 3, 4, 5)))
    parts.append(det("Instances", ["Hostname", "Type", "State", "Capacity", "Used", "Remaining %", "Jobs", "Last health check"],
                     [[esc(r["hostname"]), esc(r["type"]), esc(r["state"]), num(r["capacity"]), num(r["used"]),
                       num(r["remaining_pct"]), num(r["jobs_running"]), esc(r["last_health_check"])]
                      for r in D.get("instances") or []], (3, 4, 5, 6)))
    parts.append(det("Top failing job templates, 24h", ["Template", "Failures"],
                     [[esc(n), num(c)] for n, c in D.get("top_failing_templates") or []], (1,)))
    parts.append(det("Launch types, 24h", ["Launch type", "Jobs"],
                     [[esc(k_), num(v)] for k_, v in sorted((D.get("launch_types") or {}).items(), key=lambda x: -x[1])],
                     (1,)))
    parts.append(det("EDA activations", ["Name", "Enabled", "Status", "Restarts", "Message"],
                     [[esc(r["name"]), esc(r["enabled"]), esc(r["status"]), num(r["restarts"]), esc(r["message"])]
                      for r in D.get("eda_activations") or []], (3,)))
    parts.append(det("Operator custom resources", ["Kind", "Name", "Status", "Detail", "Version"],
                     [[esc(r["kind"]), esc(r["name"]), pill(r["status"]), esc(r["detail"]), esc(r["version"])]
                      for r in D.get("component_crs") or []]))
    parts.append(det("Pods", ["Pod", "Component", "Phase", "Ready", "Restarts", "New restarts", "Node"],
                     [[esc(r["pod"]), esc(r["component"]), esc(r["phase"]), esc(r["ready"]), num(r["restarts"]),
                       num(r["restarts_delta"]), esc(r["node"])] for r in D.get("pods") or []], (4, 5)))
    parts.append(det("Container resource usage (top 40 by memory)", ["Pod", "Container", "CPU (m)", "Memory (MiB)",
                                                                    "% of memory limit"],
                     [[esc(r["pod"]), esc(r["container"]), num(r["cpu_m"]), num(r["mem_mib"]), num(r["mem_limit_pct"], "%")]
                      for r in D.get("usage") or []], (2, 3, 4)))
    parts.append(det("Persistent volume claims", ["PVC", "Phase", "Capacity (GiB)", "Access modes", "Storage class", "Used %"],
                     [[esc(r["pvc"]), esc(r["phase"]), num(r["capacity_gib"]), esc(r["modes"]), esc(r["storage_class"]),
                       num(r["used_pct"], "%")] for r in D.get("pvcs") or []], (2, 5)))
    parts.append(det("Route certificates", ["Route", "Host", "Days left", "Expires", "Issuer"],
                     [[esc(r["route"]), esc(r["host"]), num(r.get("days_left")), esc(r.get("not_after") or r.get("error")),
                       esc(r.get("issuer"))] for r in D.get("certs") or []], (2,)))
    parts.append(det("Warning event reasons, 24h", ["Reason", "Count"],
                     [[esc(k_), num(v)] for k_, v in D.get("event_reasons") or []], (1,)))
    parts.append(det("Cluster nodes", ["Node", "Roles", "Ready", "Pressure", "Unschedulable", "Hosts AAP", "CPU %",
                                       "Memory %", "Kubelet"],
                     [[esc(r["node"]), esc(r["roles"]), esc(r["ready"]), esc(r["pressure"]), esc(r["unschedulable"]),
                       esc(r["aap"]), num(r.get("cpu_pct"), "%"), num(r.get("mem_pct"), "%"), esc(r["kubelet"])]
                      for r in D.get("nodes") or []], (6, 7)))
    al = D.get("alerts") or {}
    parts.append(det("Firing alerts", ["Severity", "Alert"],
                     [["critical", esc(a)] for a in al.get("critical") or []] +
                     [["warning", esc(a)] for a in al.get("warning") or []]))
    P.append('<section class="card"><h2>Detail for platform engineers and support</h2>%s</section>' % "".join(parts))
    P.append(footer(cfg, cfg["settings"], args))
    return page("%s - AAP health" % s["label"], "".join(P))


def write_csv(path, snaps):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["environment", "tier", "component", "check_id", "title", "status", "severity", "value", "detail",
                "hint", "guidance_url"])
    for s in snaps:
        for c in s["checks"]:
            w.writerow([s["name"], s["tier"], c["component"], c["id"], c["title"], c["status"], c["severity"],
                        c["value"], c["detail"], c["hint"], SOURCES.get(c.get("source"), ("", ""))[1]])
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(buf.getvalue(), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Config and main
# --------------------------------------------------------------------------- #
def load_config(path):
    with open(path, encoding="utf-8") as fh:
        cfg = json.load(fh)
    envs = cfg.get("environments") or []
    if not envs:
        raise ValueError("config has no environments")
    seen = set()
    for e in envs:
        if not e.get("name") or not re.match(r"^[a-z0-9][a-z0-9_-]*$", e["name"]):
            raise ValueError("each environment needs a lowercase name (letters, digits, - or _): %r" % e.get("name"))
        if e["name"] in seen:
            raise ValueError("duplicate environment name %s" % e["name"])
        seen.add(e["name"])
        if not e.get("aap_url") and not e.get("ocp_api_url"):
            raise ValueError("environment %s needs aap_url and/or ocp_api_url" % e["name"])
    cfg["thresholds"] = dict(DEFAULT_THRESHOLDS, **(cfg.get("thresholds") or {}))
    cfg["kpi_targets"] = dict(DEFAULT_KPI_TARGETS, **(cfg.get("kpi_targets") or {}))
    cfg["settings"] = dict(DEFAULT_SETTINGS, **(cfg.get("settings") or {}))
    for k in DEFAULT_SETTINGS:
        if k in cfg:
            cfg["settings"][k] = cfg[k]
    return cfg


def report_now(cfg, as_of):
    tzname = cfg.get("report_tz", "UTC")
    tz = UTC
    try:
        from zoneinfo import ZoneInfo  # Python 3.9+
        tz = ZoneInfo(tzname)
    except Exception:  # noqa: BLE001
        tzname = "UTC"
    if as_of:
        local = datetime.strptime(as_of, "%Y-%m-%d").replace(hour=22, tzinfo=tz)
    else:
        local = datetime.now(tz)
    return local.astimezone(UTC), local


def collect_env(env, cfg, now, prev, quiet):
    t = dict(cfg["thresholds"], **(env.get("thresholds") or {}))
    settings = dict(cfg["settings"])
    for k in ("cluster_checks", "check_route_certs", "minutes_saved_per_job"):
        if k in env:
            settings[k] = env[k]
    col = Collector(env, settings, t, now, prev, quiet).run()
    return build_snapshot(env, col, now, settings)


def serve(out, port):
    import functools
    import http.server
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(out))
    log("Serving %s on http://0.0.0.0:%d/ (Ctrl+C to stop)" % (out, port))
    http.server.ThreadingHTTPServer(("0.0.0.0", port), handler).serve_forever()


def main(argv=None):
    ap = argparse.ArgumentParser(description="AAP 2.6 on OpenShift daily health dashboard (read-only)")
    ap.add_argument("-c", "--config", default="environments.json")
    ap.add_argument("-o", "--out", default="site", help="output directory (the GitHub Pages root)")
    ap.add_argument("-e", "--env", action="append", help="only these environment names (repeatable)")
    ap.add_argument("--as-of", help="YYYY-MM-DD: label the run with this date (tests / backfill)")
    ap.add_argument("--chart-days", type=int, default=90, help="days shown on trend charts (default 90)")
    ap.add_argument("--keep-days", type=int, default=400, help="days kept in history and snapshots")
    ap.add_argument("--redact", action="store_true", help="mask hostnames, URLs and IPs in published output")
    ap.add_argument("--no-cluster", action="store_true", help="skip cluster-scope OpenShift checks")
    ap.add_argument("--fail-on", choices=["fail", "warn"], help="exit 3 when an environment is red (fail) or amber+")
    ap.add_argument("--workers", type=int, default=4, help="environments collected in parallel")
    ap.add_argument("--serve", type=int, metavar="PORT", help="serve the output directory after writing it")
    ap.add_argument("-q", "--quiet", action="store_true")
    ap.add_argument("--version", action="version", version=VERSION)
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.config)
    except (OSError, ValueError) as exc:
        log("config error: %s" % exc)
        return 2
    if args.no_cluster:
        cfg["settings"]["cluster_checks"] = False
    if args.redact or cfg.get("redact"):
        args.redact = True
    envs = [e for e in cfg["environments"] if not args.env or e["name"] in args.env]
    if not envs:
        log("no matching environments")
        return 2
    out = Path(args.out)
    now, now_local = report_now(cfg, args.as_of)
    today = now_local.strftime("%Y-%m-%d")
    prev = previous_snapshot(out, today)
    prev_envs = {e["name"]: e for e in prev.get("environments") or []}
    log("AAP health dashboard %s - %s - %d environments" % (VERSION, today, len(envs)), args.quiet)
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futs = [pool.submit(collect_env, e, cfg, now, prev_envs.get(e["name"]), args.quiet) for e in envs]
        snaps = [f.result() for f in futs]
    if args.redact:
        snaps = [dict(redact_value(s), details=dict(redact_value(s["details"]),
                                                    pod_restarts=s["details"].get("pod_restarts", {})))
                 for s in snaps]
    snapshot = {"date": today, "generated": iso(now), "version": VERSION, "environments": snaps}
    write_json(out / "data" / "latest.json", snapshot)
    write_json(out / "data" / "snapshots" / ("%s.json" % today), snapshot)
    cutoff = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=args.keep_days)).strftime("%Y-%m-%d")
    for p in (out / "data" / "snapshots").glob("*.json"):
        if p.stem < cutoff:
            p.unlink()
    hist = update_history(out, today, snaps, args.keep_days)
    # environments not in this run (filtered) still show if present in history? keep to configured order
    args._root = ""
    (out / "index.html").write_text(render_index(snaps, hist, cfg, now_local, args), encoding="utf-8")
    args._root = "../"
    for s in snaps:
        (out / "env").mkdir(parents=True, exist_ok=True)
        (out / "env" / ("%s.html" % s["name"])).write_text(render_env(s, hist, cfg, now_local, args), encoding="utf-8")
    write_csv(out / "data" / "checks_latest.csv", snaps)
    (out / ".nojekyll").write_text("", encoding="utf-8")
    collected = [s for s in snaps if s["status"] != "unknown"]
    for s in snaps:
        log("  %-14s %-7s score %-5s  ok %d  warn %d  fail %d  unknown %d" % (
            s["name"], s["status"], s["score"], s["metrics"]["checks_ok"], s["metrics"]["checks_warn"],
            s["metrics"]["checks_fail"], s["metrics"]["checks_unknown"]), args.quiet)
    log("Wrote %s" % (out / "index.html"), args.quiet)
    if args.fail_on:
        bad = {"fail": ("red",), "warn": ("red", "amber")}[args.fail_on]
        if any(s["status"] in bad for s in snaps):
            code = 3
        else:
            code = None
    else:
        code = None
    if args.serve:
        serve(out, args.serve)
    if code:
        return code
    if not collected:
        return 2
    if len(collected) < len(snaps) or any(s["errors"] for s in snaps):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
