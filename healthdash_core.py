#!/usr/bin/env python3
"""
healthdash_core.py - shared library for the AAP 2.6 / OpenShift health dashboards.

Not run directly. Two independent scripts use it:

  aap_health_dashboard.py   Ansible Automation Platform data only (platform gateway, automation
                            controller, Event-Driven Ansible and automation hub APIs) -> <out>/aap/
  ocp_health_dashboard.py   OpenShift data only (OpenShift API + cluster monitoring: AAP namespace,
                            operator, storage, backups, routes, nodes, etcd, alerts) -> <out>/openshift/

Both write <out>/index.html, a landing page built from whatever each has published, so the
scripts can run on different schedules, from different runners and with different tokens.

What lives here: read-only HTTP client (GET only, bearer token only), secret lookup, the
check/metric bookkeeping base class, scoring and Green/Amber/Red status, history and snapshot
files, redaction, the static HTML/SVG renderer (no JavaScript, no CDN) and the shared CLI.

Requirements: Python 3.9+, standard library only.
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
import sys
import tempfile
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import error, parse, request


VERSION = "2.0.0"
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
    # Run only environments whose tier is listed (e.g. ["prod"]). Empty = every enabled environment.
    "only_tiers": [],
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
# Collector base - one instance per environment, subclassed by each dashboard
# --------------------------------------------------------------------------- #
class BaseCollector:
    """Holds the checks, metrics and details collected for one environment.

    Subclasses build their own read-only Http clients in setup() and implement run().
    """

    def __init__(self, env, settings, thresholds, now, previous, quiet=False):
        self.env, self.s, self.t, self.now = env, settings, thresholds, now
        self.prev = previous or {}
        self.quiet = quiet
        self.checks, self.metrics, self.details, self.errors = [], {}, {}, []
        self.info = {}
        self.verify = env.get("verify_tls", True)
        self.ca = env.get("ca_bundle")
        self.timeout = int(env.get("timeout", 30))
        self.ns = env.get("namespace", "aap")
        self.cr = env.get("aap_cr_name", "aap")
        self.op_ns = env.get("operator_namespace", self.ns)
        self.since24 = now - timedelta(hours=24)
        self.since30 = now - timedelta(days=30)
        self.setup()

    def setup(self):
        pass

    def run(self):
        raise NotImplementedError

    # ---- bookkeeping -------------------------------------------------------- #

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


def build_snapshot(env, col, now, settings, headline=None):
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
            "info": col.info, "errors": col.errors, "headline": headline(col, m, settings) if headline else []}


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


def green_days(hist, env, dates, key_="status"):
    vals = [s for s in series(hist, env, key_, dates) if s]
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
.nav{display:flex;flex-wrap:wrap;gap:4px;border-bottom:1px solid var(--line);margin-bottom:16px}
.nav a{padding:8px 14px;text-decoration:none;color:var(--muted);border:1px solid transparent;border-bottom:0;
border-radius:8px 8px 0 0;margin-bottom:-1px;font-size:13px}
.nav a.on{background:var(--card);border-color:var(--line);color:var(--ink);font-weight:600}
.dom .domhead{display:flex;justify-content:space-between;align-items:center;gap:8px}
.dom .domscore{display:flex;align-items:center;gap:10px;margin:8px 0 2px}.dom .domscore .v{font-size:34px;font-weight:650}
.dom .v.good{color:var(--good)}.dom .v.warn{color:var(--warn)}.dom .v.bad{color:var(--bad)}
table.facts td{padding:5px 0}table.facts td.n{padding-left:12px}
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


# --------------------------------------------------------------------------- #
# Site layout. Each dashboard script writes only its own folder; the landing
# page is rebuilt by whichever script runs, from the data both have published.
#   <out>/index.html                  landing: one panel per dashboard
#   <out>/aap/index.html              Ansible Automation Platform dashboard   (aap_health_dashboard.py)
#   <out>/aap/env/<name>.html         AAP engineering detail
#   <out>/openshift/index.html        OpenShift dashboard                     (ocp_health_dashboard.py)
#   <out>/openshift/env/<name>.html   OpenShift engineering detail
#   <out>/<dashboard>/history.json, data/latest.json, data/checks_latest.csv, data/snapshots/
# --------------------------------------------------------------------------- #
SITE_SECTIONS = (("aap", "Ansible Automation Platform", "aap_health_dashboard.py"),
                 ("openshift", "OpenShift", "ocp_health_dashboard.py"))

NAV_CSS = """
.nav{display:flex;flex-wrap:wrap;gap:4px;border-bottom:1px solid var(--line);margin-bottom:16px}
.nav a{padding:8px 14px;text-decoration:none;color:var(--muted);border:1px solid transparent;border-bottom:0;
border-radius:8px 8px 0 0;margin-bottom:-1px;font-size:13px}
.nav a.on{background:var(--card);border-color:var(--line);color:var(--ink);font-weight:600}
.dom .domhead{display:flex;justify-content:space-between;align-items:center;gap:8px}
.dom .domscore{display:flex;align-items:center;gap:10px;margin:8px 0 2px}.dom .domscore .v{font-size:34px;font-weight:650}
.dom .v.good{color:var(--good)}.dom .v.warn{color:var(--warn)}.dom .v.bad{color:var(--bad)}
table.facts td{padding:5px 0}table.facts td.n{padding-left:12px}
"""


class Dashboard:
    """Describes one dashboard (AAP or OpenShift). Subclasses fill in the hooks."""
    key = ""            # "aap" | "openshift" (folder name)
    label = ""          # page title
    short = ""          # short name used in KPI labels
    blurb = ""          # one-line description of the data source
    components = ()     # COMPONENTS keys this dashboard owns
    requires = ()       # environment keys that must be set for an environment to be checked
    collector = None    # BaseCollector subclass
    env_chips = ()      # ((label, info key), ...)
    extra_settings = ()  # per-environment keys copied into settings

    def headline(self, col, m, settings):  # [(label, display value)] for the landing page
        return []

    def hero_cards(self, snaps, cfg):
        return []

    scorecard_headers = ()
    scorecard_nums = ()

    def scorecard_cells(self, s, cfg):
        return []

    def kpi_rows(self, snaps, hist, dates30, cfg):
        return []

    def charts(self, cfg):  # [(title, metric key, unit, ymax, target, subtitle, ymin)]
        return []

    def env_cards(self, s, cfg):
        return []

    def env_charts(self, cfg):  # [(title, [(series label, metric key)], unit, ymax, target, ymin)]
        return []

    def env_details(self, s):  # [(title, headers, rows, numeric column indexes)]
        return []

    def add_arguments(self, ap):
        pass

    def apply_arguments(self, args, cfg):
        pass


def kcell(v, tgt=None, hib=True, suffix=""):
    """KPI cell: green / red pill when a target exists."""
    if v is None:
        return '<span class="muted">&mdash;</span>'
    cls = {"good": "ok", "bad": "fail"}.get(vs_target(v, tgt, hib) if tgt is not None else "", "")
    return '<span class="pill %s">%s</span>' % (cls, num(v, suffix)) if cls else num(v, suffix)


def status_card(snaps, label):
    if len(snaps) == 1:
        st = snaps[0]["status"]
        return card(STATUS_LABEL.get(st, st), label, {"green": "good", "amber": "warn", "red": "bad"}.get(st, ""))
    st = Counter(s["status"] for s in snaps)
    return card("%d / %d / %d" % (st["green"], st["amber"], st["red"]), label + " (green / amber / red)",
                "bad" if st["red"] else ("warn" if st["amber"] else "good"))


def green_cell(hist, s, dates30, target):
    g, n = green_days(hist, s["name"], dates30)
    return kcell(pct(g, n), target, True, "%") + (' <span class="muted">(%d/%d)</span>' % (g, n) if n else "")


def common_kpis(spec, snaps, hist, dates30, cfg):
    t = cfg["kpi_targets"]
    return [("%s health score" % spec.short, ">= %s" % t["health_score"],
             [kcell(s["score"], t["health_score"]) for s in snaps]),
            ("Green days, last 30", ">= %s%%" % t["green_days_pct"],
             [green_cell(hist, s, dates30, t["green_days_pct"]) for s in snaps])]


def nav(out, active, root, spec=None, snaps=()):
    links = [("landing", "index.html", "Overview")]
    for key_, label, _ in SITE_SECTIONS:
        if (spec and key_ == spec.key) or (Path(out) / key_ / "data" / "latest.json").exists():
            links.append((key_, key_ + "/index.html", label))
    for s in snaps:
        links.append(("env-" + s["name"], "%s/env/%s.html" % (spec.key, s["name"]),
                      "%s %s detail" % (s["label"], spec.short)))
    return '<nav class="nav">%s</nav>' % "".join(
        '<a class="%s" href="%s%s">%s</a>' % ("on" if k == active else "", root, href, esc(lbl))
        for k, href, lbl in links)


def page_header(title_html, sub_html, chips_html=""):
    return ('<header><div><h1>%s</h1><div class="sub">%s</div></div><div class="chips">%s</div></header>'
            % (title_html, sub_html, chips_html))


def footer(cfg, settings, args, data_root, script):
    srcs = " &middot; ".join('<a href="%s" rel="noopener">%s</a>' % (esc(u), esc(t))
                            for k, (t, u) in SOURCES.items() if u)
    data = ('Data: <a href="%sdata/latest.json">latest.json</a> &middot; <a href="%sdata/checks_latest.csv">checks CSV</a>'
            ' &middot; <a href="%shistory.json">history.json</a>. ' % (data_root, data_root, data_root)) if data_root is not None else ""
    hours = ("Hours saved is an estimate: successful jobs x %s minutes. " % settings["minutes_saved_per_job"]
             if script != "ocp_health_dashboard.py" else "")
    return ('<footer><p>%s v%s. Read-only: every call is an HTTP GET with a read-only token. %s%s</p>'
            '<p>%sHealth checks follow Red Hat guidance: %s</p></footer>' % (
                esc(script), VERSION, data, "Hostnames and URLs are redacted." if args.redact else "", hours, srcs))


def page(title, body, root=""):
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" '
            'content="width=device-width,initial-scale=1"><title>%s</title><style>%s%s</style></head>'
            '<body><div class="wrap">%s</div></body></html>' % (esc(title), CSS, NAV_CSS, body))


def render_summary(spec, snaps, hist, cfg, now_local, args):
    """Management page for one dashboard: status, KPIs vs target, trends, open issues."""
    settings = cfg["settings"]
    all_dates = sorted(hist["days"])
    dates, dates30 = all_dates[-args.chart_days:], all_dates[-30:]
    title = "%s - %s" % (cfg.get("dashboard_title", "AAP 2.6 Platform Health"), spec.label)
    chips = "".join('<span class="chip"><span class="dot %s"></span><b>%s</b> %s</span>' % (
        s["status"], esc(s["label"]), esc(s["tier"])) for s in snaps)
    P = [nav(args.out, spec.key, "../", spec, snaps),
         page_header(esc(spec.label), "%s &middot; generated %s &middot; %d days of history" % (
             esc(spec.blurb), esc(now_local.strftime("%Y-%m-%d %H:%M %Z")), len(all_dates)), chips)]
    problems = ["<b>%s</b> %s: %s" % (esc(s["label"]), esc(e["section"]), esc(e["error"])) for s in snaps
                for e in s["errors"]]
    if problems:
        P.append('<div class="banner warn"><b>Data collection problems</b> - some checks are reported as Unknown.'
                 '<ul>%s</ul></div>' % "".join("<li>%s</li>" % p for p in problems[:12]))
    P.append('<div class="hero">%s</div>' % "".join(spec.hero_cards(snaps, cfg)))
    rows = []
    for s in snaps:
        g, n = green_days(hist, s["name"], dates30)
        issues = sum(1 for c in s["checks"] if c["status"] in ("fail", "warn"))
        rows.append(['<a href="env/%s.html"><b>%s</b></a>' % (esc(s["name"]), esc(s["label"])), esc(s["tier"]),
                     pill(s["status"]), num(s["score"]), sparkline(series(hist, s["name"], "score", dates30))]
                    + spec.scorecard_cells(s, cfg) + ["%s / %s" % (num(g), num(n)), num(issues)])
    head = ["Environment", "Tier", "Status", "Score", "Score, 30d"] + list(spec.scorecard_headers) + \
        ["Green days", "Open issues"]
    nums = (3,) + tuple(5 + i for i in spec.scorecard_nums) + (len(head) - 2, len(head) - 1)
    P.append('<section class="card"><h2>%s scorecard</h2><div class="sub">Status is Red when any critical check fails, '
             'Amber when any check fails or a critical/major check warns, otherwise Green. Select the environment for '
             'engineering detail.</div>%s</section>' % (esc(spec.short), table(head, rows, nums)))
    names = [s["label"] for s in snaps]
    krows = [[esc(lbl), '<span class="muted">%s</span>' % esc(tgt)] + cells
             for lbl, tgt, cells in spec.kpi_rows(snaps, hist, dates30, cfg)]
    P.append('<section class="card"><h2>Key performance indicators vs target</h2><div class="sub">For management '
             'review. Green pill = target met, red pill = target missed.</div>%s</section>' % table(
                 ["KPI", "Target"] + [esc(n) for n in names], krows))
    charts = []
    for title_, key_, unit, ymax, target, sub, ymin in spec.charts(cfg):
        sers = [(s["label"], i, series(hist, s["name"], key_, dates)) for i, s in enumerate(snaps)]
        charts.append('<section class="card"><h2>%s</h2><div class="sub">%s</div>%s%s</section>' % (
            esc(title_), esc(sub), svg_lines(dates, sers, ymax=ymax, ymin=ymin, unit=unit, target=target), key(names)))
    P.append('<div class="grid2">%s</div>' % "".join(charts))
    issues = [(STATUS_RANK[c["status"]], -SEVERITY_WEIGHT[c["severity"]], s, c) for s in snaps for c in s["checks"]
              if c["status"] in ("fail", "warn")]
    issues.sort(key=lambda x: (x[0], x[1], x[2]["label"]))
    P.append('<section class="card"><h2>Open %s issues</h2><div class="sub">Failures first, then warnings, by severity.'
             '</div>%s</section>' % (esc(spec.short), table(
                 ["Environment", "Status", "Severity", "Component", "Check", "Value", "What to do"],
                 [['<a href="env/%s.html">%s</a>' % (esc(s["name"]), esc(s["label"])), pill(c["status"]),
                   '<span class="sev">%s</span>' % esc(c["severity"]), esc(COMPONENT_LABEL.get(c["component"])),
                   esc(c["title"]), esc(c["value"]), '<span class="muted">%s</span>' % esc(c["hint"] or c["detail"])]
                  for _, _, s, c in issues[:80]], tall=True)))
    P.append(footer(cfg, settings, args, "", spec.script))
    return page(title, "".join(P))


def render_env(spec, s, hist, cfg, now_local, args):
    """Engineering / support detail for one environment of one dashboard."""
    t = cfg["kpi_targets"]
    dates = sorted(hist["days"])[-args.chart_days:]
    M, info = s["metrics"], s["info"]
    P = [nav(args.out, "env-" + s["name"], "../../", spec, args._snaps)]
    chips = "".join('<span class="chip"><b>%s</b> %s</span>' % (lbl, esc(info[k])) for lbl, k in spec.env_chips
                    if info.get(k))
    P.append(page_header('<span class="dot %s"></span>%s - %s' % (s["status"], esc(s["label"]), esc(spec.label)),
                         "%s &middot; engineering detail &middot; collected %s" % (
                             esc(s["tier"]), esc(now_local.strftime("%Y-%m-%d %H:%M %Z"))), chips))
    if s["errors"]:
        P.append('<div class="banner warn"><b>Collection problems</b><ul>%s</ul></div>' % "".join(
            "<li>%s: %s</li>" % (esc(e["section"]), esc(e["error"])) for e in s["errors"]))
    cards = [card(num(s["score"]), "%s health score" % spec.short, vs_target(s["score"], t["health_score"]),
                  "target %s" % t["health_score"]),
             card("%d / %d / %d" % (M.get("checks_ok", 0), M.get("checks_warn", 0), M.get("checks_fail", 0)),
                  "Checks ok / warn / fail", "", "%d unknown" % M.get("checks_unknown", 0))]
    P.append('<div class="hero">%s</div>' % "".join(cards + spec.env_cards(s, cfg)))
    tiles = []
    for k, lbl in COMPONENTS:
        c = s["components"].get(k)
        if k in spec.components and c:
            tiles.append('<div class="tile %s"><b>%s</b>%s <span class="muted">%d ok, %d warn, %d fail, %d unknown'
                         '</span></div>' % (c["status"], esc(lbl), pill(c["status"]), c["ok"], c["warn"], c["fail"],
                                            c["unknown"]))
    P.append('<section class="card"><h2>Components</h2><div class="grid3">%s</div></section>' % "".join(tiles))
    for k, lbl in COMPONENTS:
        rows = [[pill(c["status"]), '<span class="sev">%s</span>' % esc(c["severity"]), esc(c["title"]), esc(c["value"]),
                 '<span class="muted">%s</span>' % esc(c["detail"]), '<span class="muted">%s</span>' % esc(c["hint"]),
                 src_link(c.get("source"))] for c in s["checks"] if c["component"] == k]
        if rows:
            P.append('<section class="card"><h2>%s checks</h2>%s</section>' % (esc(lbl), table(
                ["Status", "Severity", "Check", "Value", "Detail", "What to do", "Guidance"], rows)))
    charts = []
    for title_, keys, unit, ymax, target, ymin in spec.env_charts(cfg):
        sers = [(lbl, i, series(hist, s["name"], k_, dates)) for i, (lbl, k_) in enumerate(keys)]
        if any(v is not None for _, _, vs in sers for v in vs):
            charts.append('<section class="card"><h2>%s</h2>%s%s</section>' % (
                esc(title_), svg_lines(dates, sers, ymax=ymax, ymin=ymin, unit=unit, target=target),
                key([lbl for lbl, _ in keys])))
    P.append('<h2 style="margin:18px 0 8px">Trends (%d days)</h2><div class="grid2">%s</div>' % (
        len(dates), "".join(charts)))
    parts = ['<details><summary>%s (%d)</summary>%s</details>' % (esc(title_), len(rows), table(
        headers, rows, nums, tall=True)) for title_, headers, rows, nums in spec.env_details(s)]
    P.append('<section class="card"><h2>Detail for platform engineers and support</h2>%s</section>' % "".join(parts))
    P.append(footer(cfg, cfg["settings"], args, "../", spec.script))
    return page("%s - %s health" % (s["label"], spec.label), "".join(P))


def render_landing(out, cfg, now_local, args):
    """Overview with one panel per dashboard, built from each dashboard's published data."""
    t = cfg["kpi_targets"]
    title = cfg.get("dashboard_title", "AAP 2.6 Platform Health")
    P = [nav(out, "landing", ""),
         page_header(esc(title), "Daily health of Ansible Automation Platform 2.6 and the OpenShift platform it runs on "
                     "&middot; updated %s" % esc(now_local.strftime("%Y-%m-%d %H:%M %Z")))]
    panels = []
    for key_, label, script in SITE_SECTIONS:
        latest = load_json(Path(out) / key_ / "data" / "latest.json", None)
        if not latest:
            panels.append('<section class="card dom"><div class="domhead"><h2>%s</h2>%s</div><p class="muted">Not '
                          'generated yet. Run <code>%s</code>.</p></section>' % (esc(label), pill("unknown"), script))
            continue
        hist = load_json(Path(out) / key_ / "history.json", {"days": {}})
        dates30 = sorted(hist.get("days", {}))[-30:]
        for s in latest["environments"]:
            g, n = green_days(hist, s["name"], dates30)
            facts = "".join('<tr><td>%s</td><td class="n">%s</td></tr>' % (esc(a), esc(b)) for a, b in s.get("headline") or [])
            panels.append(
                '<section class="card dom"><div class="domhead"><h2>%s</h2>%s</div>'
                '<div class="domscore"><span class="v %s">%s</span><span class="muted"> health score &middot; target %s'
                '</span>%s</div><div class="sub">%s &middot; %d ok, %d warn, %d fail &middot; green %s of last %s days '
                '&middot; %d open critical &middot; collected %s</div><table class="facts">%s</table>'
                '<p><a href="%s/index.html"><b>Open the %s dashboard</b></a> &middot; '
                '<a href="%s/env/%s.html">engineering detail</a></p></section>' % (
                    esc(label if len(latest["environments"]) == 1 else "%s - %s" % (label, s["label"])),
                    pill(s["status"]), vs_target(s["score"], t["health_score"]), num(s["score"]), t["health_score"],
                    sparkline(series(hist, s["name"], "score", dates30)), esc(s["label"]),
                    s["metrics"].get("checks_ok", 0), s["metrics"].get("checks_warn", 0),
                    s["metrics"].get("checks_fail", 0), num(g), num(n), s["metrics"].get("open_critical", 0),
                    esc(latest.get("date", "")), facts, key_, esc(label), key_, esc(s["name"])))
    P.append('<div class="grid2">%s</div>' % "".join(panels))
    P.append('<section class="card"><h2>How the dashboards are built</h2><div class="sub">Two independent scripts run '
             'every evening with read-only tokens. <b>aap_health_dashboard.py</b> uses only the AAP APIs (platform '
             'gateway, automation controller, Event-Driven Ansible, automation hub). <b>ocp_health_dashboard.py</b> uses '
             'only the OpenShift API and cluster monitoring (AAP namespace, operator, storage, backups, routes, nodes, '
             'etcd, alerts). Each has its own score, history and pages.</div></section>')
    P.append(footer(cfg, cfg["settings"], args, None, "aap_health_dashboard.py / ocp_health_dashboard.py"))
    return page(title, "".join(P))


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


def serve(out, port):
    import functools
    import http.server
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(out))
    log("Serving %s on http://0.0.0.0:%d/ (Ctrl+C to stop)" % (out, port))
    http.server.ThreadingHTTPServer(("0.0.0.0", port), handler).serve_forever()


def select_environments(cfg, spec, only):
    """Environments to check: --env wins; otherwise enabled ones, filtered by settings.only_tiers."""
    if only:
        envs = [e for e in cfg["environments"] if e["name"] in only]
    else:
        envs = [e for e in cfg["environments"] if e.get("enabled", True)]
        tiers = cfg["settings"].get("only_tiers")
        if tiers:
            envs = [e for e in envs if e.get("tier", e["name"]) in tiers]
    keep = []
    for e in envs:
        missing = [k for k in spec.requires if not e.get(k)]
        if missing:
            log("  skipping %s for %s: %s not set" % (e["name"], spec.label, ", ".join(missing)))
        else:
            keep.append(e)
    return keep


def collect_env(spec, env, cfg, now, prev, quiet):
    t = dict(cfg["thresholds"], **(env.get("thresholds") or {}))
    settings = dict(cfg["settings"])
    for k in ("minutes_saved_per_job",) + tuple(spec.extra_settings):
        if k in env:
            settings[k] = env[k]
    col = spec.collector(env, settings, t, now, prev, quiet).run()
    return build_snapshot(env, col, now, settings, spec.headline)


def run_dashboard(spec, argv=None):
    ap = argparse.ArgumentParser(description="%s daily health dashboard (read-only)" % spec.label)
    ap.add_argument("-c", "--config", default="environments.json")
    ap.add_argument("-o", "--out", default="site",
                    help="site root (the GitHub Pages root); this script writes <out>/%s/ and <out>/index.html" % spec.key)
    ap.add_argument("-e", "--env", action="append", help="only these environment names (repeatable)")
    ap.add_argument("--as-of", help="YYYY-MM-DD: label the run with this date (tests / backfill)")
    ap.add_argument("--chart-days", type=int, default=90, help="days shown on trend charts (default 90)")
    ap.add_argument("--keep-days", type=int, default=400, help="days kept in history and snapshots")
    ap.add_argument("--redact", action="store_true", help="mask hostnames, URLs and IPs in published output")
    ap.add_argument("--fail-on", choices=["fail", "warn"], help="exit 3 when an environment is red (fail) or amber+")
    ap.add_argument("--workers", type=int, default=4, help="environments collected in parallel")
    ap.add_argument("--serve", type=int, metavar="PORT", help="serve the site root after writing it")
    ap.add_argument("-q", "--quiet", action="store_true")
    ap.add_argument("--version", action="version", version=VERSION)
    spec.add_arguments(ap)
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.config)
    except (OSError, ValueError) as exc:
        log("config error: %s" % exc)
        return 2
    spec.apply_arguments(args, cfg)
    if args.redact or cfg.get("redact"):
        args.redact = True
    envs = select_environments(cfg, spec, args.env)
    if not envs:
        log("no environments to check for %s (check enabled / settings.only_tiers / --env / %s)" % (
            spec.label, ", ".join(spec.requires)))
        return 2
    root = Path(args.out)
    out = root / spec.key
    now, now_local = report_now(cfg, args.as_of)
    today = now_local.strftime("%Y-%m-%d")
    prev = previous_snapshot(out, today)
    prev_envs = {e["name"]: e for e in prev.get("environments") or []}
    log("%s %s - %s - %s" % (spec.script, VERSION, today, ", ".join(e["name"] for e in envs)), args.quiet)
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futs = [pool.submit(collect_env, spec, e, cfg, now, prev_envs.get(e["name"]), args.quiet) for e in envs]
        snaps = [f.result() for f in futs]
    if args.redact:
        snaps = [dict(redact_value(s), details=dict(redact_value(s["details"]),
                                                    pod_restarts=s["details"].get("pod_restarts", {})))
                 for s in snaps]
    snapshot = {"date": today, "generated": iso(now), "version": VERSION, "dashboard": spec.key,
                "environments": snaps}
    write_json(out / "data" / "latest.json", snapshot)
    write_json(out / "data" / "snapshots" / ("%s.json" % today), snapshot)
    cutoff = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=args.keep_days)).strftime("%Y-%m-%d")
    for p in (out / "data" / "snapshots").glob("*.json"):
        if p.stem < cutoff:
            p.unlink()
    hist = update_history(out, today, snaps, args.keep_days)
    args._snaps = snaps
    (out / "index.html").write_text(render_summary(spec, snaps, hist, cfg, now_local, args), encoding="utf-8")
    (out / "env").mkdir(parents=True, exist_ok=True)
    for s in snaps:
        (out / "env" / ("%s.html" % s["name"])).write_text(render_env(spec, s, hist, cfg, now_local, args),
                                                           encoding="utf-8")
    write_csv(out / "data" / "checks_latest.csv", snaps)
    (root / "index.html").write_text(render_landing(root, cfg, now_local, args), encoding="utf-8")
    (root / ".nojekyll").write_text("", encoding="utf-8")
    for s in snaps:
        log("  %-14s %-7s score %-5s  ok %d  warn %d  fail %d  unknown %d" % (
            s["name"], s["status"], s["score"], s["metrics"]["checks_ok"], s["metrics"]["checks_warn"],
            s["metrics"]["checks_fail"], s["metrics"]["checks_unknown"]), args.quiet)
    log("Wrote %s" % (out / "index.html"), args.quiet)
    code = None
    if args.fail_on:
        bad = {"fail": ("red",), "warn": ("red", "amber")}[args.fail_on]
        if any(s["status"] in bad for s in snaps):
            code = 3
    if args.serve:
        serve(root, args.serve)
    if code:
        return code
    collected = [s for s in snaps if s["status"] != "unknown"]
    if not collected:
        return 2
    if len(collected) < len(snaps) or any(s["errors"] for s in snaps):
        return 1
    return 0
