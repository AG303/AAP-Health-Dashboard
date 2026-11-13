#!/usr/bin/env python3
"""
Mock AAP 2.6 + OpenShift + Prometheus APIs for testing aap_health_dashboard.py offline.

One HTTPS server serves several environments, selected by the first path segment:
  /<env>/api/...          AAP gateway, controller, EDA and hub APIs
  /ocp-<env>/...          OpenShift / Kubernetes API
  /prom-<env>/api/v1/...  Thanos querier (Prometheus HTTP API)
Environments: prod (healthy, occasional blips), nonprod (degraded), dev (no cluster-reader,
superuser token, no Prometheus).

The mock is read-only like the real dashboard: any method other than GET returns 405.
The simulated "now" is set with GET /_mock/day?date=YYYY-MM-DD (22:00 America/Chicago);
data is deterministic per environment and day so seeded history looks like real trends.

  python3 tests/mock_platform.py --port 9443 --cert tests/certs/server.pem --key tests/certs/server.key
"""
import argparse
import json
import random
import ssl
import sys
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

UTC = timezone.utc
STATE = {"now": datetime.now(UTC).replace(microsecond=0)}
LOCK = threading.Lock()
ROUTE_PORT = {"prod": 0, "nonprod": 0, "dev": 0}
ENV_PROFILE = {
    "prod": {"jobs": 420, "success": 0.985, "instances": 3, "superuser": False, "cluster": True, "degraded": False},
    "nonprod": {"jobs": 160, "success": 0.90, "instances": 2, "superuser": False, "cluster": True, "degraded": True},
    "dev": {"jobs": 55, "success": 0.95, "instances": 1, "superuser": True, "cluster": False, "degraded": False},
}


def iso(dt):
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def day_index(now):
    return (now.date() - datetime(2026, 1, 1).date()).days


def rng(env, now, salt=""):
    return random.Random("%s|%s|%s" % (env, now.date().isoformat(), salt))


def blip(env, now, every, salt):
    """True on roughly 1 day in `every` (deterministic)."""
    return rng(env, now, salt).randrange(every) == 0


# --------------------------------------------------------------------------- #
# Data generation
# --------------------------------------------------------------------------- #
def jobs_24h(env, now):
    p = ENV_PROFILE[env]
    r = rng(env, now, "jobs")
    n = int(p["jobs"] * r.uniform(0.85, 1.15))
    rate = p["success"]
    if env == "prod" and blip(env, now, 11, "badjobs"):
        rate = 0.93
    if env == "nonprod" and blip(env, now, 4, "goodday"):
        rate = 0.965
    names = ["Patch Linux", "Deploy App", "Compliance Scan", "Backup Config", "Restart Service",
             "Provision VM", "Network Config Push", "Certificate Renewal", "Cleanup Temp", "Inventory Refresh"]
    out = []
    for i in range(n):
        fin = now - timedelta(seconds=r.randint(60, 86000))
        x = r.random()
        st = "successful" if x < rate else ("failed" if x < rate + (1 - rate) * 0.85 else "error")
        if r.random() < 0.01:
            st = "canceled"
        tpl = names[r.randrange(len(names))] if st == "successful" else names[r.choice([0, 1, 1, 5, 6])]
        el = round(r.lognormvariate(4.3, 0.8), 2)
        out.append({"id": 100000 + day_index(now) * 1000 + i, "name": tpl, "status": st, "elapsed": el,
                    "finished": iso(fin), "started": iso(fin - timedelta(seconds=el)), "created": iso(fin - timedelta(seconds=el + 3)),
                    "launch_type": r.choice(["scheduled", "scheduled", "manual", "workflow", "webhook", "relaunch"])})
    out.sort(key=lambda j: j["finished"], reverse=True)
    return out


def controller(env, now):
    p = ENV_PROFILE[env]
    r = rng(env, now, "ctl")
    insts, groups = [], []
    for i in range(p["instances"]):
        cap = 120 if env == "prod" else 60
        used = int(cap * r.uniform(0.25, 0.55))
        if env == "nonprod" and blip(env, now, 3, "cap"):
            used = int(cap * 0.86)
        state = "ready"
        errors = ""
        if env == "nonprod" and blip(env, now, 9, "inst") and i == 1:
            state, errors = "unavailable", "Failed to connect to receptor"
        hb = now - timedelta(seconds=r.randint(5, 50))
        insts.append({"id": i + 1, "hostname": "%s-controller-task-%d" % (env, i), "node_type": "control",
                      "node_state": state, "enabled": True, "errors": errors, "capacity": cap,
                      "consumed_capacity": used, "percent_capacity_remaining": round(100 - used * 100.0 / cap, 1),
                      "jobs_running": r.randint(0, 9), "version": "4.7.4",
                      "last_health_check": iso(now - timedelta(minutes=r.randint(1, 600))), "heartbeat": iso(hb)})
    tc = sum(i["capacity"] for i in insts)
    tu = sum(i["consumed_capacity"] for i in insts)
    groups.append({"id": 1, "name": "controlplane", "is_container_group": False, "capacity": tc,
                   "consumed_capacity": tu, "percent_capacity_remaining": round(100 - tu * 100.0 / tc, 1),
                   "jobs_running": sum(i["jobs_running"] for i in insts), "instances": len(insts)})
    groups.append({"id": 2, "name": "default", "is_container_group": True, "capacity": 0, "consumed_capacity": 0,
                   "percent_capacity_remaining": 100, "jobs_running": r.randint(3, 25), "instances": 0})
    return insts, groups


def pending(env, now):
    r = rng(env, now, "pend")
    if env == "nonprod" and blip(env, now, 3, "stuck"):
        return [{"id": 9001, "name": "Patch Linux", "status": "pending", "created": iso(now - timedelta(minutes=47))},
                {"id": 9002, "name": "Deploy App", "status": "waiting", "created": iso(now - timedelta(minutes=8))}]
    if r.random() < 0.3:
        return [{"id": 9003, "name": "Inventory Refresh", "status": "pending", "created": iso(now - timedelta(minutes=2))}]
    return []


def running(env, now):
    out = [{"id": 9100, "name": "Compliance Scan", "status": "running", "started": iso(now - timedelta(minutes=12))}]
    if env == "nonprod":
        out.append({"id": 9101, "name": "Provision VM", "status": "running", "started": iso(now - timedelta(hours=6.5))})
    return out


def schedules(env, now):
    out = []
    for i in range(30 if env == "prod" else 14):
        out.append({"id": i + 1, "name": "sched-%02d" % i, "unified_job_template": 100 + i, "enabled": True,
                    "next_run": iso(now + timedelta(hours=1 + i))})
    if env == "nonprod" and blip(env, now, 2, "missed"):
        out[3]["next_run"] = iso(now - timedelta(hours=5))
    # management job schedules (system job templates 1-4)
    for sid in (1, 2, 3, 4):
        if env == "dev" and sid == 2:
            continue
        out.append({"id": 900 + sid, "name": "Cleanup %d" % sid, "unified_job_template": sid, "enabled": True,
                    "next_run": iso(now + timedelta(days=1))})
    return out


def metrics_text(env, now):
    r = rng(env, now, "metrics")
    lag = r.uniform(0.2, 1.5)
    queue = r.randint(0, 300)
    if env == "nonprod" and blip(env, now, 3, "lag"):
        lag, queue = r.uniform(12, 25), r.randint(12000, 30000)
    dbc = r.randint(30, 55) if env != "dev" else r.randint(70, 90)
    lines = [
        "# HELP awx_system_info AWX System Information",
        'awx_system_info{install_uuid="x",insights_analytics="false",license_type="enterprise",tower_url_base="https://x",tower_version="4.7.4"} 1.0',
        "awx_database_connections_total %d" % dbc,
        'awx_sessions_total{type="all"} %d' % r.randint(20, 90),
        "awx_pending_jobs_total %d" % len(pending(env, now)),
        'awx_status_total{status="successful"} 1234.0',
        'awx_status_total{status="failed"} 21.0',
    ]
    for i in range(ENV_PROFILE[env]["instances"]):
        node = "%s-controller-task-%d" % (env, i)
        lines += ['awx_instance_remaining_capacity{hostname="%s"} %d' % (node, r.randint(40, 90)),
                  'callback_receiver_event_processing_avg_seconds{node="%s"} %.3f' % (node, lag * r.uniform(0.7, 1.0)),
                  'callback_receiver_events_queue_size_redis{node="%s"} %d' % (node, queue),
                  'callback_receiver_events_insert_db{node="%s"} %d' % (node, r.randint(10 ** 5, 10 ** 6))]
    return "\n".join(lines) + "\n"


def activations(env, now):
    names = ["dynatrace-problems", "servicenow-incidents", "alertmanager-webhook", "kafka-events", "splunk-alerts",
             "vm-drift"]
    out = []
    for i, n in enumerate(names[: (6 if env == "prod" else 4)]):
        st, rc = "running", 0
        if env == "nonprod" and i == 2 and not blip(env, now, 3, "eda-ok"):
            st, rc = "failed", 7
        if env == "nonprod" and i == 3 and blip(env, now, 2, "eda-restart"):
            rc = 6
        out.append({"id": i + 1, "name": n, "is_enabled": True, "status": st, "restart_count": rc,
                    "status_message": "Activation failed: rulebook process exited (code 1)" if st == "failed" else ""})
    out.append({"id": 99, "name": "retired-rulebook", "is_enabled": False, "status": "stopped", "restart_count": 0,
                "status_message": ""})
    return out


def activation_instances(env, now):
    out = []
    if env == "nonprod":
        r = rng(env, now, "ai")
        for i in range(r.randint(1, 6)):
            out.append({"id": 500 + i, "status": "failed", "ended_at": iso(now - timedelta(hours=r.randint(1, 20)))})
    out.append({"id": 10, "status": "failed", "ended_at": iso(now - timedelta(days=12))})
    return out


def audit_rules(env, now):
    r = rng(env, now, "rules")
    n = {"prod": 140, "nonprod": 60, "dev": 12}[env]
    n = int(n * r.uniform(0.6, 1.4))
    return [{"id": i, "name": "rule-%d" % (i % 9), "fired_at": iso(now - timedelta(minutes=r.randint(1, 1430)))}
            for i in range(n)] + [{"id": 99999, "name": "old", "fired_at": iso(now - timedelta(days=3))}]


def hub_status(env, now):
    r = rng(env, now, "hub")
    total = 500 * 1024 ** 3
    used_frac = 0.55 + day_index(now) * 0.0006 if env == "prod" else 0.81 + day_index(now) * 0.0002
    workers = [{"name": "w%d" % i} for i in range(2)]
    if env == "nonprod" and blip(env, now, 10, "hubdown"):
        workers = []
    return {"versions": [{"component": "galaxy", "version": "4.11.0"}], "online_workers": workers,
            "online_content_apps": [{"name": "c1"}, {"name": "c2"}],
            "database_connection": {"connected": True}, "redis_connection": {"connected": True},
            "storage": {"total": total, "used": int(total * min(used_frac, 0.97)),
                        "free": int(total * (1 - min(used_frac, 0.97)))}, "_r": r.random()}


def hub_tasks(env, now, state):
    r = rng(env, now, "hubtasks" + state)
    if state == "failed":
        n = r.randint(2, 7) if env == "nonprod" else (1 if r.random() < 0.15 else 0)
        return [{"pulp_href": "/t/%d" % i, "state": "failed", "finished_at": iso(now - timedelta(hours=2 + i))}
                for i in range(n)]
    if state == "waiting":
        if env == "nonprod" and blip(env, now, 4, "hubwait"):
            return [{"pulp_href": "/t/w1", "state": "waiting", "pulp_created": iso(now - timedelta(minutes=55))}]
        return []
    return []


# ---- OpenShift ------------------------------------------------------------- #
def cr(kind, name, ok=True, msg=""):
    conds = [{"type": "Running", "status": "True", "lastTransitionTime": "2026-10-01T00:00:00Z"},
             {"type": "Successful", "status": "True" if ok else "False", "lastTransitionTime": "2026-10-01T00:00:00Z"}]
    if not ok:
        conds.append({"type": "Failure", "status": "True", "message": msg, "reason": "Failed"})
    return {"kind": kind, "metadata": {"name": name, "namespace": "aap"}, "status": {"conditions": conds, "version": "2.6"}}


def nodes(env):
    out = []
    for i in range(3):
        out.append(("%s-master-%d.ocp.example.com" % (env, i), "master"))
    for i in range(3 if env != "dev" else 2):
        out.append(("%s-worker-%d.ocp.example.com" % (env, i), "worker"))
    return out


POD_LAYOUT = [("aap-gateway", 2, "gateway"), ("aap-controller-web", 2, "web"), ("aap-controller-task", 2, "task"),
              ("aap-eda-api", 1, "api"), ("aap-eda-activation-worker", 2, "worker"), ("aap-eda-default-worker", 1, "worker"),
              ("aap-eda-scheduler", 1, "scheduler"), ("aap-hub-api", 1, "api"), ("aap-hub-content", 2, "content"),
              ("aap-hub-worker", 2, "worker"), ("aap-redis", 1, "redis"), ("aap-gateway-operator-controller-manager", 1, "manager"),
              ("aap-postgres-15", 1, "postgres")]


def pods(env, now):
    workers = [n for n, role in nodes(env) if role == "worker"]
    d = day_index(now)
    out = []
    for wl, reps, cname in POD_LAYOUT:
        if env == "dev":
            reps = 1
        for i in range(reps):
            name = "%s-%s-%s" % (wl, "7f9c8d6b5", "x%dq%d" % (i, len(wl) % 7)) if "postgres" not in wl and "redis" not in wl \
                else "%s-%d" % (wl, i)
            owner = [{"kind": "StatefulSet", "name": wl}] if ("postgres" in wl or "redis" in wl) else \
                [{"kind": "ReplicaSet", "name": wl + "-7f9c8d6b5"}]
            node = workers[(i + len(wl)) % len(workers)]
            if env == "nonprod" and wl == "aap-controller-web":
                node = workers[0]  # both replicas on one node -> spread warning
            restarts = 0
            if env == "nonprod" and wl == "aap-eda-activation-worker":
                restarts = int(d * 1.3)
            if env == "prod" and wl == "aap-hub-worker" and i == 0:
                restarts = d // 9
            ready, waiting, last = True, None, {}
            if env == "nonprod" and wl == "aap-eda-activation-worker" and i == 1 and blip(env, now, 4, "crash"):
                ready, waiting = False, {"reason": "CrashLoopBackOff", "message": "back-off 5m0s"}
            if env == "nonprod" and wl == "aap-redis" and blip(env, now, 5, "oom"):
                last = {"terminated": {"reason": "OOMKilled", "exitCode": 137, "finishedAt": iso(now - timedelta(hours=3))}}
            limits = {"memory": "2Gi", "cpu": "1"} if not (env == "dev" and cname == "task") else {}
            out.append({"metadata": {"name": name, "namespace": "aap", "ownerReferences": owner},
                        "spec": {"nodeName": node, "containers": [{"name": cname, "resources": {"limits": limits,
                                                                                                "requests": {"memory": "512Mi"}}}]},
                        "status": {"phase": "Running", "containerStatuses": [
                            {"name": cname, "ready": ready, "restartCount": restarts,
                             "state": {"waiting": waiting} if waiting else {"running": {}}, "lastState": last}]}})
    out.append({"metadata": {"name": "automation-job-12345-abcde", "namespace": "aap"}, "spec": {"nodeName": workers[0],
                "containers": [{"name": "worker"}]}, "status": {"phase": "Running", "containerStatuses": []}})
    out.append({"metadata": {"name": "aap-backup-job-xyz", "namespace": "aap"}, "spec": {}, "status": {"phase": "Succeeded"}})
    return out


def pod_usage(env, now):
    r = rng(env, now, "usage")
    out = []
    for p in pods(env, now):
        n = p["metadata"]["name"]
        if n.startswith("automation-job") or p["status"]["phase"] != "Running":
            continue
        c = p["spec"]["containers"][0]["name"]
        mem = r.uniform(0.25, 0.7)
        if env == "nonprod" and "redis" in n and blip(env, now, 2, "redismem"):
            mem = 0.89
        out.append({"metadata": {"name": n}, "containers": [{"name": c, "usage": {
            "cpu": "%dm" % r.randint(20, 600), "memory": "%dKi" % int(2 * 1024 * 1024 * mem)}}]})
    return out


def backups(env, now):
    age_h = {"prod": 2, "nonprod": 50, "dev": 20}[env]
    if env == "prod" and blip(env, now, 15, "nobackup"):
        age_h = 30
    t = now - timedelta(hours=age_h)
    return [{"metadata": {"name": "aap-backup-%s" % t.strftime("%Y%m%d"), "creationTimestamp": iso(t)},
             "status": {"conditions": [{"type": "Successful", "status": "True", "lastTransitionTime": iso(t)}]}},
            {"metadata": {"name": "aap-backup-failed", "creationTimestamp": iso(now - timedelta(hours=1))},
             "status": {"conditions": [{"type": "Failure", "status": "True"}]}}]


def cluster_operators(env, now):
    names = ["authentication", "console", "dns", "etcd", "ingress", "kube-apiserver", "machine-config", "monitoring",
             "network", "storage", "openshift-apiserver", "image-registry"]
    out = []
    for n in names:
        deg = env == "nonprod" and n == "image-registry" and blip(env, now, 3, "co")
        out.append({"metadata": {"name": n}, "status": {"conditions": [
            {"type": "Available", "status": "True"}, {"type": "Progressing", "status": "False"},
            {"type": "Degraded", "status": "True" if deg else "False", "message": "ImagePrunerDegraded: Job failed" if deg else ""}]}})
    return out


def prom_query(env, now, q):
    r = rng(env, now, "prom" + q[:20])

    def vec(rows):
        return {"status": "success", "data": {"resultType": "vector", "result": [
            {"metric": m, "value": [now.timestamp(), str(v)]} for m, v in rows]}}
    if q.startswith("ALERTS"):
        rows = [({"alertname": "Watchdog", "severity": "none"}, 1)]
        if env == "nonprod":
            rows += [({"alertname": "KubePodCrashLooping", "severity": "warning", "namespace": "aap"}, 1),
                     ({"alertname": "PrometheusDiskSpaceLow", "severity": "warning", "namespace": "openshift-monitoring"}, 1)]
            if blip(env, now, 4, "critalert"):
                rows.append(({"alertname": "etcdHighFsyncDurations", "severity": "critical"}, 1))
        if env == "prod" and blip(env, now, 8, "warnalert"):
            rows.append(({"alertname": "KubeCPUOvercommit", "severity": "warning"}, 1))
        return vec(rows)
    if "wal_fsync" in q:
        v = r.uniform(0.003, 0.007) if env == "prod" else r.uniform(0.008, 0.016)
        return vec([({"instance": "m%d" % i}, v * r.uniform(0.8, 1)) for i in range(3)])
    if "backend_commit" in q:
        v = r.uniform(0.006, 0.015) if env == "prod" else r.uniform(0.015, 0.04)
        return vec([({"instance": "m0"}, v)])
    if "peer_round_trip" in q:
        return vec([({"instance": "m0"}, r.uniform(0.004, 0.012))])
    if "leader_changes" in q:
        return vec([({}, 0 if env == "prod" else r.choice([0, 0, 1, 2]))])
    if "kubelet_volume_stats" in q:
        base = 0.42 + day_index(now) * 0.001 if env == "prod" else 0.74 + day_index(now) * 0.0006
        return vec([({"persistentvolumeclaim": "postgres-15-aap-postgres-15-0"}, min(base, 0.96)),
                    ({"persistentvolumeclaim": "aap-hub-file-storage"}, 0.5),
                    ({"persistentvolumeclaim": "aap-redis-data"}, 0.1)])
    if "cfs_throttled" in q:
        return vec([({"pod": "aap-postgres-15-0"}, 0.31 if env == "nonprod" else 0.05),
                    ({"pod": "aap-gateway-7f9c8d6b5-x0q4"}, 0.02)])
    return vec([])


# --------------------------------------------------------------------------- #
# HTTP handler
# --------------------------------------------------------------------------- #
class Handler(BaseHTTPRequestHandler):
    server_version = "mock-aap-ocp/1.0"

    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else (body if isinstance(body, str) else json.dumps(body)).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):  # noqa: N802 - read-only mock
        self._send(405, {"detail": "read-only mock"})
    do_PUT = do_PATCH = do_DELETE = do_POST

    def page(self, items, q, base):
        size = int(q.get("page_size", ["25"])[0])
        pg = int(q.get("page", ["1"])[0])
        chunk = items[(pg - 1) * size: pg * size]
        nxt = None
        if pg * size < len(items):
            qq = {k: v[0] for k, v in q.items()}
            qq["page"] = pg + 1
            nxt = base + "?" + urlencode(qq)
        return {"count": len(items), "next": nxt, "previous": None, "results": chunk}

    def pulp(self, items, q, base):
        lim = int(q.get("limit", ["100"])[0])
        off = int(q.get("offset", ["0"])[0])
        nxt = None
        if off + lim < len(items):
            qq = {k: v[0] for k, v in q.items()}
            qq["offset"] = off + lim
            nxt = base + "?" + urlencode(qq)
        return {"count": len(items), "next": nxt, "previous": None, "results": items[off: off + lim],
                "meta": {"count": len(items)}, "links": {"next": nxt}, "data": items[off: off + lim]}

    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        q = parse_qs(u.query)
        parts = u.path.split("/", 2)
        if u.path == "/_mock/day":
            with LOCK:
                d = datetime.strptime(q["date"][0], "%Y-%m-%d")
                # 22:00 America/Chicago ~= 04:00 UTC next day (CST); close enough for the mock
                STATE["now"] = (d + timedelta(hours=28)).replace(tzinfo=UTC)
            return self._send(200, {"now": iso(STATE["now"])})
        if len(parts) < 3:
            return self._send(404, {"detail": "not found"})
        seg, rest = parts[1], "/" + parts[2]
        if not self.headers.get("Authorization", "").startswith("Bearer "):
            return self._send(401, {"detail": "Authentication credentials were not provided."})
        now = STATE["now"]
        try:
            if seg in ENV_PROFILE:
                return self.aap(seg, rest, q, now, rest.split("?")[0])
            if seg.startswith("ocp-") and seg[4:] in ENV_PROFILE:
                return self.ocp(seg[4:], rest, q, now)
            if seg.startswith("prom-") and seg[5:] in ENV_PROFILE:
                if rest.startswith("/api/v1/query"):
                    return self._send(200, prom_query(seg[5:], now, q.get("query", [""])[0]))
        except Exception as exc:  # noqa: BLE001
            return self._send(500, {"detail": str(exc)})
        return self._send(404, {"detail": "not found"})

    # ---- AAP ---------------------------------------------------------------- #
    def aap(self, env, path, q, now, base):
        p = ENV_PROFILE[env]
        G, C, E, H = "/api/gateway/v1", "/api/controller/v2", "/api/eda/v1", "/api/galaxy"
        if path == G + "/status/":
            eda = "good"
            if env == "nonprod" and blip(env, now, 10, "gweda"):
                eda = "bad"
            return self._send(200, {"status": "good" if eda == "good" else "degraded", "services": [
                {"service_name": "controller", "status": "good"}, {"service_name": "eda", "status": eda},
                {"service_name": "hub", "status": "good"},
                {"service_name": "redis", "status": "good", "response": {"mode": "cluster" if env == "prod" else "standalone"}}]})
        if path == G + "/me/":
            return self._send(200, {"count": 1, "results": [{"username": "aap-health-auditor" if not p["superuser"] else "admin",
                                                             "is_superuser": p["superuser"], "is_system_auditor": not p["superuser"]}]})
        if path == G + "/users/":
            r = rng(env, now, "users")
            n = {"prod": 240, "nonprod": 90, "dev": 25}[env]
            users = [{"id": i, "username": "u%d" % i, "last_login": iso(now - timedelta(days=r.randint(0, 120)))
                      if r.random() < 0.9 else None} for i in range(n)]
            return self._send(200, self.page(users, q, base))
        if path == G + "/organizations/":
            return self._send(200, self.page([{"id": i} for i in range(6)], q, base))
        if path == C + "/ping/":
            insts, _ = controller(env, now)
            return self._send(200, {"version": "4.7.4", "ha": len(insts) > 1, "active_node": insts[0]["hostname"],
                                    "instances": [{"node": i["hostname"], "node_type": "control", "heartbeat": i["heartbeat"],
                                                   "capacity": i["capacity"], "version": "4.7.4"} for i in insts]})
        if path == C + "/instances/":
            return self._send(200, self.page(controller(env, now)[0], q, base))
        if path == C + "/instance_groups/":
            return self._send(200, self.page(controller(env, now)[1], q, base))
        if path == C + "/metrics/":
            return self._send(200, metrics_text(env, now), "text/plain; version=0.0.4")
        if path in (C + "/jobs/", C + "/workflow_jobs/"):
            fg = q.get("finished__gt", [None])[0]
            since = datetime.fromisoformat(fg.replace("Z", "+00:00")) if fg else None
            if since and (now - since) > timedelta(days=2):
                # 30-day counts (synthetic)
                r = rng(env, now, "30d")
                total = int(p["jobs"] * 30 * r.uniform(0.95, 1.05))
                st = q.get("status", [None])[0]
                stin = q.get("status__in", [None])[0]
                rate = p["success"] + (0.005 if env == "nonprod" else 0)
                if st == "successful":
                    total = int(total * rate)
                elif stin:
                    total = int(total * (1 - rate))
                return self._send(200, {"count": total, "next": None, "results": []})
            jobs = jobs_24h(env, now)
            if path.endswith("workflow_jobs/"):
                jobs = jobs[: len(jobs) // 8]
            if since:
                jobs = [j for j in jobs if j["finished"] > iso(since)]
            return self._send(200, self.page(jobs, q, base))
        if path == C + "/unified_jobs/":
            st = q.get("status", [None])[0]
            stin = q.get("status__in", [""])[0]
            if st == "running":
                return self._send(200, self.page(running(env, now), q, base))
            if "pending" in stin:
                return self._send(200, self.page(pending(env, now), q, base))
            return self._send(200, self.page([], q, base))
        if path == C + "/projects/":
            items = [{"id": i, "name": "project-%d" % i, "status": "successful"} for i in range(12)]
            if env == "nonprod" and blip(env, now, 3, "proj"):
                items[2]["status"] = "failed"
            if "status__in" in q:
                items = [i for i in items if i["status"] in q["status__in"][0].split(",")]
            return self._send(200, self.page(items, q, base))
        if path == C + "/inventory_sources/":
            items = [{"id": 1, "name": "vmware-dc1", "status": "successful"}, {"id": 2, "name": "aws-prod", "status": "successful"}]
            if env == "nonprod":
                items[1]["status"] = "failed"
            if "status__in" in q:
                items = [i for i in items if i["status"] in q["status__in"][0].split(",")]
            return self._send(200, self.page(items, q, base))
        if path == C + "/schedules/":
            return self._send(200, self.page(schedules(env, now), q, base))
        if path == C + "/system_job_templates/":
            return self._send(200, self.page([{"id": 1, "name": "Cleanup Job Details", "job_type": "cleanup_jobs"},
                                              {"id": 2, "name": "Cleanup Activity Stream", "job_type": "cleanup_activitystream"},
                                              {"id": 3, "name": "Cleanup Expired Sessions", "job_type": "cleanup_sessions"},
                                              {"id": 4, "name": "Cleanup Expired OAuth 2 Tokens", "job_type": "cleanup_tokens"}], q, base))
        if path == C + "/system_jobs/":
            return self._send(200, self.page([], q, base))
        counts = {"job_templates": 180, "workflow_job_templates": 24, "inventories": 30, "hosts": 2400,
                  "credentials": 60, "execution_environments": 8}
        for k, v in counts.items():
            if path == "%s/%s/" % (C, k):
                v = v if env == "prod" else v // 3
                return self._send(200, {"count": v, "next": None, "results": []})
        if path == C + "/config/":
            days = {"prod": 240, "nonprod": 45, "dev": 240}[env] - (day_index(now) - 300)
            lic = {"prod": 3000, "nonprod": 500, "dev": 100}[env]
            used = {"prod": 2310, "nonprod": 480, "dev": 40}[env] + day_index(now) // 10
            return self._send(200, {"version": "4.7.4", "license_info": {
                "compliant": used <= lic, "time_remaining": max(days, 0) * 86400, "date_expired": days <= 0,
                "instance_count": lic, "current_instances": used, "subscription_name": "Red Hat Ansible Automation Platform, Premium"}})
        if path == E + "/status/":
            return self._send(200, {"status": "good"})
        if path == E + "/activations/":
            return self._send(200, self.page(activations(env, now), q, base))
        if path == E + "/activation-instances/":
            items = activation_instances(env, now)
            if "status" in q:
                items = [i for i in items if i["status"] == q["status"][0]]
            return self._send(200, self.page(items, q, base))
        if path == E + "/audit-rules/":
            return self._send(200, self.page(audit_rules(env, now), q, base))
        if path == E + "/projects/":
            items = [{"id": 1, "name": "eda-rulebooks", "import_state": "completed"}]
            if env == "nonprod":
                items.append({"id": 2, "name": "eda-sandbox", "import_state": "failed"})
            return self._send(200, self.page(items, q, base))
        if path in (E + "/decision-environments/", E + "/event-streams/"):
            return self._send(200, {"count": 3, "next": None, "results": []})
        if path == H + "/pulp/api/v3/status/":
            if env == "dev":
                return self._send(404, {"detail": "not found"})
            return self._send(200, hub_status(env, now))
        if path == H + "/pulp/api/v3/tasks/":
            return self._send(200, self.pulp(hub_tasks(env, now, q.get("state", [""])[0]), q, base))
        if path == H + "/v3/plugin/ansible/search/collection-versions/":
            return self._send(200, self.pulp([{}] * (310 if env == "prod" else 120), q, base))
        if path == H + "/v3/plugin/execution-environments/repositories/":
            return self._send(200, self.pulp([{}] * 9, q, base))
        return self._send(404, {"detail": "not found: " + path})

    # ---- OpenShift ---------------------------------------------------------- #
    def ocp(self, env, path, q, now):
        p = ENV_PROFILE[env]
        ns = "/namespaces/aap"
        L = lambda items: self._send(200, {"kind": "List", "metadata": {}, "items": items})  # noqa: E731
        if path == "/apis/aap.ansible.com/v1alpha1%s/ansibleautomationplatforms/aap" % ns:
            obj = cr("AnsibleAutomationPlatform", "aap")
            obj["spec"] = {"database": {} if env != "prod" else {"database_secret": "external-postgres"},
                           "controller": {"replicas": 2 if env != "dev" else 1}, "api": {"replicas": 2 if env != "dev" else 1}}
            return self._send(200, obj)
        if path == "/apis/automationcontroller.ansible.com/v1beta1%s/automationcontrollers" % ns:
            return L([cr("AutomationController", "aap-controller")])
        if path == "/apis/eda.ansible.com/v1alpha1%s/edas" % ns:
            bad = env == "nonprod" and blip(env, now, 4, "edacr")
            return L([cr("EDA", "aap-eda", not bad, "Deployment aap-eda-activation-worker not ready")])
        if path == "/apis/automationhub.ansible.com/v1beta1%s/automationhubs" % ns:
            return L([cr("AutomationHub", "aap-hub")])
        if path == "/apis/operators.coreos.com/v1alpha1%s/clusterserviceversions" % ns:
            return L([{"metadata": {"name": "aap-operator.v2.6.0-0.1761234567", "creationTimestamp": "2026-10-01T00:00:00Z"},
                       "spec": {"version": "2.6.0+0.1761234567"}, "status": {"phase": "Succeeded"}}])
        if path == "/apis/operators.coreos.com/v1alpha1%s/subscriptions" % ns:
            cur = "aap-operator.v2.6.0-0.1765000000" if env == "nonprod" else "aap-operator.v2.6.0-0.1761234567"
            return L([{"spec": {"name": "ansible-automation-platform-operator", "channel": "stable-2.6",
                                "installPlanApproval": "Manual"},
                       "status": {"installedCSV": "aap-operator.v2.6.0-0.1761234567", "currentCSV": cur}}])
        if path == "/api/v1%s/pods" % ns:
            return L(pods(env, now))
        if path == "/apis/apps/v1%s/deployments" % ns:
            items = []
            for wl, reps, _ in POD_LAYOUT:
                if "postgres" in wl or "redis" in wl:
                    continue
                reps = 1 if env == "dev" else reps
                ready = reps
                if env == "nonprod" and wl == "aap-eda-activation-worker" and blip(env, now, 4, "crash"):
                    ready = reps - 1
                items.append({"metadata": {"name": wl}, "spec": {"replicas": reps}, "status": {"readyReplicas": ready}})
            return L(items)
        if path == "/apis/apps/v1%s/statefulsets" % ns:
            return L([{"metadata": {"name": "aap-postgres-15"}, "spec": {"replicas": 1}, "status": {"readyReplicas": 1}},
                      {"metadata": {"name": "aap-redis"}, "spec": {"replicas": 1}, "status": {"readyReplicas": 1}}])
        if path == "/apis/policy/v1%s/poddisruptionbudgets" % ns:
            return L([{"metadata": {"name": "aap-gateway"}, "status": {"expectedPods": 2, "disruptionsAllowed": 1}},
                      {"metadata": {"name": "aap-postgres"}, "status": {"expectedPods": 1,
                                                                        "disruptionsAllowed": 0 if env == "nonprod" else 1}}])
        if path == "/api/v1%s/persistentvolumeclaims" % ns:
            return L([{"metadata": {"name": "postgres-15-aap-postgres-15-0"}, "spec": {"accessModes": ["ReadWriteOnce"],
                       "storageClassName": "ocs-storagecluster-ceph-rbd"}, "status": {"phase": "Bound", "capacity": {"storage": "120Gi" if env == "nonprod" else "50Gi"}}},
                      {"metadata": {"name": "aap-hub-file-storage"}, "spec": {"accessModes": ["ReadWriteMany"] if env != "nonprod" else ["ReadWriteOnce"],
                       "storageClassName": "ocs-storagecluster-cephfs"}, "status": {"phase": "Bound", "capacity": {"storage": "500Gi"}}},
                      {"metadata": {"name": "aap-redis-data"}, "spec": {"accessModes": ["ReadWriteOnce"]},
                       "status": {"phase": "Bound", "capacity": {"storage": "1Gi"}}}])
        if path == "/apis/metrics.k8s.io/v1beta1%s/pods" % ns:
            return L(pod_usage(env, now))
        if path == "/api/v1%s/events" % ns:
            r = rng(env, now, "events")
            n = r.randint(0, 6) if env == "prod" else r.randint(15, 45)
            reasons = ["BackOff", "Unhealthy", "FailedScheduling", "ProbeWarning", "FailedMount"]
            return L([{"type": "Warning", "reason": reasons[r.randrange(len(reasons) if env != "prod" else 2)],
                       "lastTimestamp": iso(now - timedelta(hours=r.uniform(0, 30)))} for _ in range(n)])
        if path == "/apis/route.openshift.io/v1%s/routes" % ns:
            items = []
            for rn in ("aap", "aap-controller", "aap-hub", "aap-eda"):
                items.append({"metadata": {"name": rn}, "spec": {"host": "localhost", "tls": {"termination": "edge"}},
                              "status": {"ingress": [{"conditions": [{"type": "Admitted", "status": "True"}]}]}})
            return L(items)
        if path == "/api/v1%s/resourcequotas" % ns:
            used = "26" if env == "nonprod" else "14"
            return L([{"metadata": {"name": "aap-quota"}, "status": {"hard": {"requests.cpu": "30", "requests.memory": "96Gi"},
                                                                    "used": {"requests.cpu": used, "requests.memory": "40Gi"}}}])
        if path == "/apis/aap.ansible.com/v1alpha1%s/ansibleautomationplatformbackups" % ns:
            return L(backups(env, now))
        # ---- cluster scope ----
        if not p["cluster"] and (path.startswith("/apis/config.openshift.io") or path == "/api/v1/nodes" or
                                 path.startswith("/apis/machineconfiguration") or path.startswith("/apis/certificates")
                                 or path.startswith("/api/v1/namespaces/openshift-etcd")
                                 or path == "/apis/metrics.k8s.io/v1beta1/nodes"):
            return self._send(403, {"kind": "Status", "message": "forbidden: User cannot list resource at the cluster scope"})
        if path == "/apis/config.openshift.io/v1/clusterversions/version":
            upd = [{"version": "4.18.%d" % v} for v in range(24, 27)] if env == "nonprod" else [{"version": "4.18.26"}]
            conds = [{"type": "Available", "status": "True"}, {"type": "Failing", "status": "False"},
                     {"type": "Progressing", "status": "False"}, {"type": "RetrievedUpdates", "status": "True"}]
            if env == "nonprod":
                conds.append({"type": "Upgradeable", "status": "False",
                              "message": "Cluster operator image-registry should not be upgraded between minor versions"})
            return self._send(200, {"spec": {"channel": "stable-4.18"}, "status": {
                "desired": {"version": "4.18.22" if env == "nonprod" else "4.18.25"}, "availableUpdates": upd, "conditions": conds}})
        if path == "/apis/config.openshift.io/v1/clusteroperators":
            return L(cluster_operators(env, now))
        if path == "/api/v1/nodes":
            items = []
            for n, role in nodes(env):
                conds = [{"type": "Ready", "status": "True"}, {"type": "MemoryPressure", "status": "False"},
                         {"type": "DiskPressure", "status": "True" if (env == "nonprod" and n.endswith("worker-2.ocp.example.com")
                                                                       and blip(env, now, 5, "disk")) else "False"},
                         {"type": "PIDPressure", "status": "False"}]
                items.append({"metadata": {"name": n, "labels": {"node-role.kubernetes.io/" + role: ""}},
                              "spec": {}, "status": {"conditions": conds, "allocatable": {"cpu": "15500m", "memory": "63Gi"},
                                                     "nodeInfo": {"kubeletVersion": "v1.31.9"}}})
            return L(items)
        if path == "/apis/metrics.k8s.io/v1beta1/nodes":
            r = rng(env, now, "nodeusage")
            hi = 0.88 if env == "nonprod" else 0.6
            return L([{"metadata": {"name": n}, "usage": {"cpu": "%dm" % int(15500 * r.uniform(0.2, hi)),
                                                          "memory": "%dKi" % int(63 * 1024 * 1024 * r.uniform(0.3, hi))}}
                      for n, _ in nodes(env)])
        if path == "/apis/machineconfiguration.openshift.io/v1/machineconfigpools":
            return L([{"metadata": {"name": "master"}, "spec": {}, "status": {"machineCount": 3, "readyMachineCount": 3,
                       "degradedMachineCount": 0, "conditions": [{"type": "Degraded", "status": "False"}, {"type": "Updating", "status": "False"}]}},
                      {"metadata": {"name": "worker"}, "spec": {}, "status": {"machineCount": 3, "readyMachineCount": 3,
                       "degradedMachineCount": 0, "conditions": [{"type": "Degraded", "status": "False"}, {"type": "Updating", "status": "False"}]}}])
        if path == "/apis/certificates.k8s.io/v1/certificatesigningrequests":
            items = [{"metadata": {"name": "csr-ok"}, "status": {"conditions": [{"type": "Approved"}]}}]
            if env == "nonprod" and blip(env, now, 3, "csr"):
                items.append({"metadata": {"name": "csr-pending-1"}, "status": {}})
            return L(items)
        if path == "/api/v1/namespaces/openshift-etcd/pods":
            return L([{"metadata": {"name": "etcd-m%d" % i}, "status": {"containerStatuses": [{"ready": True}]}} for i in range(3)])
        return self._send(404, {"kind": "Status", "message": "not found: " + path})


class QuietServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError, ssl.SSLError, TimeoutError)):
            return
        super().handle_error(request, client_address)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9443)
    ap.add_argument("--cert", required=True)
    ap.add_argument("--key", required=True)
    args = ap.parse_args()
    srv = QuietServer(("127.0.0.1", args.port), Handler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(args.cert, args.key)
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    print("mock platform on https://127.0.0.1:%d" % args.port, file=sys.stderr, flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
