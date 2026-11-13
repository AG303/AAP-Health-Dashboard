#!/usr/bin/env python3
"""
aap_health_dashboard.py - daily Ansible Automation Platform 2.6 health dashboard.

AAP data only: every check uses the AAP APIs behind the platform gateway route
(gateway, automation controller, Event-Driven Ansible, automation hub). OpenShift data
(pods, operator, storage, backups, nodes, etcd, alerts) is collected separately by
ocp_health_dashboard.py, which has its own token, history and pages.

Output (static site, no JavaScript, no CDN):
  <out>/aap/index.html              management view: status, KPIs vs target, trends, open issues
  <out>/aap/env/<name>.html         engineering / support detail per environment
  <out>/aap/history.json            one entry per day per environment (drives every chart)
  <out>/aap/data/latest.json, data/checks_latest.csv, data/snapshots/<date>.json
  <out>/index.html                  landing page linking the AAP and OpenShift dashboards

Checks (each cites the Red Hat guidance it implements; see docs/CHECKS.md)
  Platform gateway  status endpoint (controller / hub / EDA / Redis), API latency, token validity,
                    least privilege, active users, organizations
  Controller        ping + heartbeats, instance health / errors / capacity, instance groups,
                    metrics endpoint (job event lag, Redis callback queue, DB connections),
                    operator-managed database load (connections, concurrent jobs), 24h job success /
                    duration / volume, pending + long-running jobs, project and inventory sync
                    failures, missed schedules, cleanup management jobs, subscription
  Event-Driven      status, activation health and restarts, failed rulebook processes, rules
  Ansible           fired, project import failures
  Automation hub    Pulp status (workers, content apps, DB, Redis, storage), failed and waiting
                    tasks, content counts

Read-only by design: HTTP GET only (enforced in the Http class), bearer token only. Use an
OAuth token with scope "read" for a user with the Platform Auditor role. Each environment
names the variable holding its token (aap_token_env); in GitHub Actions the script also reads
AAP_HEALTH_SECRETS_JSON (set to ${{ toJSON(secrets) }}).

Which environments run: every environment with "enabled" not false and an aap_url, limited
by settings.only_tiers (for example ["prod"]) or --env.

Usage
  python3 aap_health_dashboard.py -c environments.json -o site
  python3 aap_health_dashboard.py -c environments.json -o site --serve 8080

Exit codes: 0 all collected; 1 partial (dashboard still written); 2 nothing collected / bad
config; 3 --fail-on tripped.

Requirements: Python 3.9+, standard library only, healthdash_core.py next to this file.
"""
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from healthdash_core import (  # noqa: E402
    ApiError, BaseCollector, Dashboard, Http, card, common_kpis, esc, gib, grade_high, grade_low,
    iso, kcell, log, lookup_secret, num, parse_dt, pct, percentile, prom_metrics, run_dashboard,
    short, status_card, vs_target,
)


class AapCollector(BaseCollector):
    def setup(self):
        env = self.env
        self.aap = None
        if env.get("aap_url"):
            self.aap = Http("aap", env["aap_url"], lookup_secret(env.get("aap_token_env")), self.verify, self.ca,
                            self.timeout)
        self.ctl = env.get("controller_prefix", "/api/controller/v2")
        self.eda = env.get("eda_prefix", "/api/eda/v1")
        self.hub = env.get("hub_prefix", "/api/galaxy")
        self.gw = env.get("gateway_prefix", "/api/gateway/v1")

    def run(self):
        log("  [%s] collecting AAP" % self.env["name"], self.quiet)
        if not self.aap.token:
            self.add("gateway.token", "gateway", "AAP API token configured", "fail", "missing",
                     "Environment variable %s is empty" % self.env.get("aap_token_env"),
                     "Create a read-scoped token for a Platform Auditor user and store it as a secret", "critical")
            return self
        self.section("gateway", "gateway status and identity", self.gateway)
        self.section("controller", "controller cluster and capacity", self.controller_cluster)
        self.section("controller", "controller metrics endpoint", self.controller_metrics)
        self.section("controller", "operator database load", self.controller_db_load)
        self.section("controller", "controller jobs", self.controller_jobs)
        self.section("controller", "controller content and schedules", self.controller_content)
        self.section("controller", "controller subscription", self.controller_license)
        if self.env.get("eda_enabled", True):
            self.section("eda", "event-driven ansible", self.eda_checks)
        if self.env.get("hub_enabled", True):
            self.section("hub", "automation hub", self.hub_checks)
        return self

    def controller_db_load(self):
        """Red Hat: the operator-deployed PostgreSQL is sized for max_connections 100 and up to
        100 concurrent jobs. The OpenShift dashboard checks the replica and PVC-size limits."""
        t = self.t
        mode = self.env.get("database", "operator-managed")
        if mode == "external":
            self.add("controller.operator_db_load", "controller", "Operator-managed database load within limits",
                     "ok", "external database", "environments.json: database = external", "", "minor",
                     "aap_topology")
            return
        reasons = []
        conns = self.metrics.get("ctl.db_connections")
        if conns is not None:
            p_ = pct(conns, t["operator_db_max_connections"])
            self.m("ctl.db_connections_pct", p_)
            if p_ >= t["db_connections_warn_pct"]:
                reasons.append("controller DB connections %d of %d" % (conns, t["operator_db_max_connections"]))
        cap = sum(int(r.get("capacity") or 0) for r in self.details.get("instance_groups") or []
                  if not r.get("container_group"))
        self.m("ctl.capacity_total", cap)
        if cap > t["operator_db_max_concurrent_jobs"]:
            reasons.append("controller capacity %d > %d concurrent jobs" % (cap, t["operator_db_max_concurrent_jobs"]))
        self.add("controller.operator_db_load", "controller", "Operator-managed database load within limits",
                 "warn" if reasons else "ok", "%s connections" % num(conns) if conns is not None else "n/a",
                 "; ".join(reasons) or "max_connections 100 and 100 concurrent jobs not exceeded",
                 "Red Hat: move to an external PostgreSQL when a limit is exceeded. Set \"database\": \"external\" "
                 "in environments.json when it already is.", "major", "aap_topology")

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


# --------------------------------------------------------------------------- #
# Dashboard definition
# --------------------------------------------------------------------------- #
class AapDashboard(Dashboard):
    key = "aap"
    label = "Ansible Automation Platform"
    short = "AAP"
    script = "aap_health_dashboard.py"
    blurb = "Platform gateway, automation controller, Event-Driven Ansible and automation hub (AAP APIs only)"
    components = ("gateway", "controller", "eda", "hub")
    requires = ("aap_url",)
    collector = AapCollector
    env_chips = (("Controller", "controller_version"), ("API user", "api_user"), ("Redis", "redis_mode"),
                 ("Subscription", "subscription"))

    def headline(self, col, m, settings):
        return [("Job success, 24h", num(m.get("ctl.success_rate_24h"), "%")),
                ("Jobs, 24h", num(m.get("ctl.jobs_24h"))),
                ("Est. hours saved, 30d", num(m.get("est_hours_saved_30d"))),
                ("Peak capacity used", num(m.get("ctl.capacity_used_pct"), "%")),
                ("Subscription days left", num(m.get("ctl.license_days_left"))),
                ("Controller", col.info.get("controller_version") or "n/a")]

    def hero_cards(self, snaps, cfg):
        t, settings = cfg["kpi_targets"], cfg["settings"]
        S = lambda k: sum(s["metrics"].get(k) or 0 for s in snaps)  # noqa: E731
        scores = [s["score"] for s in snaps if s["score"] is not None]
        avg = round(statistics.mean(scores), 1) if scores else None
        succ, failed = S("ctl.jobs_success_24h"), S("ctl.jobs_failed_24h")
        rate = pct(succ, succ + failed)
        crit = S("open_critical")
        one = len(snaps) == 1
        return [status_card(snaps, "AAP status"),
                card(num(avg), "AAP health score" if one else "Average AAP health score",
                     vs_target(avg, t["health_score"]), "target %s" % t["health_score"]),
                card(num(rate, "%"), "Job success rate, 24h", vs_target(rate, t["job_success_pct"]),
                     "target %s%%" % t["job_success_pct"]),
                card(num(S("ctl.jobs_24h")), "Jobs run in the last 24h"),
                card(num(round(S("est_hours_saved_30d"))), "Estimated hours saved, 30d", "",
                     "%s min per successful job (estimate)" % settings["minutes_saved_per_job"]),
                card(num(crit), "Open critical AAP issues", "bad" if crit else "good", "target 0")]

    scorecard_headers = ("Success 24h", "Jobs 24h", "Capacity used", "Subscription days", "Controller")
    scorecard_nums = (0, 1, 2, 3)

    def scorecard_cells(self, s, cfg):
        M = s["metrics"]
        return [num(M.get("ctl.success_rate_24h"), "%"), num(M.get("ctl.jobs_24h")),
                num(M.get("ctl.capacity_used_pct"), "%"), num(M.get("ctl.license_days_left")),
                esc(s["info"].get("controller_version") or "n/a")]

    def kpi_rows(self, snaps, hist, dates30, cfg):
        t, th = cfg["kpi_targets"], cfg["thresholds"]
        M = lambda s, k: s["metrics"].get(k)  # noqa: E731
        return common_kpis(self, snaps, hist, dates30, cfg) + [
            ("Job success rate, 24h", ">= %s%%" % t["job_success_pct"],
             [kcell(M(s, "ctl.success_rate_24h"), t["job_success_pct"], True, "%") for s in snaps]),
            ("Job success rate, 30d", ">= %s%%" % t["job_success_pct"],
             [kcell(M(s, "ctl.success_rate_30d"), t["job_success_pct"], True, "%") for s in snaps]),
            ("Jobs run, 30d", "", [kcell(M(s, "ctl.jobs_30d")) for s in snaps]),
            ("Est. hours saved, 30d", "estimate", [kcell(M(s, "est_hours_saved_30d")) for s in snaps]),
            ("Peak capacity used", "<= %s%%" % t["capacity_used_pct"],
             [kcell(M(s, "ctl.capacity_used_pct"), t["capacity_used_pct"], False, "%") for s in snaps]),
            ("Managed hosts / licensed", "",
             ["%s / %s" % (num(M(s, "ctl.hosts_used")), num(M(s, "ctl.hosts_licensed"))) for s in snaps]),
            ("Subscription days left", ">= %s" % t["license_days_left"],
             [kcell(M(s, "ctl.license_days_left"), t["license_days_left"]) for s in snaps]),
            ("Active users, 30d", "", [kcell(M(s, "gw.users_active_30d")) for s in snaps]),
            ("EDA activations running / enabled", "",
             ["%s / %s" % (num(M(s, "eda.activations_running")), num(M(s, "eda.activations_enabled")))
              for s in snaps]),
            ("EDA rules fired, 24h", "", [kcell(M(s, "eda.rules_fired_24h")) for s in snaps]),
            ("Hub storage used", "<= %s%%" % th["hub_storage_warn_pct"],
             [kcell(M(s, "hub.storage_used_pct"), th["hub_storage_warn_pct"], False, "%") for s in snaps]),
            ("Open critical issues", "= %s" % t["open_critical"],
             [kcell(M(s, "open_critical"), t["open_critical"], False) for s in snaps]),
        ]

    def charts(self, cfg):
        t, settings = cfg["kpi_targets"], cfg["settings"]
        return [
            ("AAP health score", "score", "", 100, t["health_score"], "Weighted share of AAP checks passing; "
             "dashed = target", "auto"),
            ("Job success rate, 24h", "ctl.success_rate_24h", "%", 100, t["job_success_pct"],
             "Successful / (successful + failed + error)", "auto"),
            ("Jobs per day", "ctl.jobs_24h", "", None, None, "Jobs finished in the 24h before each run", 0),
            ("Estimated hours saved per day", "est_hours_saved_24h", "h", None, None,
             "Successful jobs x %s min" % settings["minutes_saved_per_job"], 0),
            ("Active users, 30d", "gw.users_active_30d", "", None, None, "Users who logged in within 30 days", 0),
            ("Peak instance group capacity used", "ctl.capacity_used_pct", "%", 100, t["capacity_used_pct"],
             "Highest non-container instance group", 0),
        ]

    def env_cards(self, s, cfg):
        t, th = cfg["kpi_targets"], cfg["thresholds"]
        M = s["metrics"]
        return [
            card(num(M.get("ctl.success_rate_24h"), "%"), "Job success, 24h",
                 vs_target(M.get("ctl.success_rate_24h"), t["job_success_pct"]), "%s jobs" % num(M.get("ctl.jobs_24h"))),
            card(num(M.get("ctl.capacity_used_pct"), "%"), "Peak capacity used",
                 vs_target(M.get("ctl.capacity_used_pct"), t["capacity_used_pct"], False)),
            card(num(M.get("ctl.pending")), "Jobs pending now", "warn" if M.get("ctl.pending") else "good",
                 "oldest %s min" % num(M.get("ctl.oldest_pending_min")) if M.get("ctl.pending") else ""),
            card(num(M.get("ctl.event_lag_s"), "s"), "Job event lag"),
            card(num(M.get("eda.activations_running")), "EDA activations running", "",
                 "of %s enabled" % num(M.get("eda.activations_enabled"))),
            card(num(M.get("hub.storage_used_pct"), "%"), "Hub storage used",
                 vs_target(M.get("hub.storage_used_pct"), th["hub_storage_warn_pct"], False)),
        ]

    def env_charts(self, cfg):
        t = cfg["kpi_targets"]
        return [
            ("AAP health score", [("score", "score")], "", 100, t["health_score"], "auto"),
            ("Jobs per day", [("finished", "ctl.jobs_24h"), ("failed", "ctl.jobs_failed_24h")], "", None, None, 0),
            ("Job success rate", [("24h", "ctl.success_rate_24h"), ("30d", "ctl.success_rate_30d")], "%", 100,
             t["job_success_pct"], "auto"),
            ("Capacity and queue", [("capacity used %", "ctl.capacity_used_pct"), ("pending jobs", "ctl.pending")],
             "", None, None, 0),
            ("Job duration (s)", [("average", "ctl.job_avg_s"), ("p95", "ctl.job_p95_s")], "s", None, None, 0),
            ("API latency (ms)", [("gateway", "gw.latency_ms"), ("controller", "ctl.latency_ms"),
                                  ("EDA", "eda.latency_ms"), ("hub", "hub.latency_ms")], "ms", None, None, 0),
            ("Job event lag (s)", [("callback lag", "ctl.event_lag_s")], "s", None, None, 0),
            ("Redis callback queue", [("events queued", "ctl.callback_queue")], "", None, None, 0),
            ("Database connections", [("controller connections", "ctl.db_connections")], "", None, None, 0),
            ("Event-Driven Ansible", [("running activations", "eda.activations_running"),
                                      ("failed processes", "eda.failed_instances_24h"),
                                      ("rules fired", "eda.rules_fired_24h")], "", None, None, 0),
            ("Automation hub", [("storage used %", "hub.storage_used_pct"), ("failed tasks", "hub.tasks_failed_24h"),
                                ("waiting tasks", "hub.tasks_waiting")], "", None, None, 0),
        ]

    def env_details(self, s):
        D = s["details"]
        return [
            ("Instance groups", ["Name", "Container group", "Capacity", "Used", "Used %", "Running"],
             [[esc(r["name"]), esc(r["container_group"]), num(r["capacity"]), num(r["used"]), num(r["used_pct"], "%"),
               num(r["running"])] for r in D.get("instance_groups") or []], (2, 3, 4, 5)),
            ("Instances", ["Hostname", "Type", "State", "Capacity", "Used", "Remaining %", "Jobs", "Last health check"],
             [[esc(r["hostname"]), esc(r["type"]), esc(r["state"]), num(r["capacity"]), num(r["used"]),
               num(r["remaining_pct"]), num(r["jobs_running"]), esc(r["last_health_check"])]
              for r in D.get("instances") or []], (3, 4, 5, 6)),
            ("Top failing job templates, 24h", ["Template", "Failures"],
             [[esc(n), num(c)] for n, c in D.get("top_failing_templates") or []], (1,)),
            ("Launch types, 24h", ["Launch type", "Jobs"],
             [[esc(k_), num(v)] for k_, v in sorted((D.get("launch_types") or {}).items(), key=lambda x: -x[1])],
             (1,)),
            ("EDA activations", ["Name", "Enabled", "Status", "Restarts", "Message"],
             [[esc(r["name"]), esc(r["enabled"]), esc(r["status"]), num(r["restarts"]), esc(r["message"])]
              for r in D.get("eda_activations") or []], (3,)),
        ]


def main(argv=None):
    return run_dashboard(AapDashboard(), argv)


if __name__ == "__main__":
    sys.exit(main())
