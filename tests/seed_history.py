#!/usr/bin/env python3
"""
End-to-end test: start the mock platform, run BOTH dashboard scripts (aap_health_dashboard.py
and ocp_health_dashboard.py) once per day for N days with --as-of so every chart has history,
then validate the output.

  python3 tests/seed_history.py --days 60 --out /tmp/aap-health-site [--redact] [--all-envs]

By default the config mirrors production use: three environments are defined but
settings.only_tiers = ["prod"], so only Production is checked. --all-envs runs all three
(healthy prod, degraded nonprod, restricted dev) to exercise failure and RBAC paths.

Exit code 0 when every assertion passes.
"""
import argparse
import json
import os
import ssl
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from urllib import request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
import aap_health_dashboard as aap_dash  # noqa: E402
import ocp_health_dashboard as ocp_dash  # noqa: E402

AAP_COMPS = set(aap_dash.AapDashboard.components)
OCP_COMPS = set(ocp_dash.OcpDashboard.components)


def wait_up(port, cafile):
    ctx = ssl.create_default_context(cafile=cafile)
    for _ in range(50):
        try:
            request.urlopen("https://127.0.0.1:%d/_mock/day?date=2026-01-01" % port, context=ctx, timeout=2).read()
            return ctx
        except Exception:  # noqa: BLE001
            time.sleep(0.2)
    raise SystemExit("mock on port %d did not start" % port)


def config(port, short_port, all_envs=False):
    def env(name, label, tier, prom=True, route_port=port):
        e = {"name": name, "label": label, "tier": tier,
             "aap_url": "https://127.0.0.1:%d/%s" % (port, name), "aap_token_env": "AAP_TOKEN_%s" % name.upper(),
             "ocp_api_url": "https://127.0.0.1:%d/ocp-%s" % (port, name), "ocp_token_env": "OCP_TOKEN_%s" % name.upper(),
             "namespace": "aap", "aap_cr_name": "aap", "ca_bundle": str(HERE / "certs" / "server.pem"),
             "route_tls_port": route_port}
        if prom:
            e["prometheus_url"] = "https://127.0.0.1:%d/prom-%s" % (port, name)
        return e
    return {"dashboard_title": "AAP 2.6 Platform Health (mock)", "report_tz": "America/Chicago",
            "minutes_saved_per_job": 15,
            "settings": {"only_tiers": [] if all_envs else ["prod"]},
            "environments": [dict(env("prod", "Production", "prod"), database="external"),
                             env("nonprod", "Non-production", "nonprod", route_port=short_port),
                             dict(env("dev", "Development", "dev", prom=False), hub_enabled=False)]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--out", default="/tmp/aap-health-site")
    ap.add_argument("--port", type=int, default=19443)
    ap.add_argument("--redact", action="store_true")
    ap.add_argument("--end", default=None, help="last day YYYY-MM-DD (default today)")
    ap.add_argument("--all-envs", action="store_true", help="check nonprod and dev too (regression coverage)")
    args = ap.parse_args()
    if not (HERE / "certs" / "server.pem").exists():
        subprocess.check_call([str(HERE / "make_certs.sh")])
    short_port = args.port + 1
    procs = [subprocess.Popen([sys.executable, str(HERE / "mock_platform.py"), "--port", str(p), "--cert",
                               str(HERE / "certs" / (c + ".pem")), "--key", str(HERE / "certs" / (c + ".key"))])
             for p, c in ((args.port, "server"), (short_port, "short"))]
    try:
        ctx = wait_up(args.port, str(HERE / "certs" / "server.pem"))
        wait_up(short_port, str(HERE / "certs" / "short.pem"))
        cfg_path = Path(args.out + ".config.json")
        cfg_path.write_text(json.dumps(config(args.port, short_port, args.all_envs), indent=2))
        for n in ("PROD", "NONPROD", "DEV"):
            os.environ["AAP_TOKEN_" + n] = "mock-read-only-token"
        os.environ["AAP_HEALTH_SECRETS_JSON"] = json.dumps({"OCP_TOKEN_PROD": "sa-token", "OCP_TOKEN_NONPROD": "sa-token",
                                                            "OCP_TOKEN_DEV": "sa-token"})
        end = date.fromisoformat(args.end) if args.end else date.today()
        codes = {"aap": [], "openshift": []}
        t0 = time.time()
        for i in range(args.days - 1, -1, -1):
            d = (end - timedelta(days=i)).isoformat()
            request.urlopen("https://127.0.0.1:%d/_mock/day?date=%s" % (args.port, d), context=ctx).read()
            argv = ["-c", str(cfg_path), "-o", args.out, "--as-of", d, "-q"] + (["--redact"] if args.redact else [])
            codes["aap"].append(aap_dash.main(argv))
            codes["openshift"].append(ocp_dash.main(argv))
        print("ran %d days in %.1fs, exit codes %s" % (args.days, time.time() - t0,
                                                      {k: sorted(set(v)) for k, v in codes.items()}))
        # ---- assertions ----
        out = Path(args.out)
        expected = {"prod", "nonprod", "dev"} if args.all_envs else {"prod"}
        res = {}
        for key, comps, other in (("aap", AAP_COMPS, OCP_COMPS), ("openshift", OCP_COMPS, AAP_COMPS)):
            base = out / key
            hist = json.loads((base / "history.json").read_text())
            latest = json.loads((base / "data" / "latest.json").read_text())
            assert latest["dashboard"] == key
            assert len(hist["days"]) == args.days, (key, len(hist["days"]))
            envs = {e["name"]: e for e in latest["environments"]}
            assert set(envs) == expected, (key, set(envs))
            for e in envs.values():
                leaked = {c["component"] for c in e["checks"]} & other
                assert not leaked, ("%s script produced %s checks" % (key, leaked))
                assert {c["component"] for c in e["checks"]} <= comps
                assert (base / "env" / (e["name"] + ".html")).exists()
            assert envs["prod"]["status"] in ("green", "amber"), (key, envs["prod"]["status"])
            assert envs["prod"]["score"] is not None
            assert all(c in (0, 1) for c in codes[key]), (key, codes[key])
            assert (base / "index.html").stat().st_size > 15000
            assert (base / "data" / "checks_latest.csv").exists()
            if not args.all_envs:
                for e in ("nonprod", "dev"):
                    assert not (base / "env" / (e + ".html")).exists(), "non-production must not be checked"
            res[key] = envs
        a_ids = {c["id"]: c for c in res["aap"]["prod"]["checks"]}
        o_ids = {c["id"]: c for c in res["openshift"]["prod"]["checks"]}
        for must in ("controller.event_lag", "eda.activations", "hub.status", "gateway.status",
                     "controller.operator_db_load"):
            assert must in a_ids, must
        for must in ("cluster.etcd_fsync", "cluster.alerts", "platform.route_tls", "platform.backup",
                     "platform.operator_db_limits", "cluster.operators", "cluster.mcp", "cluster.csr",
                     "platform.pod_spread"):
            assert must in o_ids, must
        if args.all_envs:
            for key in res:
                assert res[key]["nonprod"]["status"] in ("red", "amber"), key
            assert res["openshift"]["prod"]["score"] > res["openshift"]["nonprod"]["score"]
            d_o = {c["id"]: c for c in res["openshift"]["dev"]["checks"]}
            d_a = {c["id"]: c for c in res["aap"]["dev"]["checks"]}
            assert d_o["cluster.nodes_ready"]["status"] == "unknown", "dev has no cluster-reader -> unknown"
            assert d_a["gateway.least_privilege"]["status"] == "warn", "dev uses a superuser token"
            n_o = {c["id"]: c for c in res["openshift"]["nonprod"]["checks"]}
            n_a = {c["id"]: c for c in res["aap"]["nonprod"]["checks"]}
            assert n_o["platform.route_tls"]["status"] == "warn", n_o["platform.route_tls"]
            assert n_o["platform.backup"]["status"] == "warn"
            assert n_a["eda.activations"]["status"] in ("fail", "ok")
            assert n_a["controller.operator_db_load"]["status"] == "warn", "operator DB over 100 concurrent jobs"
        assert a_ids["controller.operator_db_load"]["value"] == "external database"
        # separate pages: AAP content only on aap/, OpenShift content only on openshift/
        aap_html = (out / "aap" / "index.html").read_text()
        ocp_html = (out / "openshift" / "index.html").read_text()
        assert "Job success rate, 24h" in aap_html and "Job success rate" not in ocp_html
        assert "etcd WAL fsync" in ocp_html and "etcd" not in aap_html.split("<footer>")[0]
        import html as _h
        a_titles = {c["title"] for c in res["aap"]["prod"]["checks"]}
        o_titles = {c["title"] for c in res["openshift"]["prod"]["checks"]}
        a_env = (out / "aap" / "env" / "prod.html").read_text()
        o_env = (out / "openshift" / "env" / "prod.html").read_text()
        assert not [t for t in o_titles - a_titles if ">%s<" % _h.escape(t) in a_env], "OpenShift check on AAP page"
        assert not [t for t in a_titles - o_titles if ">%s<" % _h.escape(t) in o_env], "AAP check on OpenShift page"
        landing = (out / "index.html").read_text()
        assert 'href="aap/index.html"' in landing and 'href="openshift/index.html"' in landing, "landing links"
        assert (out / ".nojekyll").exists()
        if args.redact:
            blob = "".join((out / f).read_text() for f in (
                "aap/data/latest.json", "openshift/data/latest.json", "index.html", "aap/index.html",
                "openshift/index.html", "aap/env/prod.html", "openshift/env/prod.html"))
            assert "ocp.example.com" not in blob and "127.0.0.1" not in blob, "redaction leaked a hostname"
        total = sum(len(e["checks"]) for r in res.values() for e in r.values())
        print("OK - %d checks; %s" % (total, {k: {n: (e["status"], e["score"]) for n, e in r.items()}
                                               for k, r in res.items()}))
        return 0
    finally:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    sys.exit(main())
