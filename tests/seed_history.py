#!/usr/bin/env python3
"""
End-to-end test: start the mock platform, run the dashboard once per day for N days with
--as-of so every chart has history, then validate the output.

  python3 tests/seed_history.py --days 60 --out /tmp/aap-health-site [--redact]

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
import aap_health_dashboard as dash  # noqa: E402


def wait_up(port, cafile):
    ctx = ssl.create_default_context(cafile=cafile)
    for _ in range(50):
        try:
            request.urlopen("https://127.0.0.1:%d/_mock/day?date=2026-01-01" % port, context=ctx, timeout=2).read()
            return ctx
        except Exception:  # noqa: BLE001
            time.sleep(0.2)
    raise SystemExit("mock on port %d did not start" % port)


def config(port, short_port):
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
            "environments": [env("prod", "Production", "prod"),
                             env("nonprod", "Non-production", "nonprod", route_port=short_port),
                             dict(env("dev", "Development", "dev", prom=False), hub_enabled=False)]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--out", default="/tmp/aap-health-site")
    ap.add_argument("--port", type=int, default=19443)
    ap.add_argument("--redact", action="store_true")
    ap.add_argument("--end", default=None, help="last day YYYY-MM-DD (default today)")
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
        cfg_path.write_text(json.dumps(config(args.port, short_port), indent=2))
        for n in ("PROD", "NONPROD", "DEV"):
            os.environ["AAP_TOKEN_" + n] = "mock-read-only-token"
        os.environ["AAP_HEALTH_SECRETS_JSON"] = json.dumps({"OCP_TOKEN_PROD": "sa-token", "OCP_TOKEN_NONPROD": "sa-token",
                                                            "OCP_TOKEN_DEV": "sa-token"})
        end = date.fromisoformat(args.end) if args.end else date.today()
        codes = []
        t0 = time.time()
        for i in range(args.days - 1, -1, -1):
            d = (end - timedelta(days=i)).isoformat()
            request.urlopen("https://127.0.0.1:%d/_mock/day?date=%s" % (args.port, d), context=ctx).read()
            argv = ["-c", str(cfg_path), "-o", args.out, "--as-of", d, "-q"] + (["--redact"] if args.redact else [])
            codes.append(dash.main(argv))
        print("ran %d days in %.1fs, exit codes %s" % (args.days, time.time() - t0, sorted(set(codes))))
        # ---- assertions ----
        out = Path(args.out)
        hist = json.loads((out / "history.json").read_text())
        latest = json.loads((out / "data" / "latest.json").read_text())
        assert len(hist["days"]) == args.days, len(hist["days"])
        envs = {e["name"]: e for e in latest["environments"]}
        assert set(envs) == {"prod", "nonprod", "dev"}
        assert envs["prod"]["status"] in ("green", "amber"), envs["prod"]["status"]
        assert envs["nonprod"]["status"] in ("red", "amber")
        assert envs["prod"]["score"] > envs["nonprod"]["score"]
        ids = {c["id"]: c for c in envs["dev"]["checks"]}
        assert ids["cluster.nodes_ready"]["status"] == "unknown", "dev has no cluster-reader -> unknown"
        assert ids["gateway.least_privilege"]["status"] == "warn", "dev uses a superuser token"
        n_ids = {c["id"]: c for c in envs["nonprod"]["checks"]}
        for must in ("cluster.etcd_fsync", "cluster.alerts", "platform.route_tls", "platform.backup",
                     "controller.event_lag", "eda.activations", "hub.status", "platform.operator_db_limits",
                     "cluster.operators", "cluster.mcp", "cluster.csr", "platform.pod_spread"):
            assert must in n_ids, must
        assert n_ids["platform.route_tls"]["status"] == "warn", n_ids["platform.route_tls"]
        assert n_ids["platform.backup"]["status"] == "warn"
        assert n_ids["eda.activations"]["status"] in ("fail", "ok")
        for e in ("prod", "nonprod", "dev"):
            assert (out / "env" / (e + ".html")).exists()
        assert (out / "index.html").stat().st_size > 20000
        assert (out / ".nojekyll").exists() and (out / "data" / "checks_latest.csv").exists()
        assert all(c in (0, 1) for c in codes), codes
        if args.redact:
            blob = (out / "data" / "latest.json").read_text() + (out / "index.html").read_text()
            assert "ocp.example.com" not in blob and "127.0.0.1" not in blob, "redaction leaked a hostname"
        total = sum(len(e["checks"]) for e in latest["environments"])
        print("OK - %d checks across %d environments; statuses %s" % (
            total, len(envs), {k: (v["status"], v["score"]) for k, v in envs.items()}))
        return 0
    finally:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    sys.exit(main())
