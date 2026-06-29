#!/usr/bin/env python3
"""
notify_slack.py
---------------
Reads mart_churn_alerts from DuckDB and posts a weekly digest to Slack.

Usage:
    python scripts/notify_slack.py \
        --db dev.duckdb \
        --webhook https://hooks.slack.com/services/YOUR/WEBHOOK/URL

Wire as a dbt on-run-end hook in dbt_project.yml:
    on-run-end:
      - "{{ run_query('select 1') }}"   # placeholder — invoke this script externally
      # or use dbt-slack package / dbt Elementary for native alerting

Environment variable alternative:
    export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
    python scripts/notify_slack.py --db dev.duckdb
"""

import argparse
import json
import os
import sys
import urllib.request
from datetime import date

try:
    import duckdb
except ImportError:
    sys.exit("duckdb Python package not found. Run: pip install duckdb")


PRIORITY_LABELS = {
    1: ":rotating_light: CRITICAL",
    2: ":warning: URGENT",
    3: ":red_circle: HIGH",
    4: ":large_yellow_circle: WATCH",
}


def fetch_alerts(db_path: str) -> list[dict]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute("""
            select
                account_id
                , plan_name
                , mrr
                , days_to_renewal
                , risk_score
                , risk_tier
                , alert_priority
                , top_risk_driver
                , usage_trend_pct
                , active_user_ratio
                , had_payment_failure_30d
                , p1_p2_tickets_30d
            from dev.marts.mart_churn_alerts
            order by alert_priority, risk_score desc
        """).fetchall()
        cols = [d[0] for d in con.description]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        con.close()


def format_message(alerts: list[dict]) -> dict:
    if not alerts:
        return {
            "text": f":white_check_mark: *Churn Alert Digest — {date.today()}*\nNo accounts require attention this week."
        }

    priority_groups: dict[int, list] = {}
    for a in alerts:
        priority_groups.setdefault(a["alert_priority"], []).append(a)

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"⚠️ Churn Alert Digest — {date.today()}"}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{len(alerts)} account(s)* require CSM attention this week."
            }
        },
        {"type": "divider"},
    ]

    for priority in sorted(priority_groups.keys()):
        label = PRIORITY_LABELS.get(priority, f"Priority {priority}")
        accs = priority_groups[priority]
        lines = [f"*{label}* ({len(accs)} account{'s' if len(accs) > 1 else ''})"]

        for a in accs:
            trend = f"{a['usage_trend_pct']*100:+.0f}%" if a['usage_trend_pct'] is not None else "n/a"
            util = f"{a['active_user_ratio']*100:.0f}%" if a['active_user_ratio'] is not None else "n/a"
            flags = []
            if a["had_payment_failure_30d"]:
                flags.append("payment fail")
            if a["p1_p2_tickets_30d"] and a["p1_p2_tickets_30d"] > 0:
                flags.append(f"{a['p1_p2_tickets_30d']} P1/P2 tickets")
            flag_str = f" | {', '.join(flags)}" if flags else ""
            lines.append(
                f"  • *{a['account_id']}* ({a['plan_name']}, ${a['mrr']:,.0f} MRR) — "
                f"{a['days_to_renewal']}d to renewal | score {a['risk_score']:.0f} | "
                f"driver: {a['top_risk_driver']} | usage {trend}, util {util}{flag_str}"
            )

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)}
        })
        blocks.append({"type": "divider"})

    return {"blocks": blocks}


def post_to_slack(webhook_url: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Slack returned {resp.status}: {resp.read()}")
    print(f"✓ Posted {len(payload.get('blocks', []))} blocks to Slack.")


def main():
    parser = argparse.ArgumentParser(description="Post churn alerts to Slack.")
    parser.add_argument("--db", default="dev.duckdb", help="Path to DuckDB file")
    parser.add_argument("--webhook", default=os.getenv("SLACK_WEBHOOK_URL"), help="Slack webhook URL")
    parser.add_argument("--dry-run", action="store_true", help="Print payload without posting")
    args = parser.parse_args()

    if not args.webhook and not args.dry_run:
        sys.exit("Error: provide --webhook or set SLACK_WEBHOOK_URL env var.")

    alerts = fetch_alerts(args.db)
    payload = format_message(alerts)

    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return

    post_to_slack(args.webhook, payload)


if __name__ == "__main__":
    main()
