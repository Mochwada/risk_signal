"""
Risk Signal: East Africa — ETL
================================
Companion pipeline to "Funding Signal: East Africa". Pulls displacement
data (UNHCR) and conflict/protest event data (ACLED) for a set of East
African / Horn of Africa countries and loads them into Supabase. Can
optionally generate a short AI narrative brief per country via the
Claude API.

Environment variables required
-------------------------------
SUPABASE_URL                e.g. https://dqgwryvbxhlyreytctxg.supabase.co
SUPABASE_SERVICE_ROLE_KEY   service role key (Project Settings > API)
                             -- NOT the anon/public key, this one bypasses RLS
ACLED_API_KEY                from https://acleddata.com (free registration)
ACLED_EMAIL                  the email you registered with
ANTHROPIC_API_KEY            optional — only needed for AI country briefs

Usage
-----
    python etl_risk_signal.py

Designed to be run on a schedule (see risk_signal_etl.yml for a GitHub
Actions workflow that runs this daily).
"""

import os
import sys
import logging
from datetime import date, timedelta

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("risk_signal_etl")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# The countries this project tracks — matches the seed data in the schema.
COUNTRIES = ["KEN", "UGA", "TZA", "ETH", "SOM", "SSD", "RWA", "BDI", "COD"]


def supabase_upsert(table: str, rows: list[dict], on_conflict: str) -> None:
    """Upsert a batch of rows into a Supabase table via the PostgREST API."""
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    resp = requests.post(url, json=rows, headers=headers, timeout=30)
    resp.raise_for_status()


def fetch_unhcr_displacement(year: int) -> int:
    """
    Pull population figures (refugees, asylum seekers, IDPs, stateless)
    for the tracked countries from UNHCR's open population API.

    No API key required. Verify the exact path against
    https://api.unhcr.org/docs/refugee-statistics.html if this ever 404s —
    UNHCR lists a `population` resource alongside `demographics`,
    `asylum-applications`, etc. under /population/v1/.
    """
    rows = []
    for iso3 in COUNTRIES:
        url = "https://api.unhcr.org/population/v1/population/"
        params = {"coa": iso3, "cf_type": "ISO", "year": year, "limit": 200}
        r = requests.get(url, params=params, headers={"Accept": "application/json"}, timeout=30)
        r.raise_for_status()
        items = r.json().get("items", [])
        for row in items:
            ptype = row.get("population_type") or row.get("ptype") or "unknown"
            count = row.get("individuals")
            if count is None:
                count = row.get("value", 0)
            rows.append(
                {
                    "country_iso3": iso3,
                    "year": year,
                    "population_type": ptype,
                    "population_count": count,
                    "source": "UNHCR",
                }
            )
    supabase_upsert("risk_displacement_stats", rows, "country_iso3,year,population_type")
    log.info("UNHCR: upserted %d displacement rows", len(rows))
    return len(rows)


def fetch_acled_events(days_back: int = 90) -> int:
    """
    Pull recent conflict/protest events for the tracked countries from ACLED.
    Requires a free myACLED account (ACLED_API_KEY + ACLED_EMAIL).
    Field names below follow ACLED's documented export format — sanity
    check them against your account's docs/Postman collection once you
    have a key, since access tiers occasionally change field naming.
    """
    api_key = os.environ["ACLED_API_KEY"]
    email = os.environ["ACLED_EMAIL"]
    since = (date.today() - timedelta(days=days_back)).isoformat()

    rows = []
    for iso3 in COUNTRIES:
        url = "https://api.acleddata.com/acled/read"
        params = {
            "key": api_key,
            "email": email,
            "iso3": iso3,
            "event_date": since,
            "event_date_where": ">=",
            "limit": 500,
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        for e in r.json().get("data", []):
            rows.append(
                {
                    "acled_event_id": e.get("event_id_cnty"),
                    "country_iso3": iso3,
                    "event_date": e.get("event_date"),
                    "event_type": e.get("event_type"),
                    "sub_event_type": e.get("sub_event_type"),
                    "fatalities": int(e.get("fatalities") or 0),
                    "latitude": float(e["latitude"]) if e.get("latitude") else None,
                    "longitude": float(e["longitude"]) if e.get("longitude") else None,
                    "actor1": e.get("actor1"),
                }
            )
    supabase_upsert("risk_conflict_events", rows, "acled_event_id")
    log.info("ACLED: upserted %d conflict-event rows", len(rows))
    return len(rows)


def generate_country_brief(iso3: str, period: str) -> None:
    """
    Optional: ask Claude for a short, neutral situation brief per country,
    grounded only in the figures already sitting in Supabase — no open-ended
    generation. Skips silently if ANTHROPIC_API_KEY isn't set.
    """
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        log.info("ANTHROPIC_API_KEY not set — skipping AI briefs")
        return

    # Pull the figures that will ground the brief.
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    disp = requests.get(
        f"{SUPABASE_URL}/rest/v1/risk_displacement_stats",
        params={"country_iso3": f"eq.{iso3}", "order": "year.desc", "limit": 10},
        headers=headers, timeout=30,
    ).json()
    conflict = requests.get(
        f"{SUPABASE_URL}/rest/v1/risk_conflict_monthly",
        params={"country_iso3": f"eq.{iso3}", "order": "month.desc", "limit": 3},
        headers=headers, timeout=30,
    ).json()

    displacement_total = sum(d.get("population_count", 0) for d in disp)
    fatalities_90d = sum(c.get("fatalities_sum", 0) for c in conflict)
    events_90d = sum(c.get("event_count", 0) for c in conflict)

    prompt = (
        f"Write a neutral, three-sentence situation brief for {iso3} for {period}, "
        f"based only on this data — do not speculate beyond it:\n"
        f"- Displaced population (refugees/asylum-seekers/IDPs/stateless), latest figures: {displacement_total}\n"
        f"- Conflict events, last ~90 days: {events_90d}\n"
        f"- Fatalities from those events: {fatalities_90d}\n"
        f"Plain factual tone, no editorializing."
    )

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": anthropic_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    resp.raise_for_status()
    brief_text = resp.json()["content"][0]["text"]

    supabase_upsert(
        "risk_country_briefs",
        [
            {
                "country_iso3": iso3,
                "period": period,
                "brief_text": brief_text,
                "displacement_total": displacement_total,
                "conflict_events_90d": events_90d,
                "fatalities_90d": fatalities_90d,
            }
        ],
        "country_iso3,period",
    )
    log.info("Generated AI brief for %s / %s", iso3, period)


def main() -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set — aborting")
        sys.exit(1)

    year = date.today().year
    fetch_unhcr_displacement(year)

    if os.environ.get("ACLED_API_KEY") and os.environ.get("ACLED_EMAIL"):
        fetch_acled_events()
    else:
        log.info("ACLED_API_KEY / ACLED_EMAIL not set — skipping conflict events "
                  "(sign up free at https://acleddata.com)")

    if os.environ.get("ANTHROPIC_API_KEY"):
        period = date.today().strftime("%Y-%m")
        for iso3 in COUNTRIES:
            generate_country_brief(iso3, period)


if __name__ == "__main__":
    main()
