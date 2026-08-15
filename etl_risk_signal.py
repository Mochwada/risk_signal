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
ACLED_EMAIL                  the email on your myACLED account
ACLED_PASSWORD                your myACLED account password
                             -- ACLED retired the old key+email query-param
                                scheme in 2025 in favor of OAuth. New accounts
                                (registered after ~Sept 2025) never get an
                                "API key" at all — you authenticate with your
                                actual account credentials to get a short-lived
                                token instead. See acleddata.com/api-documentation.
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

# ACLED's `country` query filter matches on the full name, not ISO3 — see
# https://acleddata.com/api-documentation/getting-started. Double-check
# "Democratic Republic of Congo" against ACLED's own country list if that
# one comes back empty; it's the one name most likely to differ slightly.
ACLED_COUNTRY_NAME = {
    "KEN": "Kenya",
    "UGA": "Uganda",
    "TZA": "Tanzania",
    "ETH": "Ethiopia",
    "SOM": "Somalia",
    "SSD": "South Sudan",
    "RWA": "Rwanda",
    "BDI": "Burundi",
    "COD": "Democratic Republic of Congo",
}


def _raise_with_body(resp: requests.Response) -> None:
    """
    Same as resp.raise_for_status(), but logs the response body first.
    A bare '400 Client Error' with no body, like the one that came out of
    this run, isn't diagnosable on its own — this makes sure the next
    failure (whatever it turns out to be) shows its actual reason in the
    Actions log instead of needing a screenshot round-trip to figure out.
    """
    if not resp.ok:
        log.error("HTTP %d from %s: %s", resp.status_code, resp.url, resp.text[:500])
    resp.raise_for_status()


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
    _raise_with_body(resp)


def fetch_unhcr_displacement(current_year: int) -> int:
    """
    Pull population figures (refugees, asylum seekers, IDPs, stateless)
    for the tracked countries from UNHCR's open population API.

    No API key required. Queries the current year plus the two before it,
    not just the current year — UNHCR's annual figures are typically
    published with a lag, so the current year alone often comes back
    empty (exactly what happened on the first run: 0 rows). Querying a
    small range is a cheap way to get whatever's actually published
    without needing to know the exact cutoff, and it's strictly better
    for a dashboard anyway — more years of trend data, not less.
    """
    rows = []
    for iso3 in COUNTRIES:
        for year in (current_year, current_year - 1, current_year - 2):
            url = "https://api.unhcr.org/population/v1/population/"
            params = {"coa": iso3, "cf_type": "ISO", "year": year, "limit": 200}
            r = requests.get(url, params=params, headers={"Accept": "application/json"}, timeout=30)
            _raise_with_body(r)
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
    if not rows:
        log.warning("UNHCR returned 0 rows across all 3 years — if this repeats, "
                     "the endpoint or response shape likely needs a closer look, "
                     "not just a wider year range.")
    return len(rows)


def get_acled_access_token() -> str:
    """
    ACLED replaced the old key+email query-param scheme with OAuth in 2025
    (see acleddata.com/api-documentation/getting-started). This exchanges
    your actual myACLED email + password for a short-lived bearer token —
    there's no separate static "API key" to generate anymore.
    """
    resp = requests.post(
        "https://acleddata.com/oauth/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "username": os.environ["ACLED_EMAIL"],
            "password": os.environ["ACLED_PASSWORD"],
            "grant_type": "password",
            "client_id": "acled",
            "scope": "authenticated",
        },
        timeout=30,
    )
    _raise_with_body(resp)  # safe to log here: a failed token response describes
                             # the error (e.g. invalid_grant), it doesn't echo
                             # back the username/password that were submitted
    return resp.json()["access_token"]  # valid 24h; we just fetch a fresh one each run


def fetch_acled_events(days_back: int = 90) -> int:
    """
    Pull recent conflict/protest events for the tracked countries from ACLED,
    using the current OAuth-authenticated API. Requires ACLED_EMAIL and
    ACLED_PASSWORD for a free myACLED account.
    """
    token = get_acled_access_token()
    since = (date.today() - timedelta(days=days_back)).isoformat()
    today = date.today().isoformat()

    rows = []
    for iso3 in COUNTRIES:
        url = "https://acleddata.com/api/acled/read"
        params = {
            "_format": "json",
            "country": ACLED_COUNTRY_NAME[iso3],
            "event_date": f"{since}|{today}",
            "event_date_where": "BETWEEN",
            "limit": 500,
        }
        r = requests.get(
            url, params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        _raise_with_body(r)
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
            "model": "claude-sonnet-5",
            "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    _raise_with_body(resp)
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

    current_year = date.today().year
    fetch_unhcr_displacement(current_year)

    if os.environ.get("ACLED_EMAIL") and os.environ.get("ACLED_PASSWORD"):
        fetch_acled_events()
    else:
        log.info("ACLED_EMAIL / ACLED_PASSWORD not set — skipping conflict events "
                  "(sign up free at https://acleddata.com)")

    if os.environ.get("ANTHROPIC_API_KEY"):
        period = date.today().strftime("%Y-%m")
        for iso3 in COUNTRIES:
            generate_country_brief(iso3, period)


if __name__ == "__main__":
    main()
