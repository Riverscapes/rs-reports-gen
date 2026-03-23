"""
EPA ATTAINS API exploration script.
Tests connectivity to the ATTAINS public API endpoints.

Key findings (March 2026):
- /domains     : works fine (51 domain types)
- /huc12summary: returns HTTP 500 or times out -- appears to be a server-side outage
- /assessments : works with correct params (organizationId required for scoped queries)

API docs: https://www.epa.gov/waterdata/get-data-access-public-attains-apis
Copilot-written.
"""

import json
import time

import requests

BASE_URL = "https://attains.epa.gov/attains-public/api/"

session = requests.Session()
session.headers.update({"Accept": "application/json", "User-Agent": "Mozilla/5.0"})


def test_endpoint(path: str, params: dict | None = None, timeout: int = 20) -> None:
    """Call one ATTAINS endpoint and print a summary of the response. Copilot-written."""
    url = BASE_URL + path
    qs = ("?" + "&".join(f"{k}={v}" for k, v in (params or {}).items())) if params else ""
    print(f"  {url}{qs}")
    t0 = time.time()
    try:
        r = session.get(url, params=params, timeout=timeout)
        elapsed = time.time() - t0
        print(f"    -> {r.status_code}  {len(r.content):,} bytes  {elapsed:.1f}s")
        if r.status_code == 200:
            print(json.dumps(r.json(), indent=2)[:600])
        else:
            print(f"    Error body: {r.text[:300]}")
    except Exception as e:
        elapsed = time.time() - t0
        print(f"    -> EXCEPTION after {elapsed:.1f}s: {type(e).__name__}: {e}")


# --- /domains ---
print("\n=== /domains ===")
r = session.get(BASE_URL + "domains", timeout=15)
domains = r.json()
print(f"Status: {r.status_code}  Total domain types: {len(domains)}")
print("Domain names:", [d["domain"] for d in domains[:10]], "...")

# --- /huc12summary (documented in 2020 EPA doc) ---
print("\n=== /huc12summary ===")
for huc in ["020700100204", "031601030101", "170601040601"]:
    test_endpoint("huc12summary", {"huc": huc}, timeout=35)

# --- /assessments ---
print("\n=== /assessments ===")
# organizationId scopes the query; use Virginia as a small example
test_endpoint("assessments", {"organizationId": "VASWCB", "assessmentUnitIdentifier": "VAW-I01R_JKS01A00"})
# HUC8-level search within an org
test_endpoint("assessments", {"organizationId": "VASWCB", "huc": "02070010"})
