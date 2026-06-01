# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch FDA Data
# MAGIC Run this notebook once to populate the staging volume with openFDA data.
# MAGIC
# MAGIC **Requires widget:** `volume_path` (e.g. `/Volumes/catalog/schema/staging`)

# COMMAND ----------

import requests, json, time

VOLUME = dbutils.widgets.get("volume_path")

def fetch(endpoint, limit=1000):
    results, skip = [], 0
    while skip < limit:
        r = requests.get(f"https://api.fda.gov{endpoint}", params={"limit": 100, "skip": skip}, timeout=30)
        r.raise_for_status()
        data = r.json().get("results", [])
        results.extend(data)
        if len(data) < 100:
            break
        skip += 100
        time.sleep(0.3)
    return results

endpoints = [
    ("NDC Products",        "/drug/ndc.json",         "ndc_products.json"),
    ("Enforcement Actions", "/food/enforcement.json",  "enforcement_actions.json"),
    ("Adverse Events",      "/drug/event.json",        "adverse_events.json"),
]

for name, ep, fname in endpoints:
    print(f"Fetching {name}...")
    data = fetch(ep)
    dbutils.fs.put(f"{VOLUME}/{fname}", json.dumps(data), overwrite=True)
    print(f"  + {name}: {len(data)} records")

print("\nDone — all data uploaded to volume.")
