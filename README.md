# FDA Compliance Demo

End-to-end Databricks demo: ingest FDA regulatory data through a medallion pipeline, serve it from Lakebase, and visualize it in a compliance dashboard.

---

## How It Works

```
openFDA API --> UC Volume (raw JSON)
            --> Declarative Pipeline: Bronze --> Silver --> Gold
            --> Lakebase (synced tables, optional)
            --> Databricks App (compliance dashboard)
```

1. **Ingest**: Fetches 1,000 records each from three openFDA endpoints into a Unity Catalog volume.
2. **Transform**: A Spark Declarative Pipeline reads raw JSON, flattens nested structures, applies quality expectations, and produces gold-layer analytics tables.
3. **Serve**: Gold tables (with Change Data Feed enabled) can sync to Lakebase for low-latency operational queries.
4. **Visualize**: A FastAPI app queries gold tables via the Statement Execution API and renders a compliance dashboard.

### Data Sources

| Endpoint | Bronze Table | Gold Table | Records |
|---|---|---|---|
| `/drug/ndc.json` | `raw_ndc_products` | `gold_product_catalog` | ~1,000 |
| `/food/enforcement.json` | `raw_enforcement_actions` | `gold_enforcement_actions` | ~1,000 |
| `/drug/event.json` | `raw_adverse_events` | `gold_adverse_events_summary` | ~8,000 (aggregated) |

### Pipeline Architecture

- **Bronze**: Raw JSON records with ingestion metadata
- **Silver**: Flattened structures, parsed dates, deduplication, quality expectations (`@dp.expect_or_drop`)
- **Gold**: Aggregated analytics tables with CDF enabled for Lakebase sync

---

## Quick Start (No Local Tools Required)

### 1. Import the Repo

In your Databricks workspace: **Repos > Add Repo** > paste:

```
https://github.com/ashwinpo/fda-compliance-demo
```

### 2. Open the Setup Notebook

Open `setup` in the imported repo.

### 3. Fill in the Widgets

| Widget | Description |
|---|---|
| **Catalog** | Unity Catalog catalog to use (default: `main`) |
| **SQL Warehouse ID** | Find in SQL Warehouses > Connection Details |
| **Schema** | Schema name (default: `fda_demo`) |
| **App Name** | App name (default: `fda-compliance`) |

### 4. Run All

Click **Run All**. The notebook will:
1. Create Unity Catalog resources (schema, volume)
2. Fetch live data from the openFDA API
3. Run a Declarative Pipeline (bronze > silver > gold)
4. Deploy a compliance dashboard app
5. Grant all required permissions

Total time: ~10 minutes. The final cell displays the app URL.

---

## Alternative: CLI Deploy

If you prefer deploying from your terminal (requires Databricks CLI + Python):

```bash
cp setup.yaml.example setup.yaml
# Edit setup.yaml with your values
./deploy.sh
```

See `deploy.sh --help` for flags like `--skip-data` and `--skip-pipeline`.

---

## Lakebase Sync (Optional)

The gold tables have Change Data Feed enabled. To sync to Lakebase:

1. Go to **Catalog > Lakebase > New Project**
2. Create a database within the project
3. Create synced tables from:
   - `{catalog}.{schema}.gold_product_catalog` (PK: `product_ndc`)
   - `{catalog}.{schema}.gold_enforcement_actions` (PK: `recall_number`)
   - `{catalog}.{schema}.gold_adverse_events_summary` (PK: `product_name, reaction_name`)
4. Use **triggered** sync mode

---

## Project Structure

```
setup.py                 # One-click setup notebook (Run All — no local tools needed)

pipeline/
  fda_pipeline.py        # Declarative Pipeline notebook (bronze > silver > gold)
  fetch_fda_data.py      # Standalone data fetch notebook (used by setup.py)

app/
  app.py                 # FastAPI dashboard (compliance monitor)
  requirements.txt       # Python dependencies

deploy.sh                # CLI deploy script (alternative to setup notebook)
setup.yaml.example       # CLI config template
```

---

## Cleanup

To remove all resources created by this demo:

```sql
-- Run in SQL Editor
DROP SCHEMA <catalog>.<schema> CASCADE;
```

```
-- From the workspace UI or CLI
-- Delete the app: Compute > Apps > fda-compliance > Delete
-- Delete the pipeline: Workflows > Declarative Pipelines > FDA Compliance Pipeline > Delete
```
