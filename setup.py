# Databricks notebook source
# MAGIC %md
# MAGIC # FDA Compliance Demo — Setup
# MAGIC
# MAGIC **Run All** to deploy the complete demo. Fill in the two required widgets above, then click **Run All**.
# MAGIC
# MAGIC | What it does | Time |
# MAGIC |---|---|
# MAGIC | Creates Unity Catalog resources | ~10s |
# MAGIC | Fetches live data from openFDA | ~30s |
# MAGIC | Runs a Declarative Pipeline (bronze → silver → gold) | 2–5 min |
# MAGIC | Deploys a compliance dashboard app | 1–2 min |
# MAGIC | Grants permissions | ~10s |

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "1. Catalog")
dbutils.widgets.text("warehouse_id", "", "2. SQL Warehouse ID")
dbutils.widgets.text("schema", "fda_demo", "3. Schema")
dbutils.widgets.text("app_name", "fda-compliance", "4. App Name")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
app_name = dbutils.widgets.get("app_name").strip()

assert warehouse_id, "warehouse_id is required — find it in SQL Warehouses > Connection Details"

volume_path = f"/Volumes/{catalog}/{schema}/staging"
user_email = spark.sql("SELECT current_user()").first()[0]

# Derive paths from this notebook's location
_nb_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_nb_path = _nb_ctx.notebookPath().get()
repo_root = "/".join(_nb_path.split("/")[:-1])
pipeline_notebook = f"{repo_root}/pipeline/fda_pipeline"
app_deploy_dir = f"/Users/{user_email}/fda-compliance-demo/app"

print(f"Catalog:    {catalog}")
print(f"Schema:     {schema}")
print(f"Warehouse:  {warehouse_id}")
print(f"Volume:     {volume_path}")
print(f"Pipeline:   {pipeline_notebook}")
print(f"App:        {app_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Unity Catalog Resources

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`staging`")
print("Done — schema and volume ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Fetch FDA Data

# COMMAND ----------

import requests, json, time

def fetch_fda(endpoint, limit=1000):
    results, skip = [], 0
    while skip < limit:
        r = requests.get(
            f"https://api.fda.gov{endpoint}",
            params={"limit": 100, "skip": skip},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json().get("results", [])
        results.extend(data)
        if len(data) < 100:
            break
        skip += 100
        time.sleep(0.3)
    return results

for name, endpoint, filename in [
    ("NDC Products",        "/drug/ndc.json",         "ndc_products.json"),
    ("Enforcement Actions", "/food/enforcement.json",  "enforcement_actions.json"),
    ("Adverse Events",      "/drug/event.json",        "adverse_events.json"),
]:
    print(f"  Fetching {name}...", end="")
    data = fetch_fda(endpoint)
    dbutils.fs.put(f"{volume_path}/{filename}", json.dumps(data), overwrite=True)
    print(f" {len(data)} records")

print("Done — data uploaded to volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Declarative Pipeline

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import (
    PipelineLibrary,
    NotebookLibrary,
    PipelineCluster,
)

w = WorkspaceClient()

pipeline_name = "FDA Compliance Pipeline"
pipeline_id = None
for p in w.pipelines.list_pipelines():
    if p.name == pipeline_name:
        pipeline_id = p.pipeline_id
        break

pipeline_config = dict(
    name=pipeline_name,
    catalog=catalog,
    target=schema,
    development=True,
    channel="CURRENT",
    edition="ADVANCED",
    configuration={"volume_path": volume_path},
    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=pipeline_notebook))],
    clusters=[
        PipelineCluster(
            label="default",
            num_workers=0,
            spark_conf={"spark.master": "local[*]"},
        )
    ],
)

if pipeline_id:
    print(f"Updating existing pipeline ({pipeline_id})...")
    w.pipelines.update(pipeline_id=pipeline_id, **pipeline_config)
else:
    print("Creating pipeline...")
    result = w.pipelines.create(**pipeline_config)
    pipeline_id = result.pipeline_id
    print(f"  Created: {pipeline_id}")

print("Starting pipeline run (2-5 min)...")
w.pipelines.start_update(pipeline_id=pipeline_id)

import time as _t

for i in range(60):
    info = w.pipelines.get(pipeline_id=pipeline_id)
    if info.latest_updates:
        state = info.latest_updates[0].state.value
        if state == "COMPLETED":
            print("  Pipeline completed successfully")
            break
        elif state in ("FAILED", "CANCELED"):
            raise RuntimeError(
                f"Pipeline {state}. Check the pipeline UI for details: {pipeline_id}"
            )
    if i > 0 and i % 6 == 0:
        print(f"  ... running ({i * 5}s)")
    _t.sleep(5)
else:
    print("  Timed out after 5 min — pipeline may still be running. Check UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Dashboard App

# COMMAND ----------

import io
from databricks.sdk.service.workspace import ImportFormat

w.workspace.mkdirs(app_deploy_dir)

# Copy app source files from the repo into a workspace directory
for fname in ["app.py", "requirements.txt"]:
    src_path = f"{repo_root}/app/{fname}"
    try:
        content = w.workspace.download(src_path)
        w.workspace.upload(
            f"{app_deploy_dir}/{fname}", content, format=ImportFormat.AUTO, overwrite=True
        )
    except Exception:
        # Fallback: read from local filesystem (Git Folders set CWD to repo root)
        with open(f"app/{fname}", "rb") as f:
            w.workspace.upload(
                f"{app_deploy_dir}/{fname}",
                io.BytesIO(f.read()),
                format=ImportFormat.AUTO,
                overwrite=True,
            )
    print(f"  Copied {fname}")

# Generate app.yaml with the user's config
app_yaml = f"""\
command:
  - "python"
  - "app.py"

env:
  - name: PORT
    value: "8000"
  - name: DATABRICKS_WAREHOUSE_ID
    value: "{warehouse_id}"
  - name: CATALOG
    value: "{catalog}"
  - name: SCHEMA
    value: "{schema}"
"""
w.workspace.upload(
    f"{app_deploy_dir}/app.yaml",
    io.BytesIO(app_yaml.encode()),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print("  Generated app.yaml")

# Use REST API for app operations (SDK apps support varies by runtime version)
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_host = _ctx.apiUrl().get()
_token = _ctx.apiToken().get()
_headers = {"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}

# Create or reuse existing app
_app_resp = requests.get(f"{_host}/api/2.0/apps/{app_name}", headers=_headers)
if _app_resp.status_code == 200:
    print(f"App '{app_name}' exists — redeploying")
else:
    print(f"Creating app '{app_name}' (30-60s)...")
    _create = requests.post(
        f"{_host}/api/2.0/apps",
        headers=_headers,
        json={"name": app_name, "description": "FDA Compliance Monitor"},
    )
    _create.raise_for_status()
    print("  App created")
    # Wait for compute to be ready
    import time as _tw
    for _i in range(30):
        _app = requests.get(f"{_host}/api/2.0/apps/{app_name}", headers=_headers).json()
        if _app.get("compute_status", {}).get("state") == "ACTIVE":
            print("  Compute ready")
            break
        _tw.sleep(5)

# Deploy
print("Deploying app (1-2 min)...")
_deploy = requests.post(
    f"{_host}/api/2.0/apps/{app_name}/deployments",
    headers=_headers,
    json={"source_code_path": f"/Workspace{app_deploy_dir}"},
)
_deploy.raise_for_status()
_deploy_id = _deploy.json().get("deployment_id", "")

# Wait for deployment
import time as _tw2
for _i in range(36):
    _app = requests.get(f"{_host}/api/2.0/apps/{app_name}", headers=_headers).json()
    _app_state = _app.get("app_status", {}).get("state", "")
    if _app_state == "RUNNING":
        print("  App deployed and running")
        break
    if _i > 0 and _i % 6 == 0:
        print(f"  ... deploying ({_i * 5}s)")
    _tw2.sleep(5)
else:
    print("  App may still be starting — check the URL in a minute")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Permissions

# COMMAND ----------

_app = requests.get(f"{_host}/api/2.0/apps/{app_name}", headers=_headers).json()
sp_id = _app.get("service_principal_client_id", "")
app_url = _app.get("url", "")

if sp_id:
    # Warehouse CAN_USE
    requests.put(
        f"{_host}/api/2.0/permissions/sql/warehouses/{warehouse_id}",
        headers=_headers,
        json={"access_control_list": [{"service_principal_name": sp_id, "permission_level": "CAN_USE"}]},
    )
    print("  Warehouse CAN_USE granted")

    for grant_sql in [
        f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{sp_id}`",
        f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{schema}` TO `{sp_id}`",
        f"GRANT SELECT ON SCHEMA `{catalog}`.`{schema}` TO `{sp_id}`",
    ]:
        spark.sql(grant_sql)
        label = grant_sql.split("GRANT ")[1].split(" ON ")[0]
        print(f"  {label} granted")
else:
    print("  Warning: could not detect SP ID — grant permissions manually")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete

# COMMAND ----------

displayHTML(f"""
<div style="background:#1a1a2e; color:#e0e0e0; padding:24px; border-radius:12px; font-family:system-ui; max-width:600px;">
  <h2 style="color:#4fc3f7; margin-top:0;">FDA Compliance Demo — Live</h2>
  <p><strong>Dashboard:</strong> <a href="{app_url}" target="_blank" style="color:#81d4fa;">{app_url}</a></p>
  <p><strong>Pipeline:</strong> <code>{pipeline_id}</code></p>
  <p><strong>Tables:</strong> <code>{catalog}.{schema}.gold_*</code></p>
  <hr style="border-color:#333;">
  <p style="margin-bottom:4px;"><strong>Lakebase sync (optional):</strong></p>
  <ul style="margin-top:4px;">
    <li><code>{catalog}.{schema}.gold_product_catalog</code></li>
    <li><code>{catalog}.{schema}.gold_enforcement_actions</code></li>
    <li><code>{catalog}.{schema}.gold_adverse_events_summary</code></li>
  </ul>
</div>
""")
