#!/bin/bash
set -e

# ============================================================================
# GNC FDA Compliance Demo — Deploy Script
# ============================================================================
# Reads configuration from setup.yaml and deploys the full solution:
#   1. Creates Unity Catalog resources (catalog, schema, volume)
#   2. Fetches FDA data from the openFDA API and uploads to volume
#   3. Creates and runs a DLT pipeline (bronze -> silver -> gold)
#   4. Deploys a Databricks App (compliance dashboard)
#   5. Grants all required permissions
#
# Usage: ./deploy.sh [--setup-only] [--skip-data] [--skip-pipeline]
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SETUP_FILE="$SCRIPT_DIR/setup.yaml"

# --- Parse flags ---
SETUP_ONLY=false
SKIP_DATA=false
SKIP_PIPELINE=false
for arg in "$@"; do
  case $arg in
    --setup-only)     SETUP_ONLY=true ;;
    --skip-data)      SKIP_DATA=true ;;
    --skip-pipeline)  SKIP_PIPELINE=true ;;
    -h|--help)
      echo "Usage: ./deploy.sh [OPTIONS]"
      echo ""
      echo "Deploys the GNC FDA Compliance demo to a Databricks workspace."
      echo "Configuration is read from setup.yaml."
      echo ""
      echo "Options:"
      echo "  --setup-only      Generate config files without deploying"
      echo "  --skip-data       Skip FDA data fetch + upload (reuse existing data)"
      echo "  --skip-pipeline   Skip DLT pipeline creation and run"
      echo "  -h, --help        Show this help message"
      exit 0
      ;;
  esac
done

# --- Check prerequisites ---
if [ ! -f "$SETUP_FILE" ]; then
  echo "ERROR: setup.yaml not found."
  echo "Run: cp setup.yaml.example setup.yaml  — then fill in your values."
  exit 1
fi

if ! command -v databricks &> /dev/null; then
  echo "ERROR: Databricks CLI not found."
  echo "Install: https://docs.databricks.com/dev-tools/cli/install.html"
  exit 1
fi

if ! command -v python3 &> /dev/null; then
  echo "ERROR: python3 not found. Install Python 3.8+."
  exit 1
fi

# --- Parse setup.yaml ---
echo "Reading configuration from setup.yaml..."
eval $(python3 -c "
import yaml, sys
with open('$SETUP_FILE') as f:
    c = yaml.safe_load(f)
for k, v in c.items():
    if v is None or v == '':
        print(f'CFG_{k.upper()}=\"\"')
    elif isinstance(v, bool):
        print(f'CFG_{k.upper()}={\"true\" if v else \"false\"}')
    else:
        print(f'CFG_{k.upper()}=\"{v}\"')
")

# --- Validate required fields ---
ERRORS=""
[ -z "$CFG_WAREHOUSE_ID" ] && ERRORS="${ERRORS}\n  - warehouse_id is required"
[ -z "$CFG_CATALOG" ] && ERRORS="${ERRORS}\n  - catalog is required"
[ -z "$CFG_SCHEMA" ] && ERRORS="${ERRORS}\n  - schema is required"

if [ -n "$ERRORS" ]; then
  echo -e "ERROR: Missing required values in setup.yaml:$ERRORS"
  exit 1
fi

PROFILE="${CFG_DATABRICKS_PROFILE:-DEFAULT}"
APP_NAME="${CFG_APP_NAME:-gnc-fda-compliance}"
PIPELINE_NAME="GNC FDA Compliance Pipeline"
VOLUME_PATH="/Volumes/${CFG_CATALOG}/${CFG_SCHEMA}/staging"

# --- Test CLI auth ---
echo "Verifying Databricks CLI authentication..."
WORKSPACE_USER=$(databricks current-user me --profile "$PROFILE" -o json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])" 2>/dev/null || echo "")
if [ -z "$WORKSPACE_USER" ]; then
  echo "ERROR: Could not authenticate. Check your CLI profile '$PROFILE'."
  echo "Run: databricks auth login --host <your-workspace-url> --profile $PROFILE"
  exit 1
fi

WORKSPACE_DIR="/Users/${WORKSPACE_USER}/gnc-fda-demo"
APP_FOLDER="/Workspace${WORKSPACE_DIR}/app"

echo ""
echo "=== Configuration ==="
echo "  Profile:    $PROFILE"
echo "  User:       $WORKSPACE_USER"
echo "  App name:   $APP_NAME"
echo "  Warehouse:  $CFG_WAREHOUSE_ID"
echo "  Catalog:    $CFG_CATALOG"
echo "  Schema:     $CFG_SCHEMA"
echo "  Volume:     $VOLUME_PATH"
echo ""

if [ "$SETUP_ONLY" = "true" ]; then
  echo "Setup validated. Run without --setup-only to deploy."
  exit 0
fi

# ============================================================================
# Helper: execute SQL via Statement Execution API
# ============================================================================
run_sql() {
  local stmt="$1"
  local desc="$2"
  local tmp
  tmp=$(mktemp)
  python3 -c "
import json, sys
json.dump({
    'warehouse_id': sys.argv[1],
    'statement': sys.argv[2],
    'wait_timeout': '50s'
}, open(sys.argv[3], 'w'))
" "$CFG_WAREHOUSE_ID" "$stmt" "$tmp"

  local result
  result=$(databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json @"$tmp" 2>&1)
  rm -f "$tmp"

  local state
  state=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('state','FAILED'))" 2>/dev/null || echo "FAILED")
  if [ "$state" = "SUCCEEDED" ]; then
    echo "  + $desc"
    return 0
  else
    local err
    err=$(echo "$result" | python3 -c "import sys,json; e=json.load(sys.stdin).get('status',{}).get('error',{}); print(e.get('message','unknown')[:200])" 2>/dev/null || echo "unknown")
    echo "  x $desc — $err"
    return 1
  fi
}

# ============================================================================
# Step 1: Create Unity Catalog resources
# ============================================================================
echo "--- Step 1: Setting up Unity Catalog resources ---"
run_sql "CREATE CATALOG IF NOT EXISTS \`${CFG_CATALOG}\`" "Catalog ${CFG_CATALOG}" || true

if ! run_sql "CREATE SCHEMA IF NOT EXISTS \`${CFG_CATALOG}\`.\`${CFG_SCHEMA}\`" "Schema ${CFG_CATALOG}.${CFG_SCHEMA}"; then
  echo ""
  echo "  ERROR: Could not create schema. Possible causes:"
  echo "    - The catalog '${CFG_CATALOG}' does not exist or you lack CREATE SCHEMA permission"
  echo "    - On workspaces with Default Storage, CREATE CATALOG via SQL may not work"
  echo "      → Create the catalog from the Databricks UI first, or use an existing catalog"
  echo "    - Update 'catalog' in setup.yaml to a catalog you have access to"
  exit 1
fi

run_sql "CREATE VOLUME IF NOT EXISTS \`${CFG_CATALOG}\`.\`${CFG_SCHEMA}\`.\`staging\`" "Volume staging"
echo ""

# ============================================================================
# Step 2: Fetch FDA data and upload to volume
# ============================================================================
if [ "$SKIP_DATA" = "false" ]; then
  echo "--- Step 2: Fetching FDA data from openFDA API ---"
  TMP_DATA="/tmp/gnc-fda-data"
  mkdir -p "$TMP_DATA"
  export FDA_API_KEY="${CFG_FDA_API_KEY}"

  python3 << 'PYEOF'
import requests, json, time, os, sys

API_KEY = os.environ.get("FDA_API_KEY", "")
BASE = "https://api.fda.gov"
OUT = "/tmp/gnc-fda-data"

def fetch(endpoint, params=None, limit=1000):
    results = []
    skip = 0
    batch = 100
    while skip < limit:
        p = {"limit": batch, "skip": skip}
        if API_KEY:
            p["api_key"] = API_KEY
        if params:
            p.update(params)
        try:
            r = requests.get(f"{BASE}{endpoint}", params=p, timeout=30)
            r.raise_for_status()
            data = r.json().get("results", [])
            results.extend(data)
            if len(data) < batch:
                break
            skip += batch
            time.sleep(0.3)
        except Exception as e:
            print(f"  Warning: {e}", file=sys.stderr)
            break
    return results

endpoints = [
    ("NDC Products",         "/drug/ndc.json",          None,          "ndc_products.json"),
    ("Enforcement Actions",  "/food/enforcement.json",  None,          "enforcement_actions.json"),
    ("Adverse Events",       "/drug/event.json",        None,          "adverse_events.json"),
]

for name, ep, params, fname in endpoints:
    print(f"  Fetching {name}...")
    data = fetch(ep, params, limit=1000)
    with open(f"{OUT}/{fname}", "w") as f:
        json.dump(data, f)
    print(f"  + {name}: {len(data)} records")
PYEOF

  echo "  Uploading to volume..."
  for f in ndc_products.json enforcement_actions.json adverse_events.json; do
    databricks fs cp "$TMP_DATA/$f" "dbfs:${VOLUME_PATH}/$f" --profile "$PROFILE" --overwrite > /dev/null 2>&1 \
      && echo "  + Uploaded $f" || echo "  x Failed to upload $f"
  done
  rm -rf "$TMP_DATA"
  echo ""
else
  echo "--- Step 2: Skipping data fetch (--skip-data) ---"
  echo ""
fi

# ============================================================================
# Step 3: Upload pipeline notebook
# ============================================================================
echo "--- Step 3: Uploading pipeline notebook ---"
databricks workspace mkdirs "$WORKSPACE_DIR" --profile "$PROFILE" 2>/dev/null || true
databricks workspace import "$WORKSPACE_DIR/gnc_fda_pipeline" \
  --file "$SCRIPT_DIR/pipeline/gnc_fda_pipeline.py" \
  --format SOURCE --language PYTHON --overwrite \
  --profile "$PROFILE" \
  && echo "  + Pipeline notebook uploaded" || { echo "  x Failed to upload notebook"; exit 1; }
echo ""

# ============================================================================
# Step 4: Create and run DLT pipeline
# ============================================================================
if [ "$SKIP_PIPELINE" = "false" ]; then
  echo "--- Step 4: Running DLT pipeline ---"

  # Check if pipeline already exists by name
  PIPELINE_ID=$(databricks pipelines list-pipelines --profile "$PROFILE" --output json 2>/dev/null \
    | python3 -c "
import sys, json
data = json.load(sys.stdin)
for p in data:
    if p.get('name') == '$PIPELINE_NAME':
        print(p['pipeline_id'])
        break
" 2>/dev/null || echo "")

  PIPELINE_JSON=$(cat << PJSON
{
  "name": "$PIPELINE_NAME",
  "catalog": "$CFG_CATALOG",
  "target": "$CFG_SCHEMA",
  "development": true,
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "configuration": {
    "volume_path": "$VOLUME_PATH"
  },
  "libraries": [
    {"notebook": {"path": "$WORKSPACE_DIR/gnc_fda_pipeline"}}
  ],
  "clusters": [
    {"label": "default", "num_workers": 0, "spark_conf": {"spark.master": "local[*]"}}
  ]
}
PJSON
)

  PIPE_TMP=$(mktemp)

  if [ -n "$PIPELINE_ID" ]; then
    echo "  Pipeline exists ($PIPELINE_ID) — updating..."
    echo "$PIPELINE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); d['id']='$PIPELINE_ID'; json.dump(d,open(sys.argv[1],'w'))" "$PIPE_TMP"
    databricks pipelines update --profile "$PROFILE" --json @"$PIPE_TMP" > /dev/null 2>&1 || true
  else
    echo "  Creating pipeline..."
    echo "$PIPELINE_JSON" > "$PIPE_TMP"
    PIPELINE_ID=$(databricks pipelines create --profile "$PROFILE" --json @"$PIPE_TMP" 2>&1 \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['pipeline_id'])")
    echo "  + Pipeline created: $PIPELINE_ID"
  fi
  rm -f "$PIPE_TMP"

  echo "  Starting pipeline update..."
  databricks pipelines start-update "$PIPELINE_ID" --profile "$PROFILE" > /dev/null 2>&1

  echo "  Waiting for pipeline to complete (this takes 2-5 minutes)..."
  for i in $(seq 1 60); do
    PIPE_STATE=$(databricks pipelines get "$PIPELINE_ID" --profile "$PROFILE" --output json 2>/dev/null \
      | python3 -c "
import sys, json
d = json.load(sys.stdin)
state = d.get('state','UNKNOWN')
updates = d.get('latest_updates', [])
if updates:
    u_state = updates[0].get('state','')
    if u_state in ('COMPLETED', 'FAILED', 'CANCELED'):
        print(u_state)
    else:
        print('RUNNING')
else:
    print(state)
" 2>/dev/null || echo "UNKNOWN")

    case $PIPE_STATE in
      COMPLETED)
        echo "  + Pipeline completed successfully"
        break
        ;;
      FAILED)
        echo "  x Pipeline failed. Check the pipeline UI for details:"
        echo "    Pipeline ID: $PIPELINE_ID"
        exit 1
        ;;
      CANCELED)
        echo "  x Pipeline was canceled"
        exit 1
        ;;
      *)
        if [ $((i % 6)) -eq 0 ]; then
          echo "  ... still running ($((i*5))s)"
        fi
        sleep 5
        ;;
    esac

    if [ $i -eq 60 ]; then
      echo "  x Timed out after 5 minutes. Pipeline may still be running."
      echo "    Pipeline ID: $PIPELINE_ID"
      exit 1
    fi
  done
  echo ""
else
  echo "--- Step 4: Skipping pipeline (--skip-pipeline) ---"
  echo ""
fi

# ============================================================================
# Step 5: Generate app.yaml and upload app files
# ============================================================================
echo "--- Step 5: Uploading app files ---"

# Generate app.yaml with correct environment variables
cat > "$SCRIPT_DIR/app/app.yaml" << APPYAML
command:
  - "python"
  - "app.py"

env:
  - name: PORT
    value: "8000"
  - name: DATABRICKS_WAREHOUSE_ID
    value: "${CFG_WAREHOUSE_ID}"
  - name: CATALOG
    value: "${CFG_CATALOG}"
  - name: SCHEMA
    value: "${CFG_SCHEMA}"
APPYAML
echo "  + Generated app.yaml"

databricks workspace mkdirs "$WORKSPACE_DIR/app" --profile "$PROFILE" 2>/dev/null || true
for f in app.py requirements.txt app.yaml; do
  databricks workspace import "$WORKSPACE_DIR/app/$f" \
    --file "$SCRIPT_DIR/app/$f" \
    --format AUTO --overwrite \
    --profile "$PROFILE" > /dev/null 2>&1 \
    && echo "  + Uploaded $f" || echo "  x Failed to upload $f"
done
echo ""

# ============================================================================
# Step 6: Create and deploy Databricks App
# ============================================================================
echo "--- Step 6: Deploying Databricks App ---"

# Check if app exists
APP_EXISTS=$(databricks apps get "$APP_NAME" --profile "$PROFILE" 2>/dev/null && echo "yes" || echo "no")

if [ "$APP_EXISTS" = "no" ]; then
  echo "  Creating app '$APP_NAME'..."
  databricks apps create --json "{\"name\":\"$APP_NAME\", \"description\":\"GNC FDA Compliance Monitor\"}" --profile "$PROFILE" > /dev/null 2>&1 \
    || { echo "  x Failed to create app. The workspace may have hit the 300 app limit."; echo "    Delete a stopped app and re-run, or use: databricks apps list --profile $PROFILE"; exit 1; }
  echo "  + App created"

  # Wait for compute to be ready
  echo "  Waiting for app compute to start..."
  for i in $(seq 1 24); do
    COMPUTE_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state',''))" 2>/dev/null || echo "")
    if [ "$COMPUTE_STATE" = "ACTIVE" ]; then
      echo "  + Compute is active"
      break
    fi
    sleep 5
  done
else
  echo "  App '$APP_NAME' already exists"
fi

echo "  Deploying..."
databricks apps deploy "$APP_NAME" \
  --source-code-path "$APP_FOLDER" \
  --profile "$PROFILE" > /dev/null 2>&1 \
  && echo "  + Deployment initiated" || { echo "  x Deployment failed"; exit 1; }

# Wait for deployment
echo "  Waiting for app to be ready..."
for i in $(seq 1 36); do
  APP_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('app_status',{}).get('state',''))" 2>/dev/null || echo "")
  if [ "$APP_STATE" = "RUNNING" ]; then
    echo "  + App is running"
    break
  fi
  if [ $i -eq 36 ]; then
    echo "  Warning: App not ready after 3 minutes — it may still be starting"
  fi
  sleep 5
done
echo ""

# ============================================================================
# Step 7: Grant permissions to app service principal
# ============================================================================
echo "--- Step 7: Granting permissions ---"

SP_ID=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id',''))" 2>/dev/null || echo "")

if [ -n "$SP_ID" ]; then
  echo "  App SP: $SP_ID"

  # Warehouse CAN_USE
  databricks api put "/api/2.0/permissions/sql/warehouses/${CFG_WAREHOUSE_ID}" \
    --profile "$PROFILE" \
    --json "{\"access_control_list\":[{\"service_principal_name\":\"$SP_ID\",\"permission_level\":\"CAN_USE\"}]}" \
    > /dev/null 2>&1 \
    && echo "  + Warehouse CAN_USE" || echo "  x Could not grant warehouse access (grant manually)"

  # UC grants via SQL
  run_sql "GRANT USE CATALOG ON CATALOG \`${CFG_CATALOG}\` TO \`${SP_ID}\`" "USE CATALOG" || true
  run_sql "GRANT USE SCHEMA ON SCHEMA \`${CFG_CATALOG}\`.\`${CFG_SCHEMA}\` TO \`${SP_ID}\`" "USE SCHEMA" || true
  run_sql "GRANT SELECT ON SCHEMA \`${CFG_CATALOG}\`.\`${CFG_SCHEMA}\` TO \`${SP_ID}\`" "SELECT on schema" || true
else
  echo "  Warning: Could not detect SP ID. Grant permissions manually."
fi
echo ""

# ============================================================================
# Done
# ============================================================================
APP_URL=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || echo "unknown")

echo "============================================"
echo "  Deployment complete!"
echo "  App URL: $APP_URL"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Open the app URL in your browser"
echo "  2. Explore the Enforcement Actions, Safety Signals, and Product Catalog tabs"
echo ""
echo "Lakebase sync (optional):"
echo "  The gold tables have Change Data Feed enabled. To sync to Lakebase:"
echo "  1. Go to Catalog > Lakebase > create a project"
echo "  2. Create synced tables from:"
echo "     - ${CFG_CATALOG}.${CFG_SCHEMA}.gold_product_catalog"
echo "     - ${CFG_CATALOG}.${CFG_SCHEMA}.gold_enforcement_actions"
echo "     - ${CFG_CATALOG}.${CFG_SCHEMA}.gold_adverse_events_summary"
echo ""
echo "To redeploy after changes:"
echo "  ./deploy.sh --skip-data --skip-pipeline   # App-only redeploy"
