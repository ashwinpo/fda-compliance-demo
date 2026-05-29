import os
import logging
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fda-compliance")

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.environ.get("CATALOG", "main")
SCHEMA = os.environ.get("SCHEMA", "fda_demo")

app = FastAPI(title="FDA Compliance Monitor")
w = WorkspaceClient()


def run_sql(query: str) -> list[dict]:
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s"
    )
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise Exception(f"SQL failed: {err.error_code if err else ''} {err.message if err else ''}")
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = []
    if resp.result and resp.result.data_array:
        for row in resp.result.data_array:
            rows.append(dict(zip(cols, row)))
    return rows


def tbl(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


@app.get("/api/stats")
def api_stats():
    products = run_sql(f"SELECT COUNT(*) AS n FROM {tbl('gold_product_catalog')}")
    enforcement = run_sql(f"SELECT COUNT(*) AS total, SUM(CASE WHEN classification='Class I' THEN 1 ELSE 0 END) AS class_i, SUM(CASE WHEN status='Ongoing' THEN 1 ELSE 0 END) AS active FROM {tbl('gold_enforcement_actions')}")
    adverse = run_sql(f"SELECT SUM(event_count) AS total_events, SUM(serious_count) AS serious_events, COUNT(DISTINCT product_name) AS unique_products FROM {tbl('gold_adverse_events_summary')}")
    return {
        "products": int(products[0]["n"]) if products else 0,
        "total_recalls": int(enforcement[0]["total"]) if enforcement else 0,
        "class_i_recalls": int(enforcement[0]["class_i"] or 0) if enforcement else 0,
        "active_recalls": int(enforcement[0]["active"] or 0) if enforcement else 0,
        "total_adverse_events": int(adverse[0]["total_events"] or 0) if adverse else 0,
        "serious_events": int(adverse[0]["serious_events"] or 0) if adverse else 0,
        "unique_products_with_ae": int(adverse[0]["unique_products"] or 0) if adverse else 0,
    }


@app.get("/api/enforcement")
def api_enforcement(search: str = Query("", max_length=200), limit: int = Query(50, le=200)):
    where = ""
    if search:
        safe = search.replace("'", "''")
        where = f"WHERE LOWER(product_description) LIKE '%{safe.lower()}%' OR LOWER(recalling_firm) LIKE '%{safe.lower()}%' OR LOWER(reason_for_recall) LIKE '%{safe.lower()}%'"
    rows = run_sql(f"""
        SELECT recall_number, classification, status, recall_date, recalling_firm,
               city, state, product_description, reason_for_recall
        FROM {tbl('gold_enforcement_actions')}
        {where}
        ORDER BY recall_date DESC NULLS LAST
        LIMIT {limit}
    """)
    return {"results": rows, "count": len(rows)}


@app.get("/api/adverse-events")
def api_adverse_events(limit: int = Query(30, le=100)):
    rows = run_sql(f"""
        SELECT product_name, reaction_name, event_count, serious_count,
               earliest_report, latest_report
        FROM {tbl('gold_adverse_events_summary')}
        ORDER BY event_count DESC
        LIMIT {limit}
    """)
    return {"results": rows, "count": len(rows)}


@app.get("/api/adverse-events/by-product")
def api_ae_by_product():
    rows = run_sql(f"""
        SELECT product_name, SUM(event_count) AS total_events,
               SUM(serious_count) AS serious_events,
               COUNT(DISTINCT reaction_name) AS unique_reactions
        FROM {tbl('gold_adverse_events_summary')}
        GROUP BY product_name
        ORDER BY total_events DESC
        LIMIT 20
    """)
    return {"results": rows}


@app.get("/api/products")
def api_products(search: str = Query("", max_length=200), limit: int = Query(50, le=200)):
    where = ""
    if search:
        safe = search.replace("'", "''")
        where = f"WHERE LOWER(brand_name) LIKE '%{safe.lower()}%' OR LOWER(generic_name) LIKE '%{safe.lower()}%' OR LOWER(labeler_name) LIKE '%{safe.lower()}%'"
    rows = run_sql(f"""
        SELECT product_ndc, brand_name, generic_name, labeler_name,
               product_type, dosage_form, primary_route, primary_ingredient, ingredient_count
        FROM {tbl('gold_product_catalog')}
        {where}
        ORDER BY brand_name
        LIMIT {limit}
    """)
    return {"results": rows, "count": len(rows)}


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FDA Compliance Monitor</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg-base:#0a0e17;--bg-card:#111827;--bg-card-hover:#1a2234;
  --border:#1e293b;--border-light:#2d3a4f;
  --text:#e2e8f0;--text-muted:#94a3b8;--text-dim:#64748b;
  --accent:#0ea5e9;--accent-dim:#0c4a6e;
  --red:#ef4444;--red-dim:rgba(239,68,68,.12);
  --amber:#f59e0b;--amber-dim:rgba(245,158,11,.12);
  --blue:#3b82f6;--blue-dim:rgba(59,130,246,.12);
  --green:#10b981;--green-dim:rgba(16,185,129,.12);
  --radius:8px;--radius-lg:12px;
}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg-base);color:var(--text);line-height:1.5;min-height:100vh}
a{color:var(--accent);text-decoration:none}

/* Header */
.header{padding:20px 32px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:16px;background:linear-gradient(180deg,#0f1520 0%,var(--bg-base) 100%)}
.header-icon{width:36px;height:36px;background:var(--accent);border-radius:8px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;color:#fff;letter-spacing:-.5px}
.header h1{font-size:20px;font-weight:600;letter-spacing:-.3px}
.header .subtitle{font-size:13px;color:var(--text-muted);margin-left:auto}

/* Stats */
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;padding:20px 32px}
.stat-card{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius-lg);padding:16px 20px;transition:border-color .15s}
.stat-card:hover{border-color:var(--border-light)}
.stat-label{font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:var(--text-muted);margin-bottom:4px}
.stat-value{font-size:28px;font-weight:700;letter-spacing:-.5px;font-variant-numeric:tabular-nums}
.stat-detail{font-size:12px;color:var(--text-dim);margin-top:2px}
.stat-value.red{color:var(--red)}.stat-value.amber{color:var(--amber)}.stat-value.blue{color:var(--accent)}.stat-value.green{color:var(--green)}

/* Tabs */
.tabs{display:flex;gap:0;padding:0 32px;border-bottom:1px solid var(--border)}
.tab{padding:12px 24px;font-size:13px;font-weight:500;color:var(--text-muted);cursor:pointer;border-bottom:2px solid transparent;transition:all .15s;user-select:none}
.tab:hover{color:var(--text)}
.tab.active{color:var(--accent);border-bottom-color:var(--accent)}

/* Content */
.content{padding:20px 32px}
.tab-panel{display:none}.tab-panel.active{display:block}
.search-bar{display:flex;gap:12px;margin-bottom:16px;align-items:center}
.search-input{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);padding:8px 14px;color:var(--text);font-size:13px;width:320px;outline:none;transition:border-color .15s}
.search-input:focus{border-color:var(--accent)}
.search-input::placeholder{color:var(--text-dim)}
.result-count{font-size:12px;color:var(--text-dim)}

/* Table */
.data-table{width:100%;border-collapse:collapse;font-size:13px}
.data-table thead{position:sticky;top:0;z-index:1}
.data-table th{background:var(--bg-card);border-bottom:1px solid var(--border);padding:10px 12px;text-align:left;font-weight:500;color:var(--text-muted);font-size:11px;text-transform:uppercase;letter-spacing:.6px;white-space:nowrap}
.data-table td{border-bottom:1px solid var(--border);padding:10px 12px;vertical-align:top;max-width:300px}
.data-table tr:hover td{background:var(--bg-card-hover)}
.data-table .truncate{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:280px;display:block}

/* Badges */
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;letter-spacing:.3px}
.badge-class-i{background:var(--red-dim);color:var(--red)}
.badge-class-ii{background:var(--amber-dim);color:var(--amber)}
.badge-class-iii{background:var(--blue-dim);color:var(--blue)}
.badge-ongoing{background:var(--green-dim);color:var(--green)}
.badge-terminated{background:rgba(100,116,139,.15);color:var(--text-dim)}

/* Bar chart */
.bar-row{display:flex;align-items:center;gap:12px;padding:6px 0;border-bottom:1px solid var(--border)}
.bar-row:last-child{border-bottom:none}
.bar-label{flex:0 0 220px;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bar-track{flex:1;height:24px;background:var(--bg-card);border-radius:4px;overflow:hidden;position:relative}
.bar-fill{height:100%;border-radius:4px;display:flex;align-items:center;padding-left:8px;font-size:11px;font-weight:600;color:#fff;min-width:fit-content;transition:width .3s ease}
.bar-fill.serious{background:linear-gradient(90deg,var(--red),#dc2626)}
.bar-fill.total{background:linear-gradient(90deg,var(--accent),#0284c7)}
.bar-count{flex:0 0 60px;text-align:right;font-size:13px;font-variant-numeric:tabular-nums;color:var(--text-muted)}

/* Two column layout for adverse events */
.ae-grid{display:grid;grid-template-columns:1fr 1fr;gap:24px}
.ae-section h3{font-size:14px;font-weight:600;margin-bottom:12px;color:var(--text-muted)}
@media(max-width:1100px){.ae-grid{grid-template-columns:1fr}}

/* Loading */
.loading{text-align:center;padding:40px;color:var(--text-dim)}
.loading::after{content:'';display:inline-block;width:16px;height:16px;border:2px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .6s linear infinite;margin-left:8px;vertical-align:middle}
@keyframes spin{to{transform:rotate(360deg)}}

/* Empty state */
.empty{text-align:center;padding:40px;color:var(--text-dim);font-size:14px}
</style>
</head>
<body>

<div class="header">
  <div class="header-icon">FDA</div>
  <div>
    <h1>FDA Compliance Monitor</h1>
  </div>
  <div class="subtitle">Powered by Databricks Lakehouse &rarr; Lakebase</div>
</div>

<div class="stats" id="stats-bar">
  <div class="stat-card"><div class="stat-label">Products Tracked</div><div class="stat-value blue" id="s-products">--</div></div>
  <div class="stat-card"><div class="stat-label">Total Recalls</div><div class="stat-value amber" id="s-recalls">--</div><div class="stat-detail" id="s-recalls-detail"></div></div>
  <div class="stat-card"><div class="stat-label">Class I Recalls</div><div class="stat-value red" id="s-class-i">--</div><div class="stat-detail">Highest severity</div></div>
  <div class="stat-card"><div class="stat-label">Adverse Events</div><div class="stat-value" id="s-ae">--</div><div class="stat-detail" id="s-ae-detail"></div></div>
  <div class="stat-card"><div class="stat-label">Serious Events</div><div class="stat-value red" id="s-serious">--</div></div>
</div>

<div class="tabs">
  <div class="tab active" data-tab="recalls">Enforcement Actions</div>
  <div class="tab" data-tab="safety">Safety Signals</div>
  <div class="tab" data-tab="catalog">Product Catalog</div>
</div>

<div class="content">
  <!-- Recalls Tab -->
  <div class="tab-panel active" id="panel-recalls">
    <div class="search-bar">
      <input class="search-input" id="recall-search" placeholder="Search recalls by product, firm, or reason..." />
      <span class="result-count" id="recall-count"></span>
    </div>
    <div style="overflow-x:auto;max-height:calc(100vh - 340px);overflow-y:auto">
      <table class="data-table" id="recall-table">
        <thead>
          <tr><th>Class</th><th>Status</th><th>Date</th><th>Firm</th><th>State</th><th>Product</th><th>Reason</th></tr>
        </thead>
        <tbody id="recall-body"></tbody>
      </table>
    </div>
  </div>

  <!-- Safety Signals Tab -->
  <div class="tab-panel" id="panel-safety">
    <div class="ae-grid">
      <div class="ae-section">
        <h3>Top Products by Adverse Events</h3>
        <div id="ae-products"></div>
      </div>
      <div class="ae-section">
        <h3>Top Product-Reaction Combinations</h3>
        <div id="ae-reactions"></div>
      </div>
    </div>
  </div>

  <!-- Catalog Tab -->
  <div class="tab-panel" id="panel-catalog">
    <div class="search-bar">
      <input class="search-input" id="product-search" placeholder="Search by brand name, generic name, or manufacturer..." />
      <span class="result-count" id="product-count"></span>
    </div>
    <div style="overflow-x:auto;max-height:calc(100vh - 340px);overflow-y:auto">
      <table class="data-table" id="product-table">
        <thead>
          <tr><th>NDC</th><th>Brand Name</th><th>Generic Name</th><th>Manufacturer</th><th>Type</th><th>Form</th><th>Route</th><th>Ingredients</th></tr>
        </thead>
        <tbody id="product-body"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
const API = '';

// --- Tabs ---
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('panel-' + tab.dataset.tab).classList.add('active');
  });
});

// --- Fetch helpers ---
async function fetchJSON(url) {
  const resp = await fetch(API + url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

function esc(s) { if (!s) return ''; const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function classBadge(c) {
  const cls = {'Class I':'badge-class-i','Class II':'badge-class-ii','Class III':'badge-class-iii'}[c] || '';
  return `<span class="badge ${cls}">${esc(c)}</span>`;
}

function statusBadge(s) {
  if (!s) return '';
  const cls = s === 'Ongoing' ? 'badge-ongoing' : 'badge-terminated';
  return `<span class="badge ${cls}">${esc(s)}</span>`;
}

function fmtNum(n) { return Number(n || 0).toLocaleString(); }

// --- Stats ---
async function loadStats() {
  try {
    const d = await fetchJSON('/api/stats');
    document.getElementById('s-products').textContent = fmtNum(d.products);
    document.getElementById('s-recalls').textContent = fmtNum(d.total_recalls);
    document.getElementById('s-recalls-detail').textContent = `${fmtNum(d.active_recalls)} active`;
    document.getElementById('s-class-i').textContent = fmtNum(d.class_i_recalls);
    document.getElementById('s-ae').textContent = fmtNum(d.total_adverse_events);
    document.getElementById('s-ae-detail').textContent = `${fmtNum(d.unique_products_with_ae)} products`;
    document.getElementById('s-serious').textContent = fmtNum(d.serious_events);
  } catch(e) { console.error('Stats error:', e); }
}

// --- Enforcement ---
let recallDebounce;
async function loadRecalls(search) {
  const body = document.getElementById('recall-body');
  body.innerHTML = '<tr><td colspan="7" class="loading">Loading recalls</td></tr>';
  try {
    const q = search ? `?search=${encodeURIComponent(search)}` : '';
    const d = await fetchJSON('/api/enforcement' + q);
    document.getElementById('recall-count').textContent = `${d.count} results`;
    if (!d.results.length) { body.innerHTML = '<tr><td colspan="7" class="empty">No results</td></tr>'; return; }
    body.innerHTML = d.results.map(r => `
      <tr>
        <td>${classBadge(r.classification)}</td>
        <td>${statusBadge(r.status)}</td>
        <td style="white-space:nowrap">${esc(r.recall_date || '')}</td>
        <td>${esc(r.recalling_firm)}</td>
        <td>${esc(r.state || '')}</td>
        <td><span class="truncate" title="${esc(r.product_description)}">${esc(r.product_description)}</span></td>
        <td><span class="truncate" title="${esc(r.reason_for_recall)}">${esc(r.reason_for_recall)}</span></td>
      </tr>
    `).join('');
  } catch(e) { body.innerHTML = `<tr><td colspan="7" class="empty">Error: ${esc(e.message)}</td></tr>`; }
}

document.getElementById('recall-search').addEventListener('input', e => {
  clearTimeout(recallDebounce);
  recallDebounce = setTimeout(() => loadRecalls(e.target.value), 300);
});

// --- Adverse Events ---
async function loadAdverseEvents() {
  const prodEl = document.getElementById('ae-products');
  const rxnEl = document.getElementById('ae-reactions');
  prodEl.innerHTML = '<div class="loading">Loading</div>';
  rxnEl.innerHTML = '<div class="loading">Loading</div>';
  try {
    const [byProd, byRxn] = await Promise.all([
      fetchJSON('/api/adverse-events/by-product'),
      fetchJSON('/api/adverse-events?limit=20')
    ]);

    const maxProd = Math.max(...byProd.results.map(r => Number(r.total_events)));
    prodEl.innerHTML = byProd.results.map(r => {
      const pct = (Number(r.total_events) / maxProd * 100).toFixed(0);
      const sPct = (Number(r.serious_events) / maxProd * 100).toFixed(0);
      return `
        <div class="bar-row">
          <div class="bar-label" title="${esc(r.product_name)}">${esc(r.product_name)}</div>
          <div class="bar-track">
            <div class="bar-fill total" style="width:${pct}%"></div>
          </div>
          <div class="bar-count">${fmtNum(r.total_events)}</div>
        </div>`;
    }).join('');

    const maxRxn = Math.max(...byRxn.results.map(r => Number(r.event_count)));
    rxnEl.innerHTML = byRxn.results.map(r => {
      const pct = (Number(r.event_count) / maxRxn * 100).toFixed(0);
      return `
        <div class="bar-row">
          <div class="bar-label" title="${esc(r.product_name)} + ${esc(r.reaction_name)}">${esc(r.product_name)}<br><span style="color:var(--text-dim);font-size:11px">${esc(r.reaction_name)}</span></div>
          <div class="bar-track">
            <div class="bar-fill ${Number(r.serious_count) > 0 ? 'serious' : 'total'}" style="width:${pct}%"></div>
          </div>
          <div class="bar-count">${fmtNum(r.event_count)}</div>
        </div>`;
    }).join('');
  } catch(e) {
    prodEl.innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
    rxnEl.innerHTML = '';
  }
}

// --- Products ---
let productDebounce;
async function loadProducts(search) {
  const body = document.getElementById('product-body');
  body.innerHTML = '<tr><td colspan="8" class="loading">Loading products</td></tr>';
  try {
    const q = search ? `?search=${encodeURIComponent(search)}` : '';
    const d = await fetchJSON('/api/products' + q);
    document.getElementById('product-count').textContent = `${d.count} results`;
    if (!d.results.length) { body.innerHTML = '<tr><td colspan="8" class="empty">No results</td></tr>'; return; }
    body.innerHTML = d.results.map(r => `
      <tr>
        <td style="white-space:nowrap;font-family:monospace;font-size:12px">${esc(r.product_ndc)}</td>
        <td>${esc(r.brand_name)}</td>
        <td><span class="truncate">${esc(r.generic_name)}</span></td>
        <td><span class="truncate">${esc(r.labeler_name)}</span></td>
        <td style="font-size:12px">${esc(r.product_type)}</td>
        <td style="font-size:12px">${esc(r.dosage_form)}</td>
        <td style="font-size:12px">${esc(r.primary_route || '')}</td>
        <td style="text-align:center">${esc(r.ingredient_count || '')}</td>
      </tr>
    `).join('');
  } catch(e) { body.innerHTML = `<tr><td colspan="8" class="empty">Error: ${esc(e.message)}</td></tr>`; }
}

document.getElementById('product-search').addEventListener('input', e => {
  clearTimeout(productDebounce);
  productDebounce = setTimeout(() => loadProducts(e.target.value), 300);
});

// --- Init ---
loadStats();
loadRecalls('');
loadAdverseEvents();
loadProducts('');
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def root():
    return DASHBOARD_HTML


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
