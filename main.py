import os
import time
import requests
from urllib.parse import urljoin
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import pathlib

DAG_ID = "legal_property_pipeline"

# ── Helpers for Airflow URL detection ──────────────────────────────────────────
def normalize_base(u: str) -> str:
    return u.rstrip("/")

def probe_base(base: str) -> bool:
    """Return True if this base looks like a valid Airflow API root."""
    try:
        r = requests.get(urljoin(base + "/", "dags?limit=1"), timeout=5, auth=(AIRFLOW_USER, AIRFLOW_PW))
        return r.status_code < 500
    except Exception:
        return False

# ── Read Airflow credentials ───────────────────────────────────────────────────
AIRFLOW_USER = os.getenv("AIRFLOW_USER")
AIRFLOW_PW = os.getenv("AIRFLOW_PW")

if not AIRFLOW_USER or not AIRFLOW_PW:
    raise RuntimeError("Missing AIRFLOW_USER or AIRFLOW_PW.")

_explicit = os.getenv("AIRFLOW_API_BASE_URL")
if _explicit:
    AIRFLOW_API_BASE_URL = normalize_base(_explicit)
else:
    host = os.getenv("AIRFLOW_API_HOST", "airflow-webserver")
    port = os.getenv("AIRFLOW_API_PORT", "8080")
    candidates = [
        f"http://{host}:{port}/api/v2",
        f"http://{host}:{port}/api/v1",
    ]
    AIRFLOW_API_BASE_URL = None
    for c in candidates:
        c = normalize_base(c)
        if probe_base(c):
            AIRFLOW_API_BASE_URL = c
            break

if not AIRFLOW_API_BASE_URL:
    raise RuntimeError("Could not reach Airflow API. Set AIRFLOW_API_BASE_URL or AIRFLOW_API_HOST/AIRFLOW_API_PORT.")

try:
    AIRFLOW_DAG_MAX_WAIT_SEC = int(os.getenv("AIRFLOW_DAG_MAX_WAIT_SEC", "600"))
except ValueError:
    raise RuntimeError("AIRFLOW_DAG_MAX_WAIT_SEC must be an integer.")

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(title="NYC ACRIS QA API")

# CORS settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update with your frontend URL if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve built frontend
dist_path = os.path.join(os.path.dirname(__file__), "web", "dist")
if not os.path.exists(dist_path):
    raise RuntimeError(f"dist folder not found at: {dist_path}")
app.mount("/assets", StaticFiles(directory=os.path.join(dist_path, "assets")), name="assets")

class AskRequest(BaseModel):
    prompt: str
    top_k: Optional[int] = None

# ── Airflow helpers ────────────────────────────────────────────────────────────
def airflow_request(method: str, path: str, **kwargs):
    url = urljoin(AIRFLOW_API_BASE_URL + "/", path.lstrip("/"))
    resp = requests.request(
        method,
        url,
        auth=(AIRFLOW_USER, AIRFLOW_PW),
        timeout=15,
        **kwargs,
    )
    resp.raise_for_status()
    return resp.json() if resp.content else {}

def ensure_dag_available(dag_id: str):
    try:
        airflow_request("GET", f"/dags/{dag_id}")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            raise HTTPException(404, f"DAG '{dag_id}' not found in Airflow.")
        raise

# ── API endpoint ──────────────────────────────────────────────────────────────
@app.post("/ask")
def ask(request: AskRequest):
    prompt = request.prompt
    top_k = request.top_k

    ensure_dag_available(DAG_ID)

    conf = {"prompt": prompt}
    if top_k is not None:
        conf["top_k"] = top_k

    try:
        payload = airflow_request(
            "POST",
            f"/dags/{DAG_ID}/dagRuns",
            json={"conf": conf},
        )
    except requests.HTTPError as e:
        body = e.response.text if e.response is not None else str(e)
        raise HTTPException(500, f"DAG trigger failed. {body}")

    run_id = payload.get("dag_run_id")
    if not run_id:
        raise HTTPException(500, f"No dag_run_id returned: {payload}")

    elapsed = 0
    interval = 5
    while elapsed < AIRFLOW_DAG_MAX_WAIT_SEC:
        try:
            status = airflow_request("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}").get("state")
        except requests.HTTPError as e:
            raise HTTPException(500, f"Error checking DAG status. HTTP {e.response.status_code}")
        if status == "success":
            break
        if status in ("failed", "error"):
            raise HTTPException(500, f"DAG run ended with state: {status}")
        time.sleep(interval)
        elapsed += interval
    else:
        raise HTTPException(500, f"DAG run {run_id} did not complete in time.")

    def get_xcom(key: str):
        try:
            res = airflow_request(
                "GET",
                f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances/ask_gemini_with_vector_context/xcomEntries/{key}",
            )
            return res.get("value")
        except Exception:
            return None

    return {
        "answer": get_xcom("answer"),
        "context": get_xcom("context") or "",
        "run_id": run_id,
        "route": get_xcom("route"),
        "confidence": get_xcom("confidence"),
    }

# ── Serve index.html ──────────────────────────────────────────────────────────
@app.get("/{full_path:path}")
async def serve_frontend(full_path: str):
    index_file = os.path.join(dist_path, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    raise HTTPException(404, "Frontend not found")