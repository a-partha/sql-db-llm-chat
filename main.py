import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import requests

# Load environment variables
AIRFLOW_API_BASE_URL = os.getenv("AIRFLOW_API_BASE_URL")
AIRFLOW_USER = os.getenv("AIRFLOW_USER")
AIRFLOW_PW = os.getenv("AIRFLOW_PW")

if not AIRFLOW_API_BASE_URL or not AIRFLOW_USER or not AIRFLOW_PW:
    raise RuntimeError("Missing required environment variables for Airflow API.")

app = FastAPI()

# CORS settings — adjust origins if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change to your frontend URL if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve built frontend
dist_path = os.path.join(os.path.dirname(__file__), "web", "dist")
if not os.path.exists(dist_path):
    raise RuntimeError(f"dist folder not found at: {dist_path}")

app.mount("/assets", StaticFiles(directory=os.path.join(dist_path, "assets")), name="assets")


@app.post("/ask")
async def ask_question(payload: dict):
    try:
        # Prepare request body for Airflow
        dag_id = "legal_property_pipeline"
        top_k = payload.get("top_k")
        request_body = {
            "conf": {
                "prompt": payload.get("prompt"),
                "top_k": top_k if top_k is not None else 50
            }
        }

        url = f"{AIRFLOW_API_BASE_URL}/dags/{dag_id}/dagRuns"
        response = requests.post(url, auth=(AIRFLOW_USER, AIRFLOW_PW), json=request_body)

        if not response.ok:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        return {"message": "DAG triggered successfully", "data": response.json()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Serve index.html for any non-API route
@app.get("/{full_path:path}")
async def serve_frontend(full_path: str):
    index_file = os.path.join(dist_path, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    raise HTTPException(status_code=404, detail="Frontend not found")
