# app/main.py
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from app.migrator import run_migration, get_job
from app.memgraph_client import get_schema
from app.csv_exporter import export_neo4j
from app.config import settings

app = FastAPI(title="Antifraud Graph Migration API")


class MigrateBody(BaseModel):
    strategy: str = "deterministic"       # llm | deterministic
    mode: str = "automatic"     # automatic | incremental


@app.post("/migrate")
def migrate(body: MigrateBody = MigrateBody()):
    if body.strategy not in ("llm", "deterministic"):
        raise HTTPException(400, "strategy must be 'llm' or 'deterministic'")
    if body.mode not in ("automatic", "incremental"):
        raise HTTPException(400, "mode must be 'automatic' or 'incremental'")
    if body.strategy == "llm" and not settings.effective_llm_key:
        raise HTTPException(
            400,
            "LLM_API_KEY/OPENAI_API_KEY is empty. Provide a key, "
            "or explicitly use strategy='deterministic'.",
        )
    try:
        job = run_migration(body.strategy, body.mode)
    except FileNotFoundError:
        raise HTTPException(500, "structured2graph CLI not found on PATH")
    if job["returncode"] != 0:
        raise HTTPException(500, {"job_id": job["job_id"], "log": job["log_tail"]})
    return job


@app.get("/migrate/{job_id}/status")
def migrate_status(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job


@app.get("/schema")
def schema():
    return get_schema()


@app.get("/export/neo4j")
def export(
    labels: str | None = Query(None, description="逗号分隔，如 Account,Bank"),
    rel_types: str | None = Query(None, description="逗号分隔，如 SENT_TO,FLAGGED_WITH"),
):
    l = [x.strip() for x in labels.split(",")] if labels else None
    r = [x.strip() for x in rel_types.split(",")] if rel_types else None
    return export_neo4j(l, r)
