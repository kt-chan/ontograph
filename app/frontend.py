# antifraud_proto/app.py
from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from neo4j import GraphDatabase, basic_auth
from neo4j.graph import Node, Relationship
from pydantic import BaseModel, Field
from starlette.requests import Request

MEMGRAPH_URI = os.getenv("MEMGRAPH_URL", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USERNAME", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")

app = FastAPI(title="Anti-Fraud Memgraph Explorer Prototype", version="0.1.0")
templates = Jinja2Templates(directory="app/templates")


def _get_driver():
    auth = basic_auth(MEMGRAPH_USER, MEMGRAPH_PASSWORD) if MEMGRAPH_USER else None
    return GraphDatabase.driver(MEMGRAPH_URI, auth=auth)


def _serialize_prop(val: Any) -> Any:
    if val is None or isinstance(val, (bool, int, float, str)):
        return val
    if isinstance(val, (list, tuple)):
        return [_serialize_prop(x) for x in val]
    if isinstance(val, dict):
        return {str(k): _serialize_prop(v) for k, v in val.items()}
    return str(val)


def _node_key(node: Node) -> str:
    props = dict(node.items())
    if "_gid" in props and props["_gid"] is not None:
        return str(props["_gid"])
    if "id" in props and props["id"] is not None:
        return f"{next(iter(node.labels), 'Node')}:{props['id']}"
    return f"element:{node.element_id}"


def _rel_key(rel: Relationship, src_key: str, dst_key: str) -> str:
    props = dict(rel.items())
    if "_edge_id" in props and props["_edge_id"] is not None:
        return str(props["_edge_id"])
    return f"{src_key}-[{rel.type}->{dst_key}:{rel.element_id}"


def _extract_graph_from_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    nodes_map: dict[str, dict[str, Any]] = {}
    edges_map: dict[str, dict[str, Any]] = {}

    def add_node(n: Node) -> str:
        key = _node_key(n)
        labels = list(n.labels)
        primary_label = labels[0] if labels else "Node"
        props = {k: _serialize_prop(v) for k, v in dict(n.items()).items()}
        if key not in nodes_map:
            nodes_map[key] = {
                "id": key,
                "label": primary_label,
                "labels": labels,
                "properties": props,
            }
        return key

    def add_rel(r: Relationship) -> None:
        if r.start_node is None or r.end_node is None:
            return
        src_key = add_node(r.start_node)
        dst_key = add_node(r.end_node)
        eid = _rel_key(r, src_key, dst_key)
        props = {k: _serialize_prop(v) for k, v in dict(r.items()).items()}
        if eid not in edges_map:
            edges_map[eid] = {
                "id": eid,
                "source": src_key,
                "target": dst_key,
                "type": r.type,
                "properties": props,
            }

    def walk(val: Any) -> None:
        if isinstance(val, Node):
            add_node(val)
        elif isinstance(val, Relationship):
            add_rel(val)
        elif hasattr(val, "nodes") and hasattr(val, "relationships"):
            for n in val.nodes:
                add_node(n)
            for r in val.relationships:
                add_rel(r)
        elif isinstance(val, (list, tuple)):
            for item in val:
                walk(item)
        elif isinstance(val, dict):
            for item in val.values():
                walk(item)

    for rec in records:
        for v in rec.values():
            walk(v)

    return {
        "nodes": list(nodes_map.values()),
        "edges": list(edges_map.values()),
    }


def _run_cypher(cypher: str, params: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    driver = _get_driver()
    try:
        with driver.session() as session:
            result = session.run(cypher, parameters=params or {})
            records = [dict(r) for r in result]
            graph_data = _extract_graph_from_records(records)
            return records, graph_data
    finally:
        driver.close()


class CypherRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=8000)
    params: dict[str, Any] = Field(default_factory=dict)


class InjectFraudRequest(BaseModel):
    tx_id: str = Field(default="TX-LIVE-9099", min_length=1, max_length=64)
    sender_account_id: str = Field(default="101", min_length=1, max_length=64)
    receiver_account_id: str = Field(default="103", min_length=1, max_length=64)
    amount: float = Field(default=890000.0, gt=0)
    currency: str = Field(default="HKD", min_length=3, max_length=8)
    risk_type: str = Field(default="CIRCULAR_SPIKE", min_length=1, max_length=64)
    risk_score: float = Field(default=0.96, ge=0.0, le=1.0)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/health")
def health():
    try:
        _run_cypher("RETURN 1 AS ok")
        return {"status": "ok", "memgraph_url": MEMGRAPH_URI}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Memgraph unreachable: {exc}") from exc


@app.get("/api/graph")
def get_graph(limit: int = Query(default=250, ge=1, le=2000)):
    cypher = """
    MATCH (a)-[r]->(b)
    RETURN a, r, b
    LIMIT $limit
    """
    try:
        _, graph = _run_cypher(cypher, {"limit": limit})
        if not graph["nodes"]:
            _, fallback_graph = _run_cypher("MATCH (n) RETURN n LIMIT $limit", {"limit": limit})
            return fallback_graph
        return graph
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/query")
def run_custom_query(payload: CypherRequest):
    forbidden = ("DELETE", "DETACH", "DROP", "CREATE INDEX", "DROP INDEX")
    upper_q = payload.query.upper()
    if any(tok in upper_q for tok in forbidden):
        raise HTTPException(
            status_code=400,
            detail="Destructive or DDL statements are blocked on the ad-hoc query endpoint.",
        )
    try:
        records, graph = _run_cypher(payload.query, payload.params)
        # Convert non-graph values in records to JSON-safe primitives
        safe_records = []
        for rec in records:
            row = {}
            for k, v in rec.items():
                if isinstance(v, Node):
                    row[k] = {"_label": list(v.labels), **{pk: _serialize_prop(pv) for pk, pv in dict(v.items()).items()}}
                elif isinstance(v, Relationship):
                    row[k] = {"_type": v.type, **{pk: _serialize_prop(pv) for pk, pv in dict(v.items()).items()}}
                elif hasattr(v, "nodes") and hasattr(v, "relationships"):
                    row[k] = f"Path(nodes={len(v.nodes)}, rels={len(v.relationships)})"
                else:
                    row[k] = _serialize_prop(v)
            safe_records.append(row)
        return {"rows": safe_records, "graph": graph}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/fraud/{scenario}")
def run_fraud_scenario(scenario: str):
    if scenario == "cycles":
        # 2-to-5 hop circular fund transfers among accounts
        cypher = """
        MATCH p = (a1:Account)-[t1:TRANSFERRED_TO]->(a2:Account)-[t2:TRANSFERRED_TO]->(a3:Account)-[t3:TRANSFERRED_TO]->(a1)
        RETURN p, a1, t1, a2, t2, a3, t3
        LIMIT 50
        """
        description = "3-hop circular transfer loops (Account -> Account -> Account -> Account)"
    elif scenario == "high-risk-clearing":
        # Fraud assessments linked to transactions, clearing records, batches, and banks
        cypher = """
        MATCH p = (fa:FraudAssessment)<-[:HAS_FRAUD_ASSESSMENT]-(tx:Transaction)<-[cr:CLEARS_TRANSACTION]-(rec:ClearingRecord)-[ib:IN_CLEARING_BATCH]->(cb:ClearingBatch)-[sb:SETTLED_BY_BANK]->(bk:Bank)
        WHERE fa.risk_score >= 0.7 OR tx.status IN ['FLAGGED', 'BLOCKED', 'REVIEW']
        RETURN p, fa, tx, cr, rec, ib, cb, bk
        LIMIT 100
        """
        description = "High-risk fraud assessments traced through clearing records, batches, and settling banks"
    elif scenario == "fan-out":
        # High-value or multi-hop outbound transfer fan-out from sender accounts
        cypher = """
        MATCH (src:Account)-[t1:TRANSFERRED_TO]->(mid:Account)-[t2:TRANSFERRED_TO]->(dst:Account)
        WHERE src._gid <> dst._gid
        RETURN src, t1, mid, t2, dst
        LIMIT 100
        """
        description = "2-hop pass-through / layering chains across accounts"
    else:
        raise HTTPException(status_code=404, detail=f"Unknown scenario: {scenario}")

    try:
        records, graph = _run_cypher(cypher)
        return {
            "scenario": scenario,
            "description": description,
            "match_count": len(records),
            "graph": graph,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/fraud/inject")
def inject_fraud_case(payload: InjectFraudRequest):
    cypher = """
    MATCH (s:Account), (r:Account)
    WHERE (s._gid = $sender_id OR toString(s.id) = $sender_id OR s.account_no = $sender_id)
      AND (r._gid = $receiver_id OR toString(r.id) = $receiver_id OR r.account_no = $receiver_id)
    MERGE (tx:Transaction {_gid: $tx_gid})
    SET tx.id = $tx_id,
        tx.amount = $amount,
        tx.currency = $currency,
        tx.tx_type = 'WIRE',
        tx.status = 'FLAGGED',
        tx.ts = '2026-11-20T09:30:00Z'
    MERGE (s)-[sent:SENT_TRANSACTION {_edge_id: $sent_eid}]->(tx)
    MERGE (tx)-[recv:RECEIVED_TRANSACTION {_edge_id: $recv_eid}]->(r)
    MERGE (s)-[tf:TRANSFERRED_TO {_edge_id: $tf_eid}]->(r)
    SET tf.id = $tx_id,
        tf.amount = $amount,
        tf.currency = $currency,
        tf.ts = '2026-11-20T09:30:00Z',
        tf.tx_type = 'WIRE',
        tf.status = 'FLAGGED'
    MERGE (fa:FraudAssessment {_gid: $fa_gid})
    SET fa.id = $fa_id,
        fa.risk_type = $risk_type,
        fa.risk_score = $risk_score,
        fa.assessed_at = '2026-11-20T09:30:05Z'
    MERGE (tx)-[hfa:HAS_FRAUD_ASSESSMENT {_edge_id: $hfa_eid}]->(fa)
    RETURN s, tx, r, fa, sent, recv, tf, hfa
    """
    params = {
        "sender_id": payload.sender_account_id,
        "receiver_id": payload.receiver_account_id,
        "tx_gid": f"tx:{payload.tx_id}",
        "tx_id": payload.tx_id,
        "amount": payload.amount,
        "currency": payload.currency,
        "risk_type": payload.risk_type,
        "risk_score": payload.risk_score,
        "fa_id": f"FA-{payload.tx_id}",
        "fa_gid": f"fa:FA-{payload.tx_id}",
        "sent_eid": f"sent:{payload.tx_id}",
        "recv_eid": f"recv:{payload.tx_id}",
        "tf_eid": f"tf:{payload.tx_id}",
        "hfa_eid": f"hfa:{payload.tx_id}",
    }
    try:
        records, graph = _run_cypher(cypher, params)
        if not graph["nodes"]:
            raise HTTPException(
                status_code=404,
                detail="Sender or receiver account not found in Memgraph. Check account IDs/account_no.",
            )
        return {"injected": len(records) > 0, "graph": graph}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
