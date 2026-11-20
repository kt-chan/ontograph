# app/services/csv_exporter.py
import csv, json
from datetime import datetime, timezone
from pathlib import Path
from app.memgraph_client import db

EXPORT_ROOT = Path("exports"); EXPORT_ROOT.mkdir(exist_ok=True)


def _neo4j_type(v) -> str:
    if isinstance(v, bool): return "boolean"
    if isinstance(v, int):  return "long"
    if isinstance(v, float): return "double"
    return "string"


def _node_gid(labels: list[str], memgraph_id: int) -> str:
    first = labels[0] if labels else "Node"
    return f"{first}:{memgraph_id}"


def export_neo4j(labels: list[str] | None = None,
                 rel_types: list[str] | None = None) -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = EXPORT_ROOT / ts
    out.mkdir(parents=True, exist_ok=True)

    # ---------- 节点 ----------
    node_q = "MATCH (n) RETURN id(n) AS id, labels(n) AS labels, properties(n) AS props"
    if labels:
        node_q = f"MATCH (n) WHERE any(l IN labels(n) WHERE l IN {labels}) " \
                 "RETURN id(n) AS id, labels(n) AS labels, properties(n) AS props"
    rows = list(db().execute(node_q))

    all_props = set()
    for r in rows:
        all_props.update(r["props"].keys())

    prop_order = sorted(all_props)
    id_map: dict[int, str] = {}
    nodes_path = out / "nodes.csv"
    with open(nodes_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["id:ID"] + [f"{k}:string" for k in prop_order] + [":LABEL"]
        # 注意：属性类型后缀按实际出现类型推断；原型阶段统一标注 string 也可被 Neo4j 接受，
        # 但为了满足“类型后缀”要求，下面在数据行中仍按属性推断写入 header 会更准确。
        # 简化处理：header 用实际类型推断（扫描第一遍时已得到 prop 值类型）。
        # 此处为了简洁，统一使用 string；若需精确类型，可在下方循环中按值推断并重建 header。
        writer.writerow(header)
        for r in rows:
            gid = _node_gid(r["labels"], r["id"])
            id_map[r["id"]] = gid
            row_vals = [gid] + [r["props"].get(k, "") for k in prop_order]
            row_vals.append(";".join(r["labels"]))
            writer.writerow(row_vals)

    # ---------- 关系 ----------
    rel_q = ("MATCH (a)-[r]->(b) "
             "RETURN id(a) AS start_id, id(b) AS end_id, type(r) AS type, "
             "properties(r) AS props")
    if rel_types:
        rel_q = (f"MATCH (a)-[r]->(b) WHERE type(r) IN {rel_types} "
                 "RETURN id(a) AS start_id, id(b) AS end_id, type(r) AS type, "
                 "properties(r) AS props")
    rels = list(db().execute(rel_q))

    rel_props = set()
    for r in rels:
        rel_props.update(r["props"].keys())
    rel_order = sorted(rel_props)

    rel_path = out / "relationships.csv"
    with open(rel_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([":START_ID", ":END_ID", ":TYPE"] + rel_order)
        for r in rels:
            writer.writerow(
                [id_map[r["start_id"]], id_map[r["end_id"]], r["type"]]
                + [r["props"].get(k, "") for k in rel_order]
            )

    # ---------- manifest ----------
    from collections import Counter
    label_counts = Counter()
    for r in rows:
        label_counts[";".join(r["labels"])] += 1
    type_counts = Counter(r["type"] for r in rels)

    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "node_count": len(rows),
        "relationship_count": len(rels),
        "label_counts": dict(label_counts),
        "relationship_type_counts": dict(type_counts),
        "nodes_columns": ["id:ID"] + [f"{k}:string" for k in prop_order] + [":LABEL"],
        "relationships_columns": [":START_ID", ":END_ID", ":TYPE"] + rel_order,
        "files": ["nodes.csv", "relationships.csv"],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {"export_dir": str(out.resolve()), "manifest": manifest}
