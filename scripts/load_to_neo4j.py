#!/usr/bin/env python3
"""将 /export/neo4j 产出的 CSV 加载到 Neo4j。优先 neo4j-admin，备选 LOAD CSV。"""
import argparse, json, os, subprocess, sys
from pathlib import Path
from neo4j import GraphDatabase

NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7688")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "antifraud123")
NEO4J_DB       = os.getenv("NEO4J_DATABASE", "neo4j")


def check_empty(driver):
    with driver.session(database=NEO4J_DB) as s:
        n = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        r = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
    return n, r


def run_admin_import(export_dir: Path, force: bool) -> bool:
    nodes = export_dir / "nodes.csv"
    rels  = export_dir / "relationships.csv"
    if not nodes.exists() or not rels.exists():
        print("nodes.csv / relationships.csv 不存在"); return False
    cmd = ["neo4j-admin", "database", "import", "full"]
    if force:
        cmd.append("--overwrite-destination")
    cmd += [f"--nodes={nodes}", f"--relationships={rels}", NEO4J_DB]
    print("RUN:", " ".join(cmd))
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        print(res.stdout)
        if res.returncode != 0:
            print(res.stderr, file=sys.stderr)
        return res.returncode == 0
    except FileNotFoundError:
        print("neo4j-admin 不可用，进入备选路径")
        return False


def run_load_csv(driver, export_dir: Path):
    """备选路径：约束 + LOAD CSV。注意 CSV 需在 Neo4j 的 import 目录下。"""
    with driver.session(database=NEO4J_DB) as s:
        # 根据 manifest 创建约束
        manifest = json.loads((export_dir / "manifest.json").read_text())
        for label in manifest["label_counts"]:
            for single in label.split(";"):
                s.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:`{single}`) REQUIRE n.id IS UNIQUE")
        # 节点（全属性作为 map 传入，简化原型）
        s.run("""
            LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS row
            CALL (row) {
              WITH row
              MERGE (n {id: row.`id:ID`})
              SET n += row
            } IN TRANSACTIONS OF 1000 ROWS
        """)
        # 关系
        s.run("""
            LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row
            CALL (row) {
              WITH row
              MATCH (a {id: row.`:START_ID`})
              MATCH (b {id: row.`:END_ID`})
              CALL apoc.create.relationship(a, row.`:TYPE`, {}, b) YIELD rel
              RETURN rel
            } IN TRANSACTIONS OF 1000 ROWS
        """)


def verify(driver, export_dir: Path) -> bool:
    manifest = json.loads((export_dir / "manifest.json").read_text())
    with driver.session(database=NEO4J_DB) as s:
        n = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        r = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
    ok_n = n == manifest["node_count"]
    ok_r = r == manifest["relationship_count"]
    print(f"Nodes: Neo4j={n} manifest={manifest['node_count']} -> {'PASS' if ok_n else 'FAIL'}")
    print(f"Rels : Neo4j={r} manifest={manifest['relationship_count']} -> {'PASS' if ok_r else 'FAIL'}")
    return ok_n and ok_r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--export-dir", required=True)
    ap.add_argument("--force", action="store_true", help="清空/覆盖目标库")
    args = ap.parse_args()
    export_dir = Path(args.export_dir).resolve()

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    n, r = check_empty(driver)
    if (n or r) and not args.force:
        print(f"目标库非空（nodes={n}, rels={r}），使用 --force 清空后重试"); sys.exit(1)
    if args.force and (n or r):
        with driver.session(database=NEO4J_DB) as s:
            s.run("MATCH (n) DETACH DELETE n")
        print("已清空 Neo4j")

    if not run_admin_import(export_dir, args.force):
        print("使用 LOAD CSV 备选路径")
        run_load_csv(driver, export_dir)

    passed = verify(driver, export_dir)

    # 验证多跳可疑链
    with driver.session(database=NEO4J_DB) as s:
        chain = s.run("""
            MATCH (a)-[:SENT_TO*1..3]->(b)
            WHERE a.id = 'Account:201'
            RETURN count(*) AS c
        """).single()
        print(f"多跳路径验证（Account:201 出发 1-3 跳）: {chain['c']} 条")

    driver.close()
    sys.exit(0 if passed else 2)


if __name__ == "__main__":
    main()
