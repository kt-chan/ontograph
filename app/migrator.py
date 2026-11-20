# app/migrator.py
import argparse
import logging
import os
import re
import subprocess
import uuid
from collections import defaultdict, deque
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
JOBS: dict[str, dict[str, Any]] = {}

DEFAULT_GRAPH_CONFIG = Path("config/antifraud_graph.yaml")
STRATEGY = "deterministic"
BATCH_SIZE = 2000
MAX_TAIL_CHARS = 4000

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_ident(name: str, kind: str = "identifier") -> str:
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(f"Unsafe or invalid {kind}: {name!r}")
    return name


def _resolve_graph_config_path() -> Path:
    configured = getattr(settings, "graph_config_path", "") or os.getenv("GRAPH_CONFIG_PATH", "")
    if configured:
        return Path(configured)
    return DEFAULT_GRAPH_CONFIG


def _load_yaml_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Graph YAML config not found at: {config_path}")
    try:
        import yaml  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "PyYAML is required to parse config/antifraud_graph.yaml (`pip install pyyaml`)."
        ) from exc

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML structure in {config_path}")
    return data


def _build_env(graph_config_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            # PostgreSQL
            "SOURCE_DB_TYPE": "postgresql",
            "POSTGRES_HOST": settings.postgres_host,
            "POSTGRES_PORT": str(settings.postgres_port),
            "POSTGRES_DATABASE": settings.db_name,
            "POSTGRES_USER": settings.postgres_user,
            "POSTGRES_PASSWORD": settings.postgres_password,
            "POSTGRES_SCHEMA": settings.postgres_schema,
            # Memgraph
            "MEMGRAPH_URL": settings.memgraph_url,
            "MEMGRAPH_USERNAME": settings.memgraph_username,
            "MEMGRAPH_PASSWORD": settings.memgraph_password,
            # Graph config
            "GRAPH_CONFIG_PATH": str(graph_config_path),
        }
    )
    return env


def _format_tail(tail_lines: deque[str], max_chars: int = MAX_TAIL_CHARS) -> str:
    joined = "".join(tail_lines)
    if len(joined) <= max_chars:
        return joined
    sliced = joined[-max_chars:]
    first_newline = sliced.find("\n")
    if 0 <= first_newline < len(sliced) - 1:
        return sliced[first_newline + 1 :]
    return sliced


def _append_log(
    log_path: Path, tail_lines: deque[str], msg: str, level: str = "INFO"
) -> None:
    line = (
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} "
        f"- app.migrator - {level} - {msg}\n"
    )
    with open(log_path, "a", encoding="utf-8") as logf:
        logf.write(line)
    tail_lines.append(line)
    while len("".join(tail_lines)) > MAX_TAIL_CHARS * 2 and len(tail_lines) > 1:
        tail_lines.popleft()


def _serialize_val(val: Any) -> Any:
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, uuid.UUID):
        return str(val)
    return val


def _connect_pg():
    try:
        import psycopg2  # type: ignore

        return psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.db_name,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )
    except ImportError:
        import psycopg  # type: ignore

        return psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.db_name,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )


def _fetch_table_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    _validate_ident(schema, "schema")
    _validate_ident(table, "table")

    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    cols = [_validate_ident(r[0], "column") for r in cur.fetchall()]
    if not cols:
        return []

    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    cur.execute(f'SELECT {quoted_cols} FROM "{schema}"."{table}"')
    raw_rows = cur.fetchall()
    return [
        {col: _serialize_val(val) for col, val in zip(cols, row)}
        for row in raw_rows
    ]


def _iter_chunks(items: list[Any], size: int = BATCH_SIZE):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _truncate_memgraph(session, log_path: Path, tail_lines: deque[str]) -> None:
    """Truncate all nodes, edges, and indexes in Memgraph for a clean snapshot reload."""
    _append_log(log_path, tail_lines, "Truncating all nodes, edges, and indexes in Memgraph")
    session.run("MATCH (n) DETACH DELETE n").consume()

    try:
        idx_rows = [dict(row) for row in session.run("SHOW INDEX INFO")]
        for row_dict in idx_rows:
            label_or_type = row_dict.get("label") or row_dict.get("edge type")
            prop = row_dict.get("property") or row_dict.get("properties")
            idx_name = row_dict.get("name") or row_dict.get("index name") or row_dict.get("id")

            dropped = False
            if label_or_type and prop and _IDENT_RE.match(str(label_or_type)) and _IDENT_RE.match(str(prop)):
                drop_queries = (
                    f"DROP INDEX ON :{label_or_type}({prop})",
                    f"DROP EDGE INDEX ON :{label_or_type}({prop})",
                )
                for q in drop_queries:
                    try:
                        session.run(q).consume()
                        dropped = True
                        break
                    except Exception:
                        continue

            if not dropped and idx_name and _IDENT_RE.match(str(idx_name)):
                for q in (f"DROP INDEX ON `{idx_name}`", f"DROP INDEX {idx_name}"):
                    try:
                        session.run(q).consume()
                        dropped = True
                        break
                    except Exception:
                        continue

            if not dropped:
                _append_log(
                    log_path,
                    tail_lines,
                    f"Could not drop index entry {row_dict}; continuing",
                    level="WARNING",
                )
    except Exception as exc:
        _append_log(
            log_path,
            tail_lines,
            f"SHOW INDEX INFO / index cleanup warning: {exc}",
            level="WARNING",
        )


def _create_indexes(
    session,
    node_specs: list[dict[str, Any]],
    rel_specs: list[dict[str, Any]],
    log_path: Path,
    tail_lines: deque[str],
) -> None:
    for n_spec in node_specs:
        label = _validate_ident(n_spec["label"], "node label")
        idx_cols = list(dict.fromkeys(["_gid", *(n_spec.get("indexes") or [])]))
        for col in idx_cols:
            _validate_ident(col, "index column")
            try:
                session.run(f"CREATE INDEX ON :{label}({col})").consume()
            except Exception as exc:
                _append_log(
                    log_path,
                    tail_lines,
                    f"Node index skipped/failed on :{label}({col}): {exc}",
                    level="WARNING",
                )

    for r_spec in rel_specs:
        rel_type = _validate_ident(r_spec["type"], "relationship type")
        for col in r_spec.get("indexes") or []:
            _validate_ident(col, "relationship index column")
            created = False
            for q in (
                f"CREATE EDGE INDEX ON :{rel_type}({col})",
                f"CREATE INDEX ON :{rel_type}({col})",
            ):
                try:
                    session.run(q).consume()
                    created = True
                    break
                except Exception:
                    continue
            if not created:
                _append_log(
                    log_path,
                    tail_lines,
                    f"Relationship index skipped/failed on :{rel_type}({col})",
                    level="WARNING",
                )


def _verify_memgraph_state(
    driver, log_path: Path, tail_lines: deque[str]
) -> dict[str, Any]:
    """Query Memgraph after loading to verify node labels and relationship counts."""
    with driver.session() as session:
        label_rows = session.run(
            """
            MATCH (n)
            RETURN coalesce(labels(n)[0], '<Unlabeled>') AS label, count(*) AS cnt
            ORDER BY label
            """
        )
        node_counts = {row["label"]: row["cnt"] for row in label_rows}

        rel_rows = session.run(
            """
            MATCH ()-[r]->()
            RETURN type(r) AS rel_type, count(*) AS cnt
            ORDER BY rel_type
            """
        )
        rel_counts = {row["rel_type"]: row["cnt"] for row in rel_rows}

    total_nodes = sum(node_counts.values())
    total_rels = sum(rel_counts.values())
    _append_log(
        log_path,
        tail_lines,
        f"Verified Memgraph state: total_nodes={total_nodes}, total_rels={total_rels}",
    )
    _append_log(log_path, tail_lines, f"Node counts: {node_counts}")
    _append_log(log_path, tail_lines, f"Relationship counts: {rel_counts}")
    return {
        "total_nodes": total_nodes,
        "total_rels": total_rels,
        "node_counts": node_counts,
        "rel_counts": rel_counts,
    }


def _resolve_node_gid(
    table: str,
    col_name: str,
    val: Any,
    node_spec: dict[str, Any],
    lookup_by_table_col: dict[tuple[str, str], dict[str, str]],
) -> str | None:
    if val is None:
        return None
    str_val = str(val)
    col_map = lookup_by_table_col.get((table, col_name), {})
    if str_val in col_map:
        return col_map[str_val]
    id_col = node_spec.get("id_column", "id")
    id_map = lookup_by_table_col.get((table, id_col), {})
    return id_map.get(str_val)


def _build_edges_for_spec(
    r_spec: dict[str, Any],
    rows_by_table: dict[str, list[dict[str, Any]]],
    prepared_nodes_by_table: dict[str, list[dict[str, Any]]],
    node_spec_by_table: dict[str, dict[str, Any]],
    lookup_by_table_col: dict[tuple[str, str], dict[str, str]],
    rows_by_table_col_val: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> tuple[str, str, str, list[dict[str, Any]]]:
    rel_type = _validate_ident(r_spec["type"], "relationship type")
    from_table = _validate_ident(r_spec["from_table"], "from_table")
    from_col = _validate_ident(r_spec["from_column"], "from_column")
    to_table = _validate_ident(r_spec["to_table"], "to_table")
    to_col = _validate_ident(r_spec["to_column"], "to_column")
    source_table = r_spec.get("source_table")
    if source_table is not None:
        source_table = _validate_ident(source_table, "source_table")
    rel_prop_cols = [_validate_ident(p, "relationship property") for p in (r_spec.get("properties") or [])]

    from_spec = node_spec_by_table.get(from_table, {"id_column": "id"})
    to_spec = node_spec_by_table.get(to_table, {"id_column": "id"})
    from_label = _validate_ident(from_spec.get("label") or from_table.capitalize(), "from_label")
    to_label = _validate_ident(to_spec.get("label") or to_table.capitalize(), "to_label")

    edges: list[dict[str, Any]] = []

    if source_table:
        for idx, src_row in enumerate(rows_by_table.get(source_table, [])):
            src_gid = _resolve_node_gid(
                from_table,
                from_col,
                src_row.get(from_col),
                from_spec,
                lookup_by_table_col,
            )
            dst_gid = _resolve_node_gid(
                to_table,
                to_col,
                src_row.get(to_col),
                to_spec,
                lookup_by_table_col,
            )
            if not src_gid or not dst_gid:
                continue

            edge_id = str(
                src_row.get("id")
                if src_row.get("id") is not None
                else f"{rel_type}:{idx}"
            )
            e_props: dict[str, Any] = {"_edge_id": edge_id}
            for p in rel_prop_cols:
                if p in src_row and src_row[p] is not None:
                    e_props[p] = src_row[p]

            edges.append(
                {
                    "src_gid": src_gid,
                    "dst_gid": dst_gid,
                    "edge_id": edge_id,
                    "props": e_props,
                }
            )
        return rel_type, from_label, to_label, edges

    from_id_col = from_spec.get("id_column", "id")
    to_id_col = to_spec.get("id_column", "id")

    if from_col == from_id_col and to_col != to_id_col:
        for f_item in prepared_nodes_by_table.get(from_table, []):
            f_val = f_item["raw"].get(from_col)
            if f_val is None:
                continue
            src_gid = f_item["gid"]
            matching_to_rows = rows_by_table_col_val.get((to_table, to_col, str(f_val)), [])
            for idx, t_raw in enumerate(matching_to_rows):
                dst_gid = _resolve_node_gid(
                    to_table,
                    to_id_col,
                    t_raw.get(to_id_col),
                    to_spec,
                    lookup_by_table_col,
                )
                if not dst_gid:
                    continue
                edge_id = f"{rel_type}:{src_gid}->{dst_gid}:{idx}"
                e_props: dict[str, Any] = {"_edge_id": edge_id}
                for p in rel_prop_cols:
                    if p in f_item["raw"] and f_item["raw"][p] is not None:
                        e_props[p] = f_item["raw"][p]
                    elif p in t_raw and t_raw[p] is not None:
                        e_props[p] = t_raw[p]
                edges.append(
                    {
                        "src_gid": src_gid,
                        "dst_gid": dst_gid,
                        "edge_id": edge_id,
                        "props": e_props,
                    }
                )
    else:
        for idx, f_item in enumerate(prepared_nodes_by_table.get(from_table, [])):
            f_raw = f_item["raw"]
            f_val = f_raw.get(from_col)
            if f_val is None:
                continue

            if to_col == to_id_col:
                dst_gid = _resolve_node_gid(
                    to_table,
                    to_col,
                    f_val,
                    to_spec,
                    lookup_by_table_col,
                )
                if not dst_gid:
                    continue
                edge_id = f"{rel_type}:{f_item['gid']}->{dst_gid}:{idx}"
                e_props = {"_edge_id": edge_id}
                for p in rel_prop_cols:
                    if p in f_raw and f_raw[p] is not None:
                        e_props[p] = f_raw[p]
                edges.append(
                    {
                        "src_gid": f_item["gid"],
                        "dst_gid": dst_gid,
                        "edge_id": edge_id,
                        "props": e_props,
                    }
                )
            else:
                matching_to_rows = rows_by_table_col_val.get((to_table, to_col, str(f_val)), [])
                for t_idx, t_raw in enumerate(matching_to_rows):
                    dst_gid = _resolve_node_gid(
                        to_table,
                        to_id_col,
                        t_raw.get(to_id_col),
                        to_spec,
                        lookup_by_table_col,
                    )
                    if not dst_gid:
                        continue
                    edge_id = f"{rel_type}:{f_item['gid']}->{dst_gid}:{idx}:{t_idx}"
                    e_props = {"_edge_id": edge_id}
                    for p in rel_prop_cols:
                        if p in f_raw and f_raw[p] is not None:
                            e_props[p] = f_raw[p]
                        elif p in t_raw and t_raw[p] is not None:
                            e_props[p] = t_raw[p]
                    edges.append(
                        {
                            "src_gid": f_item["gid"],
                            "dst_gid": dst_gid,
                            "edge_id": edge_id,
                            "props": e_props,
                        }
                    )

    return rel_type, from_label, to_label, edges


def _load_from_yaml_config(
    config_path: Path, log_path: Path, tail_lines: deque[str]
) -> int:
    """Truncate Memgraph and load nodes & relationships defined in config/antifraud_graph.yaml."""
    from neo4j import GraphDatabase  # type: ignore

    cfg = _load_yaml_config(config_path)
    migration_cfg = cfg.get("migration") or {}
    schema = _validate_ident(
        migration_cfg.get("schema") or settings.postgres_schema or "public",
        "schema",
    )
    node_specs: list[dict[str, Any]] = cfg.get("nodes") or []
    rel_specs: list[dict[str, Any]] = cfg.get("relationships") or []

    _append_log(
        log_path,
        tail_lines,
        f"Loading graph snapshot from YAML '{config_path}' (schema='{schema}', "
        f"nodes={len(node_specs)}, relationships={len(rel_specs)})",
    )

    needed_tables: list[str] = []
    for n in node_specs:
        tbl = _validate_ident(n["table"], "node table")
        if tbl not in needed_tables:
            needed_tables.append(tbl)
    for r in rel_specs:
        for t_key in ("source_table", "from_table", "to_table"):
            t_name = r.get(t_key)
            if t_name:
                validated_t = _validate_ident(t_name, t_key)
                if validated_t not in needed_tables:
                    needed_tables.append(validated_t)

    rows_by_table: dict[str, list[dict[str, Any]]] = {}
    with _connect_pg() as pg_conn:
        with pg_conn.cursor() as cur:
            for tbl in needed_tables:
                rows_by_table[tbl] = _fetch_table_rows(cur, schema, tbl)

    node_spec_by_table: dict[str, dict[str, Any]] = {}
    lookup_by_table_col: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    rows_by_table_col_val: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    prepared_nodes_by_table: dict[str, list[dict[str, Any]]] = {}

    for tbl, tbl_rows in rows_by_table.items():
        for raw in tbl_rows:
            for col_name, col_val in raw.items():
                if col_val is not None:
                    rows_by_table_col_val[(tbl, col_name, str(col_val))].append(raw)

    for n_spec in node_specs:
        table = _validate_ident(n_spec["table"], "node table")
        label = _validate_ident(n_spec["label"], "node label")
        id_col = _validate_ident(n_spec.get("id_column", "id"), "id_column")
        prop_cols = [_validate_ident(p, "node property") for p in (n_spec.get("properties") or [])]
        node_spec_by_table[table] = n_spec

        prepared: list[dict[str, Any]] = []
        for idx, raw in enumerate(rows_by_table.get(table, [])):
            gid_val = raw.get(id_col) if raw.get(id_col) is not None else f"{table}:{idx}"
            gid = str(gid_val)
            props: dict[str, Any] = {"_gid": gid}
            for p in prop_cols:
                if p in raw and raw[p] is not None:
                    props[p] = raw[p]
            if id_col not in props and raw.get(id_col) is not None:
                props[id_col] = raw[id_col]

            prepared.append({"gid": gid, "props": props, "raw": raw})

            lookup_by_table_col[(table, id_col)][str(gid_val)] = gid
            lookup_by_table_col[(table, "_gid")][gid] = gid
            for col_name, col_val in raw.items():
                if col_val is not None and (table, col_name) not in lookup_by_table_col:
                    lookup_by_table_col[(table, col_name)][str(col_val)] = gid

        prepared_nodes_by_table[table] = prepared
        _append_log(
            log_path,
            tail_lines,
            f"Prepared :{label} from '{schema}.{table}' -> {len(prepared)} rows",
        )

    prepared_rel_batches: list[tuple[str, str, str, list[dict[str, Any]]]] = []
    for r_spec in rel_specs:
        rel_type, from_label, to_label, edges = _build_edges_for_spec(
            r_spec=r_spec,
            rows_by_table=rows_by_table,
            prepared_nodes_by_table=prepared_nodes_by_table,
            node_spec_by_table=node_spec_by_table,
            lookup_by_table_col=lookup_by_table_col,
            rows_by_table_col_val=rows_by_table_col_val,
        )
        prepared_rel_batches.append((rel_type, from_label, to_label, edges))

    auth = (
        (settings.memgraph_username, settings.memgraph_password)
        if settings.memgraph_username
        else None
    )
    driver = GraphDatabase.driver(settings.memgraph_url, auth=auth)

    try:
        with driver.session() as session:
            _truncate_memgraph(session, log_path, tail_lines)
            _create_indexes(session, node_specs, rel_specs, log_path, tail_lines)

            total_nodes = 0
            for n_spec in node_specs:
                table = _validate_ident(n_spec["table"], "node table")
                label = _validate_ident(n_spec["label"], "node label")
                batch = [
                    {"gid": item["gid"], "props": item["props"]}
                    for item in prepared_nodes_by_table.get(table, [])
                ]
                if not batch:
                    continue
                for chunk in _iter_chunks(batch, BATCH_SIZE):
                    session.run(
                        f"""
                        UNWIND $rows AS row
                        MERGE (n:{label} {{_gid: row.gid}})
                        SET n += row.props
                        """,
                        rows=chunk,
                    ).consume()
                total_nodes += len(batch)

            total_rels = 0
            for rel_type, from_label, to_label, edges in prepared_rel_batches:
                if edges:
                    for chunk in _iter_chunks(edges, BATCH_SIZE):
                        session.run(
                            f"""
                            UNWIND $edges AS edge
                            MATCH (a:{from_label} {{_gid: edge.src_gid}}),
                                  (b:{to_label} {{_gid: edge.dst_gid}})
                            MERGE (a)-[r:{rel_type} {{_edge_id: edge.edge_id}}]->(b)
                            SET r += edge.props
                            """,
                            edges=chunk,
                        ).consume()

                total_rels += len(edges)
                _append_log(
                    log_path,
                    tail_lines,
                    f"Loaded relationship :{rel_type} ({from_label} -> {to_label}): {len(edges)} edges",
                )

            _append_log(
                log_path,
                tail_lines,
                f"YAML graph load complete: total_nodes={total_nodes}, total_rels={total_rels}",
            )
            _verify_memgraph_state(driver, log_path, tail_lines)

        return 0
    except Exception as exc:
        _append_log(log_path, tail_lines, f"YAML graph loader failed: {exc}", level="ERROR")
        logger.exception("YAML graph loader failed: %s", exc)
        return 1
    finally:
        driver.close()


def run_migration(mode: str = "automatic") -> dict:
    graph_config_path = _resolve_graph_config_path()
    env = _build_env(graph_config_path)

    logger.info(
        "Starting migration job: strategy=%s mode=%s config=%s",
        STRATEGY,
        mode,
        graph_config_path,
    )

    job_id = uuid.uuid4().hex
    log_path = LOG_DIR / f"migrate_{job_id}.log"
    cmd = [
        "structured2graph",
        "--mode",
        mode,
        "--strategy",
        STRATEGY,
        "--meta-graph",
        "reset",
        "--log-level",
        "INFO",
    ]
    started = datetime.now(timezone.utc)
    tail_lines: deque[str] = deque()

    logger.info("Running migration job command: %s", " ".join(cmd))

    with open(log_path, "w", encoding="utf-8") as logf:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            logf.write(line)
            tail_lines.append(line)
            while len("".join(tail_lines)) > MAX_TAIL_CHARS * 2 and len(tail_lines) > 1:
                tail_lines.popleft()
        proc.wait()

    returncode = proc.returncode
    log_tail = _format_tail(tail_lines, MAX_TAIL_CHARS)

    if (
        "There is no procedure named 'migrate.postgresql'." in log_tail
        or returncode != 0
    ):
        returncode = _load_from_yaml_config(graph_config_path, log_path, tail_lines)
        log_tail = _format_tail(tail_lines, MAX_TAIL_CHARS)

    JOBS[job_id] = {
        "job_id": job_id,
        "strategy": STRATEGY,
        "mode": mode,
        "returncode": returncode,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "log_file": str(log_path),
        "log_tail": log_tail,
    }
    return JOBS[job_id]


def get_job(job_id: str) -> dict | None:
    return JOBS.get(job_id)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic antifraud migration to Memgraph")
    parser.add_argument(
        "--mode",
        default="automatic",
        help="Migration mode (default: automatic)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    args = _parse_args()
    job = run_migration(mode=args.mode)
    print(f"Job finished: {job['job_id']} (returncode={job['returncode']})")
    print(f"Log file: {job['log_file']}")
    if job["log_tail"]:
        print("--- Log Tail ---")
        print(job["log_tail"])