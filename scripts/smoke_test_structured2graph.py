import importlib
import os
import sys

from neo4j import GraphDatabase

MEMGRAPH_URL = os.getenv("MEMGRAPH_URL", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USERNAME", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DATABASE", os.getenv("POSTGRES_DB", "antifraud"))
PG_USER = os.getenv("POSTGRES_USER", "antifraud")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "antifraud")
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

EXPECTED_TABLES = [
    "banks",
    "accounts",
    "transactions",
    "clearing_batches",
    "clearing_records",
    "fraud_assessments",
]


def _connect_pg():
    try:
        import psycopg  # type: ignore

        return psycopg.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
    except ImportError:
        import psycopg2  # type: ignore

        return psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )


def check_structured2graph_import() -> bool:
    try:
        mod = importlib.import_module("core.migration_agent")
        print(f"[OK] Imported core.migration_agent: {mod.__file__}")
        return True
    except Exception as exc:
        print(f"[FAIL] Could not import core.migration_agent: {exc}")
        return False


def check_memgraph_queries(driver) -> bool:
    try:
        with driver.session() as session:
            rec = session.run("RETURN 1 AS ok").single()
            assert rec and rec["ok"] == 1

            # Verify parameterized Cypher and cleanup in a single round-trip
            session.run(
                """
                MERGE (t:_SmokeTestNode {id: $id})
                SET t.label = $label
                WITH t
                MATCH (t2:_SmokeTestNode {id: $id})
                DELETE t2
                """,
                id="smoke_test_probe",
                label="ok",
            ).consume()
        print(
            f"[OK] Memgraph Bolt connection & Cypher query logic working ({MEMGRAPH_URL})"
        )
        return True
    except Exception as exc:
        print(f"[FAIL] Memgraph query check failed ({MEMGRAPH_URL}): {exc}")
        return False


def check_migrate_postgresql_procedure(driver) -> bool:
    try:
        with driver.session() as session:
            result = session.run("CALL mg.procedures() YIELD *")
            records = [dict(r) for r in result]
            print(f"[INFO] mg.procedures() returned {len(records)} procedure(s)")

            matched = []
            for row in records:
                text = " ".join(str(v) for v in row.values()).lower()
                if "migrate" in text or "cross_database" in text or "pg" in text:
                    matched.append(row)

            if matched:
                for item in matched:
                    print(f"[INFO] Matching procedure: {item}")
            else:
                print(
                    "[INFO] No 'migrate' / 'cross_database' procedure found in mg.procedures(); "
                    "direct Python loader fallback is recommended for deterministic migration."
                )
            return False
    except Exception as exc:
        print(f"[FAIL] Could not inspect Memgraph procedures: {exc}")
        return False


def check_postgres_schema_and_data() -> bool:
    try:
        with _connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_schema()")
                db_name, active_schema = cur.fetchone()
                print(
                    f"[OK] Connected to PostgreSQL ({PG_HOST}:{PG_PORT}, db={db_name}, schema={PG_SCHEMA}, active={active_schema})"
                )

                counts = {}
                for table in EXPECTED_TABLES:
                    cur.execute(f"SELECT COUNT(*) FROM {PG_SCHEMA}.{table}")
                    counts[table] = cur.fetchone()[0]

                print(f"[OK] PostgreSQL table row counts: {counts}")
                return True
    except Exception as exc:
        print(f"[FAIL] PostgreSQL database/query check failed: {exc}")
        return False


def main() -> int:
    import_ok = check_structured2graph_import()

    auth = (MEMGRAPH_USER, MEMGRAPH_PASSWORD) if MEMGRAPH_USER else None
    driver = GraphDatabase.driver(MEMGRAPH_URL, auth=auth)
    try:
        mg_query_ok = check_memgraph_queries(driver)
        proc_ok = check_migrate_postgresql_procedure(driver)
    finally:
        driver.close()

    pg_ok = check_postgres_schema_and_data()

    # Core environment is healthy if import, Memgraph queries, and Postgres queries pass,
    # even if migrate.postgresql is missing (which triggers your Python fallback).
    if import_ok and mg_query_ok and pg_ok:
        if proc_ok:
            print("[PASS] All checks (including migrate.postgresql) passed")
        else:
            print(
                "[PASS WITH FALLBACK NEEDED] Import, Memgraph queries, and PostgreSQL queries passed; "
                "use direct Python loader since migrate.postgresql is unavailable"
            )
        return 0

    print("[FAIL] One or more core checks failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
