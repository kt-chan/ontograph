# app/services/memgraph_client.py
from gqlalchemy import Memgraph
from app.config import settings

_db: Memgraph | None = None


def db() -> Memgraph:
    global _db
    if _db is None:
        _db = Memgraph(
            host=settings.memgraph_url.replace("bolt://", "").split(":")[0],
            port=int(settings.memgraph_url.rsplit(":", 1)[-1]),
            username=settings.memgraph_username or None,
            password=settings.memgraph_password or None,
        )
    return _db


def get_schema() -> dict:
    labels = [r["label"] for r in db().execute("CALL db.labels() YIELD label RETURN label")]
    rel_types = [r["relationshipType"] for r in
                 db().execute("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")]
    prop_keys = [r["propertyKey"] for r in
                 db().execute("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey")]
    return {"labels": labels, "relationship_types": rel_types, "property_keys": prop_keys}
