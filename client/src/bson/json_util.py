"""Small Extended JSON codec for the BSON types used by this HTTP driver."""
import json
from datetime import datetime, timezone
from .objectid import ObjectId


def _default(value):
    if isinstance(value, ObjectId): return {"$oid": str(value)}
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return {"$date": {"$numberLong": str(int(dt.timestamp() * 1000))}}
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def dumps(value, **kwargs): return json.dumps(value, default=_default, **kwargs)


def _hook(obj):
    if set(obj) == {"$oid"}: return ObjectId(obj["$oid"])
    if set(obj) == {"$date"}:
        date = obj["$date"]
        millis = int(date["$numberLong"] if isinstance(date, dict) else date)
        return datetime.fromtimestamp(millis / 1000, timezone.utc)
    if set(obj) == {"$numberInt"}: return int(obj["$numberInt"])
    if set(obj) == {"$numberLong"}: return int(obj["$numberLong"])
    if set(obj) == {"$numberDouble"}: return float(obj["$numberDouble"])
    return obj


def loads(value, **kwargs): return json.loads(value, object_hook=_hook, **kwargs)
