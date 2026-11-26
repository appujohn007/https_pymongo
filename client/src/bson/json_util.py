"""Small Extended JSON codec for the BSON types used by this HTTP driver."""
import json
import base64
import re
from decimal import Decimal
from datetime import datetime, timezone
from .objectid import ObjectId


def _default(value):
    encoder = getattr(value, "_https_pymongo_encode", None)
    if callable(encoder): return encoder()
    if isinstance(value, ObjectId): return {"$oid": str(value)}
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return {"$date": {"$numberLong": str(int(dt.timestamp() * 1000))}}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"$binary": {"base64": base64.b64encode(bytes(value)).decode("ascii"), "subType": "00"}}
    if isinstance(value, Decimal): return {"$numberDecimal": str(value)}
    if isinstance(value, re.Pattern):
        flags = "".join(letter for flag, letter in ((re.IGNORECASE, "i"), (re.MULTILINE, "m"), (re.DOTALL, "s"), (re.UNICODE, "u")) if value.flags & flag)
        return {"$regularExpression": {"pattern": value.pattern, "options": flags}}
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
    if set(obj) == {"$numberDecimal"}: return Decimal(obj["$numberDecimal"])
    if set(obj) == {"$binary"}: return base64.b64decode(obj["$binary"]["base64"])
    if set(obj) == {"$regularExpression"}:
        value = obj["$regularExpression"]
        flags = 0
        if "i" in value["options"]: flags |= re.IGNORECASE
        if "m" in value["options"]: flags |= re.MULTILINE
        if "s" in value["options"]: flags |= re.DOTALL
        return re.compile(value["pattern"], flags)
    return obj


def loads(value, **kwargs): return json.loads(value, object_hook=_hook, **kwargs)
