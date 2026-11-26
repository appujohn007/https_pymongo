"""Public HTTPS RPC bridge for PyMongo-compatible clients.

This endpoint intentionally has no shared API key and no fixed Atlas URI. The caller's
MongoDB URI is sent over HTTPS with each operation and used only for that operation.
"""
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlsplit
import json

from bson import json_util
from pymongo import MongoClient

_MAX_REQUEST_BYTES = 2_000_000
_CLIENT_OPTION_NAMES = {
    "appname", "authsource", "authmechanism", "tls", "ssl", "replicaset", "directconnection",
    "serverselectiontimeoutms", "connecttimeoutms", "sockettimeoutms", "retrywrites", "retryreads",
    "readpreference", "w", "wtimeoutms", "journal", "compressors",
}
_CURSOR_OPTIONS = {"skip", "limit", "sort", "batch_size", "hint", "comment", "max_time_ms", "collation"}


def _validate_uri(uri):
    if not isinstance(uri, str):
        raise ValueError("A MongoDB URI is required")
    scheme = urlsplit(uri).scheme.lower()
    if scheme not in ("mongodb", "mongodb+srv"):
        raise ValueError("URI must use mongodb:// or mongodb+srv://")


def _pack(value):
    """Convert PyMongo cursors and result objects into Extended-JSON-safe values."""
    name = type(value).__name__
    if name == "Database":
        return {"__https_pymongo_object__": "Database", "name": value.name}
    if name == "Collection":
        return {"__https_pymongo_object__": "Collection", "database": value.database.name, "name": value.name}
    if name in ("Cursor", "CommandCursor", "RawBatchCursor", "SearchIndexCursor"):
        return [_pack(item) for item in value]
    result_types = {
        "InsertOneResult": "InsertOneResult", "InsertManyResult": "InsertManyResult",
        "UpdateResult": "UpdateResult", "DeleteResult": "DeleteResult",
    }
    if name in result_types:
        fields = {
            "InsertOneResult": ("inserted_id",),
            "InsertManyResult": ("inserted_ids",),
            "UpdateResult": ("matched_count", "modified_count", "upserted_id", "raw_result"),
            "DeleteResult": ("deleted_count",),
        }[name]
        payload = {field: getattr(value, field, None) for field in fields}
        return {"__https_pymongo_result__": result_types[name], "value": payload}
    if isinstance(value, tuple): return [_pack(item) for item in value]
    if isinstance(value, list): return [_pack(item) for item in value]
    if isinstance(value, dict): return {key: _pack(item) for key, item in value.items()}
    return value


def _run(data):
    uri = data.get("uri")
    _validate_uri(uri)
    target = data.get("target")
    method_name = data.get("method")
    if not isinstance(method_name, str) or not method_name or method_name.startswith("_"):
        raise ValueError("A public method name is required")
    options = data.get("client_options") or {}
    options = {key: value for key, value in options.items() if key.lower() in _CLIENT_OPTION_NAMES}
    client = MongoClient(uri, **options)
    try:
        if target == "client":
            receiver = client
        elif target == "database":
            receiver = client[data["database"]]
        elif target == "collection":
            receiver = client[data["database"]][data["collection"]]
        else:
            raise ValueError("Unknown operation target")
        method = getattr(receiver, method_name)
        if not callable(method):
            raise ValueError("Requested member is not callable")
        result = method(*(data.get("args") or []), **(data.get("kwargs") or {}))
        if type(result).__name__ in ("Cursor", "CommandCursor", "RawBatchCursor", "SearchIndexCursor"):
            cursor_options = data.get("cursor") or {}
            for option, value in cursor_options.items():
                if option in _CURSOR_OPTIONS and value is not None:
                    getattr(result, option)(value)
            result = list(result)
        return _pack(result)
    finally:
        client.close()


class handler(BaseHTTPRequestHandler):
    def _reply(self, status, value):
        body = json_util.dumps(value).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._reply(200, {"service": "https-pymongo", "status": "ok"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > _MAX_REQUEST_BYTES:
                self._reply(413 if length > _MAX_REQUEST_BYTES else 400, {"error": "Invalid request size"})
                return
            data = json_util.loads(self.rfile.read(length))
            self._reply(200, {"ok": True, "result": _run(data)})
        except (KeyError, TypeError, ValueError) as exc:
            self._reply(400, {"ok": False, "error": str(exc), "type": type(exc).__name__})
        except Exception as exc:
            self._reply(502, {"ok": False, "error": str(exc), "type": type(exc).__name__})
