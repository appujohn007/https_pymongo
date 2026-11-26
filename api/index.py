"""Public HTTPS RPC bridge for PyMongo-compatible clients.

This endpoint intentionally has no shared API key and no fixed Atlas URI. The caller's
MongoDB URI is sent over HTTPS with each operation and used only for that operation.
"""
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlsplit
from collections import OrderedDict
from contextlib import contextmanager
import hashlib
import json
import threading
import time

from bson import json_util
from pymongo import MongoClient

_MAX_REQUEST_BYTES = 4_000_000
_MAX_RESPONSE_BYTES = 4_000_000
_MAX_CACHED_CLIENTS = 8
_CLIENT_IDLE_TTL_SECONDS = 120
_DEFAULT_MAX_POOL_SIZE = 5
_CLIENT_CACHE = OrderedDict()
_CLIENT_CACHE_LOCK = threading.RLock()
_CLIENT_OPTION_NAMES = {
    "appname", "authsource", "authmechanism", "tls", "ssl", "replicaset", "directconnection",
    "serverselectiontimeoutms", "connecttimeoutms", "sockettimeoutms", "retrywrites", "retryreads",
    "readpreference", "read_preference", "w", "wtimeoutms", "wtimeout", "journal", "compressors",
    "maxpoolsize", "minpoolsize", "maxidletimems", "maxconnecting", "waitqueuetimeoutms",
}
_CURSOR_OPTIONS = {"skip", "limit", "sort", "batch_size", "hint", "comment", "max_time_ms", "collation"}


class ResponseTooLarge(Exception):
    pass


def _validate_uri(uri):
    if not isinstance(uri, str):
        raise ValueError("A MongoDB URI is required")
    scheme = urlsplit(uri).scheme.lower()
    if scheme not in ("mongodb", "mongodb+srv"):
        raise ValueError("URI must use mongodb:// or mongodb+srv://")


def _client_cache_key(uri, options):
    material = uri + "\0" + json.dumps(options, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _make_client(uri, options):
    # Bound pools: Vercel can create many instances, and each cached URI owns a pool.
    options = dict(options)
    requested_pool = options.get("maxPoolSize", options.get("maxpoolsize", _DEFAULT_MAX_POOL_SIZE))
    try: requested_pool = int(requested_pool)
    except (TypeError, ValueError): requested_pool = _DEFAULT_MAX_POOL_SIZE
    max_pool = max(1, min(requested_pool, 10))
    requested_min = options.get("minPoolSize", options.get("minpoolsize", 0))
    try: requested_min = int(requested_min)
    except (TypeError, ValueError): requested_min = 0
    options.pop("maxpoolsize", None)
    options.pop("minpoolsize", None)
    options.pop("maxidletimems", None)
    options["maxPoolSize"] = max_pool
    options["minPoolSize"] = max(0, min(requested_min, max_pool))
    options.setdefault("maxIdleTimeMS", 120_000)
    return MongoClient(uri, **options)


def _collect_evictions(now, *, max_entries=_MAX_CACHED_CLIENTS):
    """Remove idle or excess clients; never close one while a request is using it."""
    closing = []
    with _CLIENT_CACHE_LOCK:
        for key, entry in list(_CLIENT_CACHE.items()):
            if entry["active"] == 0 and now - entry["last_used"] > _CLIENT_IDLE_TTL_SECONDS:
                closing.append(_CLIENT_CACHE.pop(key)["client"])
        while len(_CLIENT_CACHE) > max_entries:
            victim = next((key for key, entry in _CLIENT_CACHE.items() if entry["active"] == 0), None)
            if victim is None: break
            closing.append(_CLIENT_CACHE.pop(victim)["client"])
    return closing


def _close_clients(clients):
    for client in clients:
        try:
            client.close()
        except Exception:
            # Eviction must not fail the unrelated request that caused it.
            pass


@contextmanager
def _client_lease(uri, options):
    key = _client_cache_key(uri, options)
    now = time.monotonic()
    closing = _collect_evictions(now)
    with _CLIENT_CACHE_LOCK:
        entry = _CLIENT_CACHE.get(key)
        if entry is None:
            while len(_CLIENT_CACHE) >= _MAX_CACHED_CLIENTS:
                victim = next((candidate for candidate, value in _CLIENT_CACHE.items() if value["active"] == 0), None)
                if victim is None: break
                closing.append(_CLIENT_CACHE.pop(victim)["client"])
            entry = {"client": _make_client(uri, options), "active": 1, "last_used": now}
            if len(_CLIENT_CACHE) < _MAX_CACHED_CLIENTS:
                entry["cached"] = True
                _CLIENT_CACHE[key] = entry
            else:
                # Keep the cache strictly bounded under bursts of unique URIs.
                entry["cached"] = False
        else:
            _CLIENT_CACHE.move_to_end(key)
            entry["active"] += 1
            entry["last_used"] = now
        client = entry["client"]
    _close_clients(closing)
    try:
        yield client
    finally:
        if not entry["cached"]:
            _close_clients([client])
        else:
            with _CLIENT_CACHE_LOCK:
                entry["active"] -= 1
                entry["last_used"] = time.monotonic()
                _CLIENT_CACHE.move_to_end(key)
            _close_clients(_collect_evictions(time.monotonic()))


def _clear_client_cache():
    """Test helper; also useful to release clients during controlled shutdowns."""
    with _CLIENT_CACHE_LOCK:
        clients = [entry["client"] for entry in _CLIENT_CACHE.values()]
        _CLIENT_CACHE.clear()
    _close_clients(clients)


def _pack(value):
    """Convert PyMongo cursors and result objects into Extended-JSON-safe values."""
    name = type(value).__name__
    if name == "Database":
        return {"__https_pymongo_object__": "Database", "name": value.name}
    if name == "Collection":
        return {"__https_pymongo_object__": "Collection", "database": value.database.name, "name": value.name}
    if name in ("Cursor", "CommandCursor", "RawBatchCursor", "SearchIndexCursor"):
        items = []
        estimated_bytes = 2
        for item in value:
            packed = _pack(item)
            estimated_bytes += len(json_util.dumps(packed).encode("utf-8")) + 1
            if estimated_bytes > _MAX_RESPONSE_BYTES - 512:
                raise ResponseTooLarge("Cursor result exceeds the HTTPS response limit; apply a smaller limit")
            items.append(packed)
        return items
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
    if not isinstance(options, dict):
        raise ValueError("client_options must be an object")
    if any(not isinstance(key, str) for key in options):
        raise ValueError("client option names must be strings")
    options = {key: value for key, value in options.items() if key.lower() in _CLIENT_OPTION_NAMES}
    args = data.get("args") or []
    kwargs = data.get("kwargs") or {}
    if not isinstance(args, list) or not isinstance(kwargs, dict):
        raise ValueError("args must be an array and kwargs must be an object")
    with _client_lease(uri, options) as client:
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
        result = method(*args, **kwargs)
        if type(result).__name__ in ("Cursor", "CommandCursor", "RawBatchCursor", "SearchIndexCursor"):
            cursor_options = data.get("cursor") or {}
            for option, value in cursor_options.items():
                if option in _CURSOR_OPTIONS and value is not None:
                    getattr(result, option)(value)
            return _pack(result)
        return _pack(result)


class handler(BaseHTTPRequestHandler):
    def _reply(self, status, value):
        body = json_util.dumps(value).encode("utf-8")
        if len(body) > _MAX_RESPONSE_BYTES:
            status = 413
            body = json_util.dumps({"ok": False, "error": "Response exceeds the HTTPS bridge size limit", "type": "ResponseTooLarge"}).encode("utf-8")
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
        except ResponseTooLarge as exc:
            self._reply(413, {"ok": False, "error": str(exc), "type": type(exc).__name__})
        except (KeyError, TypeError, ValueError) as exc:
            self._reply(400, {"ok": False, "error": str(exc), "type": type(exc).__name__})
        except Exception as exc:
            self._reply(502, {"ok": False, "error": str(exc), "type": type(exc).__name__})
