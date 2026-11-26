"""PyMongo-shaped synchronous client that sends database calls through HTTPS."""
import os
import logging
import threading
from urllib.parse import unquote, urlsplit

import httpx

from bson import json_util
from .errors import ConfigurationError, OperationFailure, UnsupportedOperation, ConnectionFailure

_CURSOR_METHODS = {"find", "aggregate", "list_indexes", "list_search_indexes", "find_raw_batches", "aggregate_raw_batches"}
_CLIENT_CURSOR_METHODS = {"list_databases"}
_CLIENT_METHODS = {"list_database_names", "list_databases", "drop_database", "watch", "start_session", "server_info", "bulk_write", "append_metadata"}
_DATABASE_CURSOR_METHODS = {"aggregate", "list_collections"}
_DATABASE_METHODS = {"command", "list_collection_names", "list_collections", "create_collection", "drop_collection", "dereference", "watch", "validate_collection", "aggregate", "cursor_command", "with_options"}
_RETRYABLE_READS = {
    "list_database_names", "list_databases", "list_collection_names", "list_collections",
    "find", "find_one", "count_documents", "estimated_document_count", "distinct",
    "list_indexes", "list_search_indexes", "index_information", "options", "server_info",
}
_HTTPX_LOG_CONTEXT = threading.local()


class _BridgeHTTPXLogFilter(logging.Filter):
    """Hide only HTTPX request lines emitted while this package performs a call."""
    def filter(self, record):
        if (getattr(_HTTPX_LOG_CONTEXT, "active", False)
                and record.name == "httpx"
                and record.getMessage().startswith("HTTP Request:")):
            return False
        return True


if os.getenv("HTTPS_PYMONGO_HTTPX_LOGS", "").lower() not in {"1", "true", "yes"}:
    _httpx_logger = logging.getLogger("httpx")
    if not any(isinstance(item, _BridgeHTTPXLogFilter) for item in _httpx_logger.filters):
        _httpx_logger.addFilter(_BridgeHTTPXLogFilter())
_CLIENT_OPTIONS = {
    "appname", "authsource", "authmechanism", "tls", "ssl", "replicaset", "directconnection",
    "serverselectiontimeoutms", "connecttimeoutms", "sockettimeoutms", "retrywrites",
    "retryreads", "readpreference", "w", "wtimeoutms", "journal", "compressors", "maxpoolsize",
    "minpoolsize", "maxidletimems", "maxconnecting", "waitqueuetimeoutms",
}


class MongoClient:
    """Drop-in-shaped client; the supplied URI is sent only to the HTTPS bridge."""
    def __init__(self, host=None, port=None, *, api_url=None, **kwargs):
        if host is None and "uri" in kwargs:
            host = kwargs.pop("uri")
        if isinstance(host, (list, tuple)):
            host = ",".join(str(item) for item in host) if host else None
        if host is None:
            host = f"mongodb://localhost:{port or 27017}"
        if isinstance(host, int):
            host = f"mongodb://localhost:{host}"
        elif port is not None and isinstance(host, str) and not host.startswith(("mongodb://", "mongodb+srv://")):
            host = f"mongodb://{host}:{port}"
        elif isinstance(host, str) and not host.startswith(("mongodb://", "mongodb+srv://")):
            host = f"mongodb://{host}"
        self._uri = str(host)
        self._client_options = {k: v for k, v in kwargs.items() if k.lower() in _CLIENT_OPTIONS and isinstance(v, (str, int, float, bool, type(None), list, dict))}
        self.api_url = (api_url or os.getenv("HTTPS_PYMONGO_API_URL", "https://httpspymongo.vercel.app/api")).rstrip("/")
        self._closed = False
        # One pooled transport per MongoClient; Motor's worker threads share it safely.
        self._http = httpx.Client(timeout=60.0, limits=httpx.Limits(max_connections=20, max_keepalive_connections=10))

    def __getitem__(self, name): return Database(self, str(name))
    def get_database(self, name=None, **kwargs):
        if name is None:
            path = urlsplit(self._uri).path.strip("/")
            name = unquote(path.split("?", 1)[0]) if path else None
        if not name: raise ConfigurationError("No default database is specified in the URI")
        return self[name]
    def get_default_database(self, default=None, **kwargs):
        try: return self.get_database()
        except ConfigurationError:
            if default is not None: return self[default]
            raise
    def close(self):
        if not self._closed:
            self._closed = True
            self._http.close()
    def __enter__(self): return self
    def __exit__(self, *exc): self.close()

    def _request(self, target, database="", collection="", method="", args=(), kwargs=None, cursor=None):
        if self._closed: raise OperationFailure("MongoClient is closed")
        request_data = {
            "uri": self._uri, "target": target, "database": database,
            "collection": collection, "method": method,
            "args": list(args), "kwargs": kwargs or {}, "cursor": cursor or {},
            "client_options": self._client_options,
        }
        body = json_util.dumps(request_data).encode("utf-8")
        can_retry_read = method in _RETRYABLE_READS and target in {"client", "database", "collection"}
        response = envelope = None
        for attempt in range(2 if can_retry_read else 1):
            try:
                previous_log_context = getattr(_HTTPX_LOG_CONTEXT, "active", False)
                _HTTPX_LOG_CONTEXT.active = True
                try:
                    response = self._http.post(self.api_url, content=body, headers={"Content-Type": "application/json"})
                finally:
                    _HTTPX_LOG_CONTEXT.active = previous_log_context
            except (httpx.ReadError, httpx.RemoteProtocolError) as exc:
                if can_retry_read and attempt == 0:
                    continue
                raise ConnectionFailure(f"HTTPS API transport failed ({type(exc).__name__}): {exc}", timeout=isinstance(exc, httpx.TimeoutException)) from exc
            except httpx.HTTPError as exc:
                raise ConnectionFailure(f"HTTPS API transport failed ({type(exc).__name__}): {exc}", timeout=isinstance(exc, httpx.TimeoutException)) from exc
            try:
                envelope = json_util.loads(response.content.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                if can_retry_read and attempt == 0:
                    continue
                content_type = response.headers.get("content-type", "unknown")
                try:
                    standard_json = json_util.json.loads(response.content.decode("utf-8"))
                    shape = f"valid JSON ({type(standard_json).__name__})"
                    if isinstance(standard_json, dict): shape += f" with fields {', '.join(sorted(standard_json.keys())[:12])}"
                except Exception:
                    shape = "invalid JSON"
                raise OperationFailure(f"HTTPS bridge response decoding failed: {shape}; HTTP {response.status_code}, content-type {content_type}, {len(response.content)} bytes. Check API/client codec compatibility or deployment health") from exc
            break
        if response.is_error:
            raise _remote_error(envelope, response.status_code)
        if not envelope.get("ok"):
            raise _remote_error(envelope, response.status_code)
        return _decode(envelope.get("result"), self)

    def __getattr__(self, method):
        if method.startswith("_"): raise AttributeError(method)
        if method in _CLIENT_METHODS:
            if method in _CLIENT_CURSOR_METHODS:
                def cursor(*args, **kwargs): return SyncCursor(self, method, args, kwargs)
                return cursor
            def call(*args, **kwargs): return self._request("client", method=method, args=args, kwargs=kwargs)
            return call
        return self[method]


class Database:
    def __init__(self, client, name): self.client, self.name = client, name
    def __getitem__(self, name): return Collection(self, str(name))
    def get_collection(self, name, **kwargs): return self[name]
    def __getattr__(self, method):
        if method.startswith("_"): raise AttributeError(method)
        if method in _DATABASE_METHODS:
            if method in _DATABASE_CURSOR_METHODS:
                def cursor(*args, **kwargs): return SyncCursor(self, method, args, kwargs)
                return cursor
            def call(*args, **kwargs): return self.client._request("database", self.name, method=method, args=args, kwargs=kwargs)
            return call
        return self[method]


class Collection:
    def __init__(self, database, name): self.database, self.name = database, name
    def find(self, *args, **kwargs): return SyncCursor(self, "find", args, kwargs)
    def aggregate(self, *args, **kwargs): return SyncCursor(self, "aggregate", args, kwargs)
    def list_indexes(self, *args, **kwargs): return SyncCursor(self, "list_indexes", args, kwargs)
    def _call(self, method, args=(), kwargs=None, cursor=None):
        return self.database.client._request("collection", self.database.name, self.name, method, args, kwargs, cursor)
    def __getattr__(self, method):
        if method.startswith("_"): raise AttributeError(method)
        def call(*args, **kwargs):
            if method in _CURSOR_METHODS: return SyncCursor(self, method, args, kwargs)
            return self._call(method, args, kwargs)
        return call


class SyncCursor:
    def __init__(self, owner, method, args, kwargs):
        self.owner, self.method = owner, method
        self.args, self.kwargs = args, dict(kwargs)
        self.options = {}
        self._items = None
    def _set(self, key, value): self.options[key] = value; self._items = None; return self
    def skip(self, count): return self._set("skip", count)
    def limit(self, count): return self._set("limit", count)
    def sort(self, key_or_list, direction=None):
        order = [[key_or_list, direction]] if isinstance(key_or_list, str) else key_or_list
        return self._set("sort", order)
    def batch_size(self, size): return self._set("batch_size", size)
    def hint(self, hint): return self._set("hint", hint)
    def comment(self, comment): return self._set("comment", comment)
    def max_time_ms(self, value): return self._set("max_time_ms", value)
    def collation(self, value): return self._set("collation", value)
    def __iter__(self):
        if self._items is None:
            if isinstance(self.owner, Collection):
                self._items = self.owner._call(self.method, self.args, self.kwargs, self.options)
            elif isinstance(self.owner, Database):
                self._items = self.owner.client._request("database", self.owner.name, method=self.method, args=self.args, kwargs=self.kwargs, cursor=self.options)
            else:
                self._items = self.owner._request("client", method=self.method, args=self.args, kwargs=self.kwargs, cursor=self.options)
        return iter(self._items)
    def __len__(self): return len(list(iter(self)))
    def __getitem__(self, index): return list(iter(self))[index]
    def rewind(self): self._items = None; return self
    def clone(self):
        duplicate = SyncCursor(self.owner, self.method, self.args, self.kwargs)
        duplicate.options = dict(self.options)
        return duplicate
    def to_list(self, length=None):
        if length is not None and (not isinstance(length, int) or length < 0):
            raise ValueError("length must be a non-negative integer or None")
        if length is None: return list(iter(self))
        cursor = self.clone()
        existing = cursor.options.get("limit")
        cursor.limit(min(length, existing) if existing else length)
        return list(iter(cursor))


class _WriteResult:
    def __init__(self, value):
        self.raw_result = value
        for name, item in value.items(): setattr(self, name, item)
    @property
    def acknowledged(self): return bool(self.raw_result.get("ok", 1))


class InsertOneResult(_WriteResult): pass
class InsertManyResult(_WriteResult): pass
class UpdateResult(_WriteResult): pass
class DeleteResult(_WriteResult): pass
class BulkWriteResult(_WriteResult):
    @property
    def acknowledged(self): return bool(self.raw_result.get("ok", 1))


class ClientBulkWriteResult(BulkWriteResult):
    @property
    def has_verbose_results(self): return bool(self.raw_result.get("has_verbose_results", False))
    @property
    def insert_results(self): return self.raw_result.get("insert_results")
    @property
    def update_results(self): return self.raw_result.get("update_results")
    @property
    def delete_results(self): return self.raw_result.get("delete_results")


def _remote_error(payload, status):
    from . import errors
    message = payload.get("error", "HTTPS bridge operation failed")
    error_type = payload.get("type", "")
    code = payload.get("code")
    details = payload.get("details")
    if error_type in {"UnsupportedMethodError", "UnsupportedOperation"}:
        return UnsupportedOperation(message)
    if status == 503 or error_type in {"ServerSelectionTimeoutError", "ConnectionFailure", "NetworkTimeout"}:
        cls = getattr(errors, error_type, errors.ServerSelectionTimeoutError)
        return cls(message, timeout=error_type == "NetworkTimeout")
    if error_type == "DuplicateKeyError" or code == 11000:
        return errors.DuplicateKeyError(message, code=code, details=details)
    if error_type == "BulkWriteError":
        return errors.BulkWriteError(message, details=details)
    if error_type == "ConfigurationError":
        return errors.ConfigurationError(message)
    if error_type in {"WriteError", "WriteConcernError"}:
        return getattr(errors, error_type)(message, code=code, details=details)
    if status == 400 and error_type in {"ValueError", "TypeError", "KeyError"}:
        return errors.InvalidOperation(message)
    return errors.OperationFailure(message, code=code, details=details)


def _decode(value, client=None):
    if isinstance(value, dict) and "__https_pymongo_object__" in value and client is not None:
        kind = value["__https_pymongo_object__"]
        if kind == "Database": return client[value["name"]]
        if kind == "Collection": return client[value["database"]][value["name"]]
    if isinstance(value, dict) and "__https_pymongo_result__" in value:
        kind = value["__https_pymongo_result__"]
        payload = value.get("value", {})
        cls = {"InsertOneResult": InsertOneResult, "InsertManyResult": InsertManyResult,
               "UpdateResult": UpdateResult, "DeleteResult": DeleteResult,
               "BulkWriteResult": BulkWriteResult, "ClientBulkWriteResult": ClientBulkWriteResult}.get(kind)
        if cls: return cls(payload)
    if isinstance(value, list): return [_decode(item, client) for item in value]
    if isinstance(value, dict): return {key: _decode(item, client) for key, item in value.items()}
    return value
