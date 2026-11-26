"""PyMongo-shaped synchronous client that sends database calls through HTTPS."""
import os
import urllib.error
import urllib.request

from bson import json_util
from .errors import ConfigurationError, OperationFailure

_CURSOR_METHODS = {"find", "aggregate", "list_indexes", "list_search_indexes", "list_database_names_cursor"}
_CLIENT_METHODS = {"list_database_names", "list_databases", "drop_database", "watch", "start_session", "server_info", "database_names"}
_DATABASE_METHODS = {"command", "list_collection_names", "list_collections", "create_collection", "drop_collection", "dereference", "watch", "validate_collection", "create_view"}
_CLIENT_OPTIONS = {
    "appname", "authsource", "authmechanism", "tls", "ssl", "replicaset", "directconnection",
    "serverselectiontimeoutms", "connecttimeoutms", "sockettimeoutms", "retrywrites",
    "retryreads", "readpreference", "w", "wtimeoutms", "journal", "compressors",
}


class MongoClient:
    """Drop-in-shaped client; the supplied URI is sent only to the HTTPS bridge."""
    def __init__(self, host=None, port=None, *, api_url=None, **kwargs):
        if host is None and "uri" in kwargs:
            host = kwargs.pop("uri")
        if isinstance(host, (list, tuple)):
            host = host[0] if host else None
        if host is None:
            host = "mongodb://localhost:27017"
        if isinstance(host, int):
            host = f"mongodb://localhost:{host}"
        self._uri = str(host)
        self._client_options = {k: v for k, v in kwargs.items() if k.lower() in _CLIENT_OPTIONS and isinstance(v, (str, int, float, bool, type(None), list, dict))}
        self.api_url = (api_url or os.getenv("HTTPS_PYMONGO_API_URL", "https://httpspymongo.vercel.app/api")).rstrip("/")
        self._closed = False

    def __getitem__(self, name): return Database(self, str(name))
    def get_database(self, name="test", **kwargs): return self[name]
    def close(self): self._closed = True
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
        req = urllib.request.Request(self.api_url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                envelope = json_util.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            try:
                detail = json_util.loads(exc.read().decode("utf-8"))
                message = detail.get("error", str(exc))
                error_type = detail.get("type")
            except Exception:
                message, error_type = str(exc), None
            if error_type == "ConfigurationError": raise ConfigurationError(message) from exc
            raise OperationFailure(message) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise OperationFailure(f"HTTPS API request failed: {exc}") from exc
        if not envelope.get("ok"):
            raise OperationFailure(envelope.get("error", "API request failed"))
        return _decode(envelope.get("result"))

    def __getattr__(self, method):
        if method.startswith("_"): raise AttributeError(method)
        if method in _CLIENT_METHODS:
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
    def __init__(self, collection, method, args, kwargs):
        self.collection, self.method = collection, method
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
            self._items = self.collection._call(self.method, self.args, self.kwargs, self.options)
        return iter(self._items)
    def __len__(self): return len(list(iter(self)))
    def to_list(self, length=None):
        items = list(iter(self))
        return items if length is None else items[:length]


class _WriteResult:
    def __init__(self, value):
        self.raw_result = value
        for name, item in value.items(): setattr(self, name, item)


class InsertOneResult(_WriteResult): pass
class InsertManyResult(_WriteResult): pass
class UpdateResult(_WriteResult): pass
class DeleteResult(_WriteResult): pass


def _decode(value):
    if isinstance(value, dict) and "__https_pymongo_result__" in value:
        kind = value["__https_pymongo_result__"]
        payload = value.get("value", {})
        cls = {"InsertOneResult": InsertOneResult, "InsertManyResult": InsertManyResult,
               "UpdateResult": UpdateResult, "DeleteResult": DeleteResult}.get(kind)
        if cls: return cls(payload)
    if isinstance(value, list): return [_decode(item) for item in value]
    if isinstance(value, dict): return {key: _decode(item) for key, item in value.items()}
    return value
