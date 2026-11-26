import json
import os
import urllib.error
import urllib.request

from bson import json_util
from .errors import ConfigurationError, OperationFailure


class MongoClient:
    """Synchronous HTTP client. The positional Mongo URI is deliberately never transmitted."""
    def __init__(self, uri=None, *, api_url=None, api_key=None, **kwargs):
        self.api_url = (api_url or os.getenv("HTTPS_PYMONGO_API_URL", "https://httpspymongo.vercel.app/api")).rstrip("/")
        self.api_key = api_key or os.getenv("HTTPS_PYMONGO_API_KEY")
        if not self.api_key:
            raise ConfigurationError("Set HTTPS_PYMONGO_API_KEY to the key configured on the HTTPS API")
        self._closed = False

    def __getitem__(self, name): return Database(self, name)
    def get_database(self, name="test", **kwargs): return self[name]
    def close(self): self._closed = True
    def server_info(self): return self._request("ping", "admin", "", {})
    def _request(self, action, database, collection, args):
        body = json_util.dumps({"action": action, "database": database, "collection": collection, "args": args}).encode()
        req = urllib.request.Request(self.api_url, data=body, headers={"Content-Type": "application/json", "Authorization": "Bearer " + self.api_key}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as response: data = json_util.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            try: detail = json_util.loads(exc.read().decode()).get("error", str(exc))
            except Exception: detail = str(exc)
            raise OperationFailure(detail) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise OperationFailure(f"HTTPS API request failed: {exc}") from exc
        if not data.get("ok"): raise OperationFailure(data.get("error", "API request failed"))
        return data["result"]


class Database:
    def __init__(self, client, name): self.client, self.name = client, name
    def __getitem__(self, name): return Collection(self, name)
    def get_collection(self, name, **kwargs): return self[name]
    def __getattr__(self, name):
        if name.startswith("_"): raise AttributeError(name)
        return self[name]


class Collection:
    def __init__(self, database, name): self.database, self.name = database, name
    def _call(self, action, **args): return self.database.client._request(action, self.database.name, self.name, args)
    def find_one(self, filter=None, projection=None, **kwargs): return self._call("find_one", filter=filter or {}, projection=projection)
    def find(self, filter=None, projection=None, **kwargs): return SyncCursor(self, filter or {}, projection)
    def count_documents(self, filter, **kwargs): return self._call("count_documents", filter=filter, options=kwargs)
    def insert_one(self, document, **kwargs): return InsertOneResult(self._call("insert_one", document=document)["inserted_id"])
    def insert_many(self, documents, ordered=True, **kwargs): return InsertManyResult(self._call("insert_many", documents=list(documents), ordered=ordered)["inserted_ids"])
    def delete_one(self, filter, **kwargs): return DeleteResult(self._call("delete_one", filter=filter)["deleted_count"])
    def delete_many(self, filter, **kwargs): return DeleteResult(self._call("delete_many", filter=filter)["deleted_count"])
    def update_one(self, filter, update, upsert=False, **kwargs): return UpdateResult(self._call("update_one", filter=filter, update=update, upsert=upsert))
    def update_many(self, filter, update, upsert=False, **kwargs): return UpdateResult(self._call("update_many", filter=filter, update=update, upsert=upsert))
    def replace_one(self, filter, replacement, upsert=False, **kwargs): return UpdateResult(self._call("replace_one", filter=filter, replacement=replacement, upsert=upsert))
    def distinct(self, key, filter=None, **kwargs): return self._call("distinct", key=key, filter=filter or {})


class SyncCursor:
    def __init__(self, collection, filter, projection): self.collection, self.filter, self.projection = collection, filter, projection; self._skip=0; self._limit=0; self._sort=None
    def skip(self, count): self._skip=count; return self
    def limit(self, count): self._limit=count; return self
    def sort(self, key_or_list, direction=None): self._sort=[[key_or_list, direction]] if isinstance(key_or_list, str) else key_or_list; return self
    def __iter__(self): return iter(self.collection._call("find", filter=self.filter, projection=self.projection, skip=self._skip, limit=self._limit, sort=self._sort))


class InsertOneResult:
    def __init__(self, value): self.inserted_id=value
class InsertManyResult:
    def __init__(self, values): self.inserted_ids=values
class DeleteResult:
    def __init__(self, count): self.deleted_count=count
class UpdateResult:
    def __init__(self, value): self.matched_count=value["matched_count"]; self.modified_count=value["modified_count"]; self.upserted_id=value["upserted_id"]
