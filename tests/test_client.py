import asyncio
import json

from bson import ObjectId, json_util
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import DESCENDING, MongoClient


class FakeResponse:
    def __init__(self, value, status_code=200):
        self.content = json_util.dumps(value).encode()
        self.status_code = status_code
        self.is_error = status_code >= 400
        self.headers = {"content-type": "application/json"}


def test_extended_json_round_trip_objectid():
    value = {"_id": ObjectId("0123456789abcdef01234567")}
    assert json_util.loads(json_util.dumps(value)) == value


def test_extended_json_reads_relaxed_iso_dates_from_official_pymongo():
    value = json_util.loads('{"created":{"$date":"2026-11-26T12:34:56.789Z"}}')
    assert value["created"].isoformat() == "2026-11-26T12:34:56.789000+00:00"


def test_motor_passes_uri_and_query_over_https_without_api_key(monkeypatch):
    import httpx
    requests = []
    def fake_post(self, url, **kwargs):
        requests.append((url, kwargs))
        return FakeResponse({"ok": True, "result": [{"ok": 1}]})
    monkeypatch.setattr(httpx.Client, "post", fake_post)
    async def run():
        client = AsyncIOMotorClient("mongodb+srv://account:secret@cluster.example/")
        cursor = client["app"]["users"].find({"active": True}).sort("_id", DESCENDING).skip(1).limit(2)
        assert await cursor.to_list(length=1) == [{"ok": 1}]
        client.close()
    asyncio.run(run())
    payload = json.loads(requests[0][1]["content"])
    assert payload["uri"] == "mongodb+srv://account:secret@cluster.example/"
    assert payload["target"] == "collection"
    assert payload["method"] == "find"
    assert payload["database"] == "app"
    assert payload["collection"] == "users"
    assert payload["args"] == [{"active": True}]
    assert payload["cursor"] == {"sort": [["_id", -1]], "skip": 1, "limit": 1}
    assert "Authorization" not in requests[0][1]["headers"]


def test_sync_generic_methods_preserve_arguments(monkeypatch):
    import httpx
    captured = []
    def fake_post(self, url, **kwargs):
        captured.append(json.loads(kwargs["content"]))
        return FakeResponse({"ok": True, "result": {"ok": 3}})
    monkeypatch.setattr(httpx.Client, "post", fake_post)
    client = MongoClient("mongodb://user:pw@host/db")
    assert client.db.users.find_one({"id": 7}, {"_id": 0}) == {"ok": 3}
    client.close()
    assert captured[0]["uri"] == "mongodb://user:pw@host/db"
    assert captured[0]["method"] == "find_one"
    assert captured[0]["args"] == [{"id": 7}, {"_id": 0}]


def test_bulk_operation_wire_format():
    from pymongo import InsertOne, UpdateOne
    value = [InsertOne({"name": "Ada"}), UpdateOne({"name": "Ada"}, {"$set": {"active": True}}, upsert=True),
             InsertOne({"name": "Grace"}, namespace="app.users")]
    wire = json_util.loads(json_util.dumps(value))
    assert wire[0]["__https_pymongo_operation__"] == "InsertOne"
    assert wire[1]["__https_pymongo_operation__"] == "UpdateOne"
    assert wire[1]["kwargs"] == {"upsert": True}
    assert wire[2]["namespace"] == "app.users"


def test_client_and_database_cursor_methods_return_cursor_objects(monkeypatch):
    import httpx
    captured = []
    def fake_post(self, url, **kwargs):
        captured.append(json.loads(kwargs["content"]))
        return FakeResponse({"ok": True, "result": [{"name": "app"}]})
    monkeypatch.setattr(httpx.Client, "post", fake_post)
    client = MongoClient("mongodb://host")
    cursor = client.list_databases(nameOnly=True)
    assert list(cursor) == [{"name": "app"}]
    assert captured[0]["method"] == "list_databases"
    assert captured[0]["kwargs"] == {"nameOnly": True}
    client.close()


def test_bulk_write_result_is_decoded_to_result_object(monkeypatch):
    import httpx
    def fake_post(self, url, **kwargs):
        return FakeResponse({"ok": True, "result": {"__https_pymongo_result__": "ClientBulkWriteResult",
            "value": {"inserted_count": 1, "matched_count": 0, "modified_count": 0,
                      "deleted_count": 0, "upserted_count": 0, "bulk_api_result": {"ok": 1}}}})
    monkeypatch.setattr(httpx.Client, "post", fake_post)
    client = MongoClient("mongodb://host")
    result = client.bulk_write([])
    assert result.inserted_count == 1 and result.acknowledged
    client.close()


def test_read_only_call_retries_one_dropped_response(monkeypatch):
    import httpx
    attempts = []
    def fake_post(self, url, **kwargs):
        attempts.append(url)
        if len(attempts) == 1: raise httpx.ReadError("stale keep-alive connection")
        return FakeResponse({"ok": True, "result": {"n": 4}})
    monkeypatch.setattr(httpx.Client, "post", fake_post)
    client = MongoClient("mongodb://host")
    assert client.db.items.count_documents({}) == {"n": 4}
    assert len(attempts) == 2
    client.close()


def test_bridge_errors_map_to_informative_pymongo_errors():
    from pymongo.errors import DuplicateKeyError, UnsupportedOperation
    from pymongo.mongo_client import _remote_error
    duplicate = _remote_error({"error": "duplicate key", "type": "DuplicateKeyError", "code": 11000}, 409)
    unsupported = _remote_error({"error": "Unsupported collection method 'foo'.", "type": "UnsupportedMethodError"}, 400)
    assert isinstance(duplicate, DuplicateKeyError) and duplicate.code == 11000
    assert isinstance(unsupported, UnsupportedOperation) and "foo" in str(unsupported)
