import asyncio
import json

from bson import ObjectId, json_util
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import DESCENDING, MongoClient


class FakeResponse:
    def __init__(self, value): self.value = value
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def read(self): return json_util.dumps({"ok": True, "result": self.value}).encode()


def test_extended_json_round_trip_objectid():
    value = {"_id": ObjectId("0123456789abcdef01234567")}
    assert json_util.loads(json_util.dumps(value)) == value


def test_motor_passes_uri_and_query_over_https_without_api_key(monkeypatch):
    import pymongo.mongo_client as http_client
    requests = []
    def fake_urlopen(request, timeout):
        requests.append(request)
        return FakeResponse([{"ok": 1}])
    monkeypatch.setattr(http_client.urllib.request, "urlopen", fake_urlopen)
    async def run():
        client = AsyncIOMotorClient("mongodb+srv://account:secret@cluster.example/")
        cursor = client["app"]["users"].find({"active": True}).sort("_id", DESCENDING).skip(1).limit(2)
        assert await cursor.to_list(length=1) == [{"ok": 1}]
    asyncio.run(run())
    payload = json.loads(requests[0].data)
    assert payload["uri"] == "mongodb+srv://account:secret@cluster.example/"
    assert payload["target"] == "collection"
    assert payload["method"] == "find"
    assert payload["database"] == "app"
    assert payload["collection"] == "users"
    assert payload["args"] == [{"active": True}]
    assert payload["cursor"] == {"sort": [["_id", -1]], "skip": 1, "limit": 2}
    assert "Authorization" not in requests[0].header_items()


def test_sync_generic_methods_preserve_arguments(monkeypatch):
    import pymongo.mongo_client as http_client
    captured = []
    def fake_urlopen(request, timeout):
        captured.append(json.loads(request.data))
        return FakeResponse({"ok": 3})
    monkeypatch.setattr(http_client.urllib.request, "urlopen", fake_urlopen)
    client = MongoClient("mongodb://user:pw@host/db")
    assert client.db.users.find_one({"id": 7}, {"_id": 0}) == {"ok": 3}
    assert captured[0]["uri"] == "mongodb://user:pw@host/db"
    assert captured[0]["method"] == "find_one"
    assert captured[0]["args"] == [{"id": 7}, {"_id": 0}]
