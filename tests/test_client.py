import asyncio
import json

from bson import ObjectId, json_util
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import DESCENDING


class FakeResponse:
    def __init__(self, value): self.value = value
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def read(self): return json_util.dumps({"ok": True, "result": self.value}).encode()


def test_extended_json_round_trip_objectid():
    value = {"_id": ObjectId("0123456789abcdef01234567")}
    assert json_util.loads(json_util.dumps(value)) == value


def test_async_motor_calls_preserve_database_collection_and_query(monkeypatch):
    import pymongo.mongo_client as http_client
    calls = []
    def fake_urlopen(request, timeout):
        calls.append(json.loads(request.data))
        return FakeResponse([{"ok": 1}])
    monkeypatch.setenv("HTTPS_PYMONGO_API_KEY", "test-key")
    monkeypatch.setattr(http_client.urllib.request, "urlopen", fake_urlopen)
    async def run():
        client = AsyncIOMotorClient("mongodb://must-not-be-sent")
        cursor = client["app"]["users"].find({"active": True}).sort("_id", DESCENDING).skip(1).limit(2)
        rows = await cursor.to_list(length=1)
        assert rows == [{"ok": 1}]
    asyncio.run(run())
    assert calls[0]["database"] == "app"
    assert calls[0]["collection"] == "users"
    assert calls[0]["args"] == {"filter": {"active": True}, "projection": None, "skip": 1, "limit": 2, "sort": [["_id", -1]]}
    assert "must-not-be-sent" not in json.dumps(calls)
