import io

from api import index


class FakeCursor:
    def __init__(self): self.ops=[]
    def sort(self, value): self.ops.append(("sort", value)); return self
    def skip(self, value): self.ops.append(("skip", value)); return self
    def limit(self, value): self.ops.append(("limit", value)); return self
    def __iter__(self): return iter([{"n": 1}])


class FakeCollection:
    def __init__(self): self.cursor=FakeCursor(); self.filter=None
    def find(self, filter, projection): self.filter=filter; return self.cursor


class FakeDatabase:
    def __init__(self, collection): self.collection=collection
    def __getitem__(self, name): return self.collection


class FakeClient:
    def __init__(self, database): self.database=database
    def __getitem__(self, name): return self.database


def test_find_dispatch_applies_query_and_cursor_options(monkeypatch):
    collection=FakeCollection()
    monkeypatch.setattr(index, "_client", FakeClient(FakeDatabase(collection)))
    result=index._run({"action":"find", "database":"app", "collection":"users", "args":{
        "filter":{"active":True}, "projection":{"name":1}, "sort":[["_id",-1]], "skip":3, "limit":5
    }})
    assert result == [{"n":1}]
    assert collection.filter == {"active":True}
    assert collection.cursor.ops == [("sort", [["_id",-1]]), ("skip",3), ("limit",5)]


def test_post_rejects_missing_or_wrong_api_key(monkeypatch):
    monkeypatch.setenv("HTTPS_PYMONGO_API_KEY", "expected-secret")
    request=object.__new__(index.handler)
    request.headers={"Authorization":"Bearer wrong", "Content-Length":"2"}
    request.rfile=io.BytesIO(b"{}")
    request.wfile=io.BytesIO()
    statuses=[]
    request.send_response=statuses.append
    request.send_header=lambda *args: None
    request.end_headers=lambda: None
    request.do_POST()
    assert statuses == [401]
