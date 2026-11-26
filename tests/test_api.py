import io

from api import index


class Cursor:
    def __init__(self): self.options=[]
    def sort(self, value): self.options.append(("sort", value)); return self
    def skip(self, value): self.options.append(("skip", value)); return self
    def limit(self, value): self.options.append(("limit", value)); return self
    def __iter__(self): return iter([{"n": 1}])


class Collection:
    def __init__(self): self.cursor=Cursor(); self.query=None
    def find(self, query, projection=None): self.query=(query, projection); return self.cursor


class Database:
    def __init__(self, collection): self.collection=collection
    def __getitem__(self, name): return self.collection


class Client:
    def __init__(self, uri, **options): self.uri=uri; self.closed=False
    def __getitem__(self, name): return db
    def close(self): self.closed=True


def test_uri_and_generic_cursor_call_are_forwarded(monkeypatch):
    global db
    collection=Collection(); db=Database(collection); uris=[]
    def fake_client(uri, **options):
        uris.append(uri)
        return Client(uri, **options)
    monkeypatch.setattr(index, "MongoClient", fake_client)
    result=index._run({"uri":"mongodb+srv://one:two@cluster.example/", "target":"collection",
        "database":"app", "collection":"users", "method":"find",
        "args":[{"active":True}], "kwargs":{"projection":{"name":1}},
        "cursor":{"sort":[["_id",-1]], "skip":2, "limit":4}})
    assert uris == ["mongodb+srv://one:two@cluster.example/"]
    assert collection.query == ({"active":True}, {"name":1})
    assert collection.cursor.options == [("sort", [["_id",-1]]), ("skip",2), ("limit",4)]
    assert result == [{"n":1}]


def test_public_post_needs_no_api_key(monkeypatch):
    monkeypatch.delenv("HTTPS_PYMONGO_API_KEY", raising=False)
    monkeypatch.setattr(index, "_run", lambda request: {"ok": True})
    request=object.__new__(index.handler)
    request.headers={"Content-Length":"2"}
    request.rfile=io.BytesIO(b"{}")
    request.wfile=io.BytesIO()
    statuses=[]
    request.send_response=statuses.append
    request.send_header=lambda *args: None
    request.end_headers=lambda: None
    request.do_POST()
    assert statuses == [200]
