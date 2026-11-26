import io

import pytest

from api import index


class Cursor:
    def __init__(self, rows=None): self.options=[]; self.rows=rows or [{"n":1}]
    def sort(self, value): self.options.append(("sort",value)); return self
    def skip(self, value): self.options.append(("skip",value)); return self
    def limit(self, value): self.options.append(("limit",value)); return self
    def __iter__(self): return iter(self.rows)


class Collection:
    def __init__(self, cursor=None): self.cursor=cursor or Cursor(); self.query=None
    def find(self, query, projection=None): self.query=(query,projection); return self.cursor


class Database:
    def __init__(self, collection): self.collection=collection
    def __getitem__(self, name): return self.collection


class Client:
    def __init__(self, uri, **options): self.uri=uri; self.closed=False
    def __getitem__(self, name): return db
    def close(self): self.closed=True
    def list_database_names(self): return ["app"]


def test_uri_and_generic_cursor_call_are_forwarded(monkeypatch):
    index._clear_client_cache()
    global db
    collection=Collection(); db=Database(collection); uris=[]
    def fake_client(uri, **options):
        uris.append(uri)
        return Client(uri, **options)
    monkeypatch.setattr(index, "MongoClient", fake_client)
    result=index._run({"uri":"mongodb+srv://user:pass@cluster.example/", "target":"collection",
        "database":"app", "collection":"users", "method":"find",
        "args":[{"active":True}], "kwargs":{"projection":{"name":1}},
        "cursor":{"sort":[["_id",-1]], "skip":2, "limit":4}})
    assert uris == ["mongodb+srv://user:pass@cluster.example/"]
    assert collection.query == ({"active":True},{"name":1})
    assert collection.cursor.options == [("sort",[["_id",-1]]),("skip",2),("limit",4)]
    assert result == [{"n":1}]
    index._clear_client_cache()


def test_client_pool_is_reused_and_lru_is_bounded(monkeypatch):
    index._clear_client_cache()
    global db
    db=Database(Collection()); created=[]
    monkeypatch.setattr(index, "_MAX_CACHED_CLIENTS", 2)
    monkeypatch.setattr(index, "_CLIENT_IDLE_TTL_SECONDS", 1000)
    def fake_client(uri, **options):
        value=Client(uri, **options); created.append(value); return value
    monkeypatch.setattr(index, "MongoClient", fake_client)
    request={"target":"client", "method":"list_database_names", "args":[], "kwargs":{}}
    for _ in range(3): index._run({**request,"uri":"mongodb://one.example"})
    assert len(created) == 1
    assert not created[0].closed
    index._run({**request,"uri":"mongodb://two.example"})
    index._run({**request,"uri":"mongodb://three.example"})
    assert len(created) == 3
    assert created[0].closed
    assert len(index._CLIENT_CACHE) == 2
    index._clear_client_cache()


def test_pool_options_are_capped(monkeypatch):
    index._clear_client_cache()
    created=[]
    def fake_client(uri, **options):
        value=Client(uri, **options); created.append((value,options)); return value
    monkeypatch.setattr(index, "MongoClient", fake_client)
    with index._client_lease("mongodb://pool.example",{"maxPoolSize":100,"minPoolSize":50}) as lease:
        client, cache_status = lease
        assert cache_status == "miss"
        assert created[0][0] is client
        assert created[0][1]["maxPoolSize"] == 10
        assert created[0][1]["minPoolSize"] == 10
        assert created[0][1]["maxIdleTimeMS"] == 120_000
    index._clear_client_cache()


def test_cache_stays_bounded_when_all_entries_are_active(monkeypatch):
    index._clear_client_cache()
    monkeypatch.setattr(index,"_MAX_CACHED_CLIENTS",2)
    created=[]
    def fake_client(uri, **options):
        value=Client(uri, **options); created.append(value); return value
    monkeypatch.setattr(index,"MongoClient",fake_client)
    with index._client_lease("mongodb://one.example",{}) as one:
        with index._client_lease("mongodb://two.example",{}) as two:
            with index._client_lease("mongodb://three.example",{}) as three:
                transient, cache_status = three
                assert cache_status == "transient"
                assert len(index._CLIENT_CACHE) == 2
                assert len(created) == 3
                assert not transient.closed
            assert transient.closed
    assert len(index._CLIENT_CACHE) == 2
    index._clear_client_cache()


def test_cursor_stops_before_building_oversized_response(monkeypatch):
    index._clear_client_cache()
    monkeypatch.setattr(index,"_MAX_RESPONSE_BYTES",200)
    global db
    collection=Collection(Cursor(rows=[{"body":"x"*100}]))
    db=Database(collection)
    monkeypatch.setattr(index,"MongoClient",lambda uri, **options: Client(uri,**options))
    with pytest.raises(index.ResponseTooLarge):
        index._run({"uri":"mongodb://large.example","target":"collection","database":"app",
            "collection":"data","method":"find","args":[{}],"kwargs":{},"cursor":{}})
    index._clear_client_cache()


def test_public_post_needs_no_api_key(monkeypatch):
    monkeypatch.delenv("HTTPS_PYMONGO_API_KEY",raising=False)
    monkeypatch.setattr(index,"_run",lambda request, metrics: {"ok":True})
    request=object.__new__(index.handler)
    request.headers={"Content-Length":"2"}; request.rfile=io.BytesIO(b"{}")
    request.wfile=io.BytesIO(); statuses=[]
    request.send_response=statuses.append; request.send_header=lambda *args:None; request.end_headers=lambda:None
    request.do_POST()
    assert statuses == [200]


def test_metrics_are_structured_and_do_not_contain_uri(monkeypatch, capsys):
    def fake_run(data, metrics):
        metrics["client_cache"] = "hit"
        metrics["db_call_ms"] = 1.2
        metrics["documents"] = 3
        return [{"ok":True}]
    monkeypatch.setattr(index, "_run", fake_run)
    request=object.__new__(index.handler)
    request.headers={"Content-Length":"2"}; request.rfile=io.BytesIO(b"{}")
    request.wfile=io.BytesIO(); request.send_response=lambda *args:None
    request.send_header=lambda *args:None; request.end_headers=lambda:None
    request.do_POST()
    output=capsys.readouterr().out
    assert '"client_cache":"hit"' in output
    assert '"db_call_ms":1.2' in output
    assert '"documents":3' in output
    assert "mongodb://" not in output


def test_oversized_response_returns_bounded_error(monkeypatch):
    monkeypatch.setattr(index,"_MAX_RESPONSE_BYTES",200)
    request=object.__new__(index.handler); request.wfile=io.BytesIO(); statuses=[]
    request.send_response=statuses.append; request.send_header=lambda *args:None; request.end_headers=lambda:None
    request._reply(200,{"large":"x"*300})
    assert statuses == [413]
    assert len(request.wfile.getvalue()) < 200
