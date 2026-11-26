"""Vercel serverless MongoDB HTTP bridge. Configure MONGODB_URI and HTTPS_PYMONGO_API_KEY."""
from http.server import BaseHTTPRequestHandler
import hmac
import json
import os
import threading

from bson import json_util
from pymongo import MongoClient

_client = None
_lock = threading.Lock()


def _mongo():
    global _client
    if _client is None:
        with _lock:
            if _client is None:
                uri = os.environ.get("MONGODB_URI")
                if not uri:
                    raise RuntimeError("MONGODB_URI is not configured")
                _client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    return _client


def _run(data):
    action = data["action"]
    db = _mongo()[data["database"]]
    if action == "ping":
        return db.command("ping")
    col = db[data["collection"]]
    args = data.get("args", {})
    if action == "find_one": return col.find_one(args.get("filter", {}), args.get("projection"))
    if action == "find":
        cursor = col.find(args.get("filter", {}), args.get("projection"))
        if args.get("sort"): cursor.sort(args["sort"])
        if args.get("skip"): cursor.skip(args["skip"])
        if args.get("limit"): cursor.limit(args["limit"])
        return list(cursor)
    if action == "count_documents": return col.count_documents(args.get("filter", {}), **args.get("options", {}))
    if action == "insert_one": return {"inserted_id": col.insert_one(args["document"]).inserted_id}
    if action == "insert_many": return {"inserted_ids": col.insert_many(args["documents"], ordered=args.get("ordered", True)).inserted_ids}
    if action == "delete_one":
        r = col.delete_one(args.get("filter", {})); return {"deleted_count": r.deleted_count}
    if action == "delete_many":
        r = col.delete_many(args.get("filter", {})); return {"deleted_count": r.deleted_count}
    if action == "update_one":
        r = col.update_one(args["filter"], args["update"], upsert=args.get("upsert", False))
        return {"matched_count": r.matched_count, "modified_count": r.modified_count, "upserted_id": r.upserted_id}
    if action == "update_many":
        r = col.update_many(args["filter"], args["update"], upsert=args.get("upsert", False))
        return {"matched_count": r.matched_count, "modified_count": r.modified_count, "upserted_id": r.upserted_id}
    if action == "replace_one":
        r = col.replace_one(args["filter"], args["replacement"], upsert=args.get("upsert", False))
        return {"matched_count": r.matched_count, "modified_count": r.modified_count, "upserted_id": r.upserted_id}
    if action == "distinct": return col.distinct(args["key"], args.get("filter", {}))
    raise ValueError("Unsupported action")


class handler(BaseHTTPRequestHandler):
    def _reply(self, status, value):
        body = json_util.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._reply(200, {"service": "https-pymongo", "status": "ok"})

    def do_POST(self):
        expected = os.environ.get("HTTPS_PYMONGO_API_KEY", "")
        supplied = self.headers.get("Authorization", "")
        if not expected or not hmac.compare_digest(supplied, "Bearer " + expected):
            self._reply(401, {"error": "Unauthorized"}); return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > 2_000_000: self._reply(413, {"error": "Request too large"}); return
            data = json_util.loads(self.rfile.read(length))
            result = _run(data)
            self._reply(200, {"ok": True, "result": result})
        except (KeyError, TypeError, ValueError) as exc:
            self._reply(400, {"error": str(exc)})
        except Exception as exc:
            self._reply(502, {"error": str(exc), "type": type(exc).__name__})
