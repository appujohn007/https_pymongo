# https-pymongo

This package provides the import names `pymongo`, `motor.motor_asyncio`, and `bson` for an HTTPS-backed subset of PyMongo/Motor. The initial compatibility target is the supplied `Database` class: async collection selection, `find_one`, `find`, cursor paging/sorting/iteration, counts, inserts, updates, deletes, and ObjectId.

It is not full PyMongo parity. It does not yet support sessions/transactions, change streams, GridFS, aggregation, bulk writes, connection pooling, or every option/error class. Each request goes to one configured Atlas account; keep the API key private.

## Configure the server

On the Vercel project connected to the **API** branch, set these server environment variables and redeploy:

- `MONGODB_URI`: Atlas connection string, held only by the server.
- `HTTPS_PYMONGO_API_KEY`: a long random shared secret.

The API branch contains only the Vercel handler, so its `pymongo` import resolves to the real PyMongo dependency. The client package is under `client/src` on PACKAGE and does not shadow the server dependency.

## Configure the app

Install this repository's PACKAGE branch in the app environment, then set:

```powershell
$env:HTTPS_PYMONGO_API_KEY = "the same secret configured in Vercel"
$env:HTTPS_PYMONGO_API_URL = "https://httpspymongo.vercel.app/api" # optional
```

The app can keep its imports and `AsyncIOMotorClient(uri)` calls unchanged. The passed Mongo URI is deliberately never transmitted; the server-side `MONGODB_URI` chooses the Atlas account. The supplied URI has no database path, and its embedded database credentials must be moved into Vercel's secret settings before deployment.

Supported API operations: `find_one`, `find`, `count_documents`, `insert_one`, `insert_many`, `delete_one`, `delete_many`, `update_one`, `update_many`, `replace_one`, and `distinct`. Cursors support `skip`, `limit`, `sort`, `to_list`, and `async for`.
