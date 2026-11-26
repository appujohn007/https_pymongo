# HTTPS PyMongo

`https-pymongo` is a Python client facade that keeps the familiar `pymongo`, `motor.motor_asyncio`, and `bson` import names while sending MongoDB operations to an HTTPS API. The API then connects to MongoDB using the URI supplied to the client.

This is a compatibility subset for projects that need an HTTPS path to MongoDB. It is not the official PyMongo or Motor driver and does not provide complete driver parity.

## Install

Use a virtual environment for your project. This package supplies the `pymongo`, `motor`, and `bson` modules, so remove the official packages from that environment first:

```powershell
python -m pip uninstall -y pymongo motor
python -m pip install --upgrade --force-reinstall git+https://github.com/appujohn007/https_pymongo.git@PACKAGE
```

To update later, run the same install command again. The `PACKAGE` branch is installed directly from GitHub.

## Synchronous example

Pass the MongoDB URI your application already uses. No separate MongoDB URI or shared API key is configured on the package:

```python
from pymongo import MongoClient

uri = "mongodb+srv://user:password@cluster.example/mydb"

with MongoClient(uri) as client:
    collection = client["mydb"]["users"]
    print(collection.count_documents({"active": True}))
    for user in collection.find({"active": True}).sort("_id", 1).limit(20):
        print(user)
```

Common client, database, and collection methods are forwarded to the API, including CRUD, aggregation, index management, and bulk writes supported by the server's installed PyMongo release.

## Async example

The Motor-shaped API runs synchronous HTTPS calls in worker threads and reuses the same HTTP connection pool:

```python
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

async def main():
    client = AsyncIOMotorClient("mongodb+srv://user:password@cluster.example/mydb")
    try:
        users = await client["mydb"]["users"].find({"active": True}).limit(20).to_list(length=20)
        print(users)
    finally:
        client.close()

asyncio.run(main())
```

## Configuration

- The default API URL is `https://httpspymongo.vercel.app/api`.
- Override it with `HTTPS_PYMONGO_API_URL` or `MongoClient(..., api_url="https://your-host/api")`.
- Re-enable HTTPX's per-request log lines with `HTTPS_PYMONGO_HTTPX_LOGS=1`; by default the package filters only its own repetitive request lines and leaves warnings/errors visible.
- Supply the MongoDB URI to `MongoClient` or `AsyncIOMotorClient` as usual. The package sends it to the API over HTTPS for each operation.
- The API does not require an API key and does not use a server-side fixed MongoDB URI.

## Errors and unsupported behavior

MongoDB errors are mapped to PyMongo-shaped exceptions with codes and write details when available. Unsupported methods raise `UnsupportedOperation` with an explanation or likely method names. Read-only calls can be retried once after a dropped response; writes are not automatically retried because the server may have completed a write before the response was lost.

One request cannot preserve a MongoDB session between calls. Sessions and transactions, change streams, tailable cursors, and live streaming cursors therefore require direct PyMongo/Motor. Cursor results are materialized into a response, with a 4 MB request and response limit.

## Security and production use

The API is intentionally public and unauthenticated: anyone who can reach it can submit a MongoDB URI and ask it to perform operations using that account. Use narrowly scoped database users and do not expose privileged credentials. The API does not intentionally log request bodies; its metrics omit URIs, queries, database/collection names, and document contents.

Warm Vercel instances may reuse bounded MongoClient pools, and the package reuses client-to-API HTTPS connections. Vercel may replace or suspend instances, so this reuse is not guaranteed. Every read reaches MongoDB; query results are not cached. Expect additional latency compared with direct PyMongo, and validate your workload before production use.
