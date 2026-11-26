# HTTPS PyMongo API bridge

This branch contains the public HTTPS service used by the [`https-pymongo` package](https://github.com/appujohn007/https_pymongo/tree/PACKAGE). The service receives a MongoDB operation and the caller's MongoDB URI, executes it with official PyMongo, and returns an Extended JSON response.

## Endpoint

The deployed endpoint is:

```text
https://httpspymongo.vercel.app/api
```

`GET /api` is a health check. `POST /api` accepts a JSON object with these fields:

| Field | Meaning |
| --- | --- |
| `uri` | Caller-provided `mongodb://` or `mongodb+srv://` URI |
| `target` | `client`, `database`, or `collection` |
| `database` | Database name for database/collection targets |
| `collection` | Collection name for collection targets |
| `method` | Public callable method exposed by the installed PyMongo driver |
| `args`, `kwargs` | Positional and named method arguments |
| `cursor` | Supported cursor options such as `sort`, `skip`, `limit`, and `batch_size` |
| `client_options` | Supported MongoClient options |

The package handles request encoding and response decoding. This endpoint is primarily intended for that client rather than as a general-purpose public REST API.

## Operations and errors

The bridge forwards public callable methods on PyMongo client, database, and collection objects. Common supported work includes CRUD, database/collection management, aggregation, index operations, and collection or client bulk writes. Bulk-operation markers from the package are reconstructed as official PyMongo operation objects on the server.

Unsupported method names return an informative client error, including close matches where possible. Sessions, transactions, change streams, and driver lifecycle/iteration methods are rejected because the bridge cannot keep their state across separate HTTP requests. MongoDB errors include their exception type, numeric code when available, and write-error details. Duplicate-key errors return HTTP 409; connectivity failures return HTTP 503.

Requests and responses are limited to 4 MB. Cursor results are fully materialized into one response, so callers should use filters and limits. The API does not stream a live MongoDB cursor to the client.

## Connection reuse and performance

Each warm function instance keeps a bounded cache of MongoClient objects keyed by a hash of URI and supported connection options. The cache holds up to eight clients, expires idle clients, and caps each MongoClient pool. Reuse can reduce Atlas connection setup on warm instances; Vercel can replace or suspend instances, so it is not durable. Reads are not cached and always reach MongoDB.

The package reuses its own HTTPX connection pool to the API. The extra HTTPS/Vercel hop adds latency compared with a direct MongoDB connection. This design is useful where HTTPS connectivity is required, not as a way to make remote MongoDB calls faster than direct PyMongo.

## Metrics and privacy

Each POST emits a structured `https_pymongo_metrics` log entry with request/response byte counts, method and target, cache status, client lookup time, MongoDB call time, cursor read/packing/encoding times, document count, status, and error type. It does not log the URI, database or collection names, query filters, document contents, or exception text.

## Deploy

The API branch is configured for Vercel. `vercel.json` enables Fluid Compute and sets `maxDuration` to **60 seconds**. `requirements.txt` pins the service to PyMongo 4.x. Push API changes to the configured branch and check the Vercel deployment before relying on them.

The endpoint is intentionally public and has no API key or caller authentication. Any caller can submit a URI and request operations using the permissions of that MongoDB account. Use HTTPS, narrowly scoped database credentials, and monitor Vercel usage. This open bridge is not suitable for privileged credentials or an unrestricted production deployment without an access-control layer.

## Local checks

Run the unit suite from this branch:

```sh
python -m pip install -r requirements.txt pytest
python -m pytest -q
```
