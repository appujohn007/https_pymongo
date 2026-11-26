# API branch

Vercel HTTPS bridge. Each request contains the caller's MongoDB URI and a PyMongo method call. The handler reuses a bounded, per-instance cache of official PyMongo clients so warm Fluid Compute instances can reuse pooled connections. The cache is keyed by a one-way hash of the URI and connection options, expires idle entries, and closes evicted clients. Cold instances still create new connections. Query results are not cached, so each read reaches MongoDB.

Fluid Compute is enabled in `vercel.json`; the function duration is set to the Hobby maximum. Vercel can still suspend instances, so global caches are performance opportunities rather than durable storage.

Each POST emits one structured `https_pymongo_metrics` log line with request/response byte counts, total time, cache hit/miss/transient status, client lookup time, database-call time, cursor read time, packing/encoding time, and returned document count. It never logs the URI, filters, collection/database names, or document contents.

This endpoint is intentionally public and unauthenticated. Anyone can submit a URI, so use HTTPS and narrowly permissioned MongoDB accounts. Do not log request bodies or publish credentials. Cached clients are limited to eight per warm instance, with small connection pools, to limit connection growth from arbitrary caller URIs.


MSUT KEEP maxDuration to 60 sec MAX
