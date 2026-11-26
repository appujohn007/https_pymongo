# API branch

Vercel serverless HTTPS bridge. Each request contains the caller's MongoDB URI and a PyMongo method call. The handler creates an official PyMongo client for that operation, materializes cursor results, serializes values with MongoDB Extended JSON, and closes the connection. It does not require a Vercel Atlas URI or shared API key.

This endpoint is intentionally public and unauthenticated. Anyone can submit a URI, so use HTTPS and narrowly permissioned MongoDB accounts. Do not log request bodies or publish credentials.
