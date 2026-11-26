# HTTPS PyMongo

The PACKAGE branch installs a distribution named `https-pymongo` while exposing the familiar `pymongo`, `motor.motor_asyncio`, and `bson` imports. It sends operations to the public API at `https://httpspymongo.vercel.app/api`.

## Install in a project

Run this in the project's virtual environment. The first command removes the official PyMongo and Motor packages because this project provides those import names:

```powershell
python -m pip uninstall -y pymongo motor
python -m pip install git+https://github.com/appujohn007/https_pymongo.git@PACKAGE
```

Then existing code can continue to use the URI it already passes to the client:

```python
import pymongo
import motor.motor_asyncio

client = pymongo.MongoClient("mongodb+srv://user:password@cluster.example/")
db = client["my_database"]
```

The URI is sent to the API in each HTTPS request. The API does not use a fixed `MONGODB_URI` setting and does not require a shared API key.

## API design and limits

The API is an unauthenticated bridge by design. It accepts the caller's MongoDB URI, target, method, and arguments, executes the call with the official PyMongo driver, and returns the result using Extended JSON. Use HTTPS and database credentials with only the permissions the app needs. The API does not intentionally persist or log request bodies.

The client forwards callable PyMongo client, database, and collection methods supported by the API's installed driver. This includes common CRUD, aggregation, index management, collection/database commands, collection bulk writes, and cross-collection client bulk writes (MongoDB server support required). The API returns structured error types, MongoDB error codes, and write error details; unsupported methods include close matches or explain when persistent state is required.

Each `MongoClient` owns a reusable HTTPX connection pool to the API, and Motor-style calls share it from a worker thread pool. This reuses client-to-Vercel HTTPS connections; it does not keep a MongoDB cursor or session alive between API requests. Cursor results are materialized into one response and the public bridge has a 4 MB request/response cap. Sessions/transactions, change streams, tailable cursors, and truly streaming cursors therefore require direct PyMongo and are explicitly rejected where applicable. This is a broad compatibility subset, not complete PyMongo/Motor parity. Validate behavior against your own query patterns before production use.
