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

The client forwards common synchronous PyMongo calls dynamically and provides async wrappers for Motor-style calls. Cursor-returning calls are materialized by the API and sent back as document lists. This keeps typical CRUD, collection, database, and query code familiar, but stateless HTTP cannot reproduce every PyMongo behavior: sessions and multi-call transactions, change streams, tailable cursors, client-side connection pooling, and unbounded/live server cursors are not equivalent. This package is not yet complete PyMongo/Motor parity.
