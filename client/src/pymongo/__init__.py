"""HTTPS-backed PyMongo-compatible subset. See README for supported operations."""
ASCENDING = 1
DESCENDING = -1
version = "0.2.0"
__version__ = version

from .errors import (PyMongoError, ConfigurationError, OperationFailure, ServerSelectionTimeoutError,
                     ConnectionFailure, NetworkTimeout, DuplicateKeyError, WriteError,
                     WriteConcernError, BulkWriteError, InvalidOperation, UnsupportedOperation)
from .mongo_client import MongoClient, InsertOneResult, InsertManyResult, UpdateResult, DeleteResult, BulkWriteResult, ClientBulkWriteResult
from .operations import InsertOne, UpdateOne, UpdateMany, ReplaceOne, DeleteOne, DeleteMany

__all__ = ["MongoClient", "ASCENDING", "DESCENDING", "PyMongoError", "InsertOne", "UpdateOne",
           "UpdateMany", "ReplaceOne", "DeleteOne", "DeleteMany", "BulkWriteResult"]
