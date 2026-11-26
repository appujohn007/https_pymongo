"""HTTPS-backed PyMongo-compatible subset. See README for supported operations."""
ASCENDING = 1
DESCENDING = -1
version = "0.1.0"
__version__ = version

from .errors import (PyMongoError, ConfigurationError, OperationFailure, ServerSelectionTimeoutError,
                     ConnectionFailure, NetworkTimeout, DuplicateKeyError, WriteError,
                     WriteConcernError, BulkWriteError, InvalidOperation)
from .mongo_client import MongoClient

__all__ = ["MongoClient", "ASCENDING", "DESCENDING", "PyMongoError"]
