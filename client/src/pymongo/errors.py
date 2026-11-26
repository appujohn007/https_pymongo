"""Error hierarchy compatible with the commonly used PyMongo exceptions."""


class PyMongoError(Exception):
    def __init__(self, message="", *args, **kwargs):
        super().__init__(message, *args)
        self._details = kwargs.get("details")
        self._timeout = kwargs.get("timeout", False)

    @property
    def timeout(self):
        return self._timeout

    def has_error_label(self, label):
        return bool(self._details and label in self._details.get("errorLabels", ()))


class ConfigurationError(PyMongoError): pass
class InvalidOperation(PyMongoError): pass
class UnsupportedOperation(InvalidOperation): pass
class OperationFailure(PyMongoError):
    def __init__(self, message="", code=None, details=None, **kwargs):
        super().__init__(message, details=details, **kwargs)
        self.code = code
        self.details = details
class ConnectionFailure(PyMongoError): pass
class ServerSelectionTimeoutError(ConnectionFailure): pass
class NetworkTimeout(ConnectionFailure): pass
class DuplicateKeyError(OperationFailure): pass
class WriteError(OperationFailure): pass
class WriteConcernError(OperationFailure): pass
class BulkWriteError(OperationFailure):
    def __init__(self, message="", details=None, **kwargs):
        super().__init__(message, details=details, **kwargs)
