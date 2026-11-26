class PyMongoError(Exception): pass
class ConfigurationError(PyMongoError): pass
class OperationFailure(PyMongoError): pass
class ServerSelectionTimeoutError(PyMongoError): pass
