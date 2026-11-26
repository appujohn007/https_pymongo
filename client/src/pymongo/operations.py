"""Write operation definitions accepted by Collection.bulk_write."""


class _WriteOp:
    _kind = ""
    def __init__(self, filter, *args, **kwargs):
        self._filter = filter
        self._args = args
        self._namespace = kwargs.pop("namespace", None)
        self._kwargs = kwargs

    def _https_pymongo_encode(self):
        return {"__https_pymongo_operation__": self._kind, "namespace": self._namespace,
                "filter": self._filter, "args": list(self._args), "kwargs": self._kwargs}


class InsertOne(_WriteOp):
    _kind = "InsertOne"
    def __init__(self, document, namespace=None): self._document = document; self._namespace = namespace
    def _https_pymongo_encode(self):
        return {"__https_pymongo_operation__": self._kind, "namespace": self._namespace, "document": self._document}


class UpdateOne(_WriteOp): _kind = "UpdateOne"
class UpdateMany(_WriteOp): _kind = "UpdateMany"
class ReplaceOne(_WriteOp): _kind = "ReplaceOne"
class DeleteOne(_WriteOp): _kind = "DeleteOne"
class DeleteMany(_WriteOp): _kind = "DeleteMany"

