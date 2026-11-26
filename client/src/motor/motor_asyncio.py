"""Async Motor-shaped wrappers over the HTTPS PyMongo client."""
import asyncio

from pymongo.mongo_client import (MongoClient, Database as SyncDatabase,
                                  Collection as SyncCollection, SyncCursor,
                                  _CLIENT_CURSOR_METHODS, _CURSOR_METHODS,
                                  _DATABASE_CURSOR_METHODS)


def _wrap(value):
    if isinstance(value, SyncDatabase): return AsyncIOMotorDatabase(value)
    if isinstance(value, SyncCollection): return AsyncIOMotorCollection(value)
    if isinstance(value, SyncCursor): return AsyncIOMotorCursor(value)
    return value


class AsyncIOMotorClient:
    def __init__(self, host=None, port=None, **kwargs):
        if host is None and "uri" in kwargs:
            host = kwargs.pop("uri")
        self._sync = MongoClient(host, port, **kwargs)
    def __getitem__(self, name): return AsyncIOMotorDatabase(self._sync[name])
    def get_database(self, name=None, **kwargs):
        return AsyncIOMotorDatabase(self._sync.get_database(name, **kwargs))
    def get_default_database(self, default=None, **kwargs):
        return AsyncIOMotorDatabase(self._sync.get_default_database(default, **kwargs))
    def close(self): self._sync.close()
    def __enter__(self): return self
    def __exit__(self, *exc): self.close()
    def __getattr__(self, method):
        if method.startswith("_"): raise AttributeError(method)
        value = getattr(self._sync, method)
        if isinstance(value, SyncDatabase): return AsyncIOMotorDatabase(value)
        if not callable(value): return _wrap(value)
        if method in _CLIENT_CURSOR_METHODS:
            def cursor(*args, **kwargs): return _wrap(value(*args, **kwargs))
            return cursor
        async def call(*args, **kwargs): return _wrap(await asyncio.to_thread(value, *args, **kwargs))
        return call


class AsyncIOMotorDatabase:
    def __init__(self, sync): self._sync = sync
    def __getitem__(self, name): return AsyncIOMotorCollection(self._sync[name])
    def get_collection(self, name, **kwargs): return self[name]
    def __getattr__(self, method):
        if method.startswith("_"): raise AttributeError(method)
        value = getattr(self._sync, method)
        if isinstance(value, SyncCollection): return AsyncIOMotorCollection(value)
        if not callable(value): return _wrap(value)
        if method in _DATABASE_CURSOR_METHODS:
            def cursor(*args, **kwargs): return _wrap(value(*args, **kwargs))
            return cursor
        async def call(*args, **kwargs): return _wrap(await asyncio.to_thread(value, *args, **kwargs))
        return call


class AsyncIOMotorCollection:
    def __init__(self, sync): self._sync = sync
    def find(self, *args, **kwargs): return AsyncIOMotorCursor(self._sync.find(*args, **kwargs))
    def aggregate(self, *args, **kwargs): return AsyncIOMotorCursor(self._sync.aggregate(*args, **kwargs))
    def list_indexes(self, *args, **kwargs): return AsyncIOMotorCursor(self._sync.list_indexes(*args, **kwargs))
    def __getattr__(self, method):
        if method.startswith("_"): raise AttributeError(method)
        fn = getattr(self._sync, method)
        if not callable(fn): return _wrap(fn)
        if method in _CURSOR_METHODS:
            def cursor(*args, **kwargs): return _wrap(fn(*args, **kwargs))
            return cursor
        async def call(*args, **kwargs): return _wrap(await asyncio.to_thread(fn, *args, **kwargs))
        return call


class AsyncIOMotorCursor:
    def __init__(self, cursor): self._cursor = cursor; self._items = None; self._position = 0
    def _chain(self, method, *args, **kwargs): getattr(self._cursor, method)(*args, **kwargs); self._items = None; return self
    def skip(self, count): return self._chain("skip", count)
    def limit(self, count): return self._chain("limit", count)
    def sort(self, key_or_list, direction=None): return self._chain("sort", key_or_list, direction)
    def batch_size(self, size): return self._chain("batch_size", size)
    def hint(self, hint): return self._chain("hint", hint)
    def comment(self, comment): return self._chain("comment", comment)
    def max_time_ms(self, value): return self._chain("max_time_ms", value)
    def collation(self, value): return self._chain("collation", value)
    def clone(self): return AsyncIOMotorCursor(self._cursor.clone())
    def rewind(self): self._cursor.rewind(); self._items = None; self._position = 0; return self
    async def to_list(self, length=None):
        if length is not None and (not isinstance(length, int) or length < 0):
            raise ValueError("length must be a non-negative integer or None")
        if self._items is None:
            cursor = self._cursor
            if length is not None:
                cursor = cursor.clone()
                existing = cursor.options.get("limit")
                cursor.limit(min(length, existing) if existing else length)
            self._items = await asyncio.to_thread(cursor.to_list)
        return self._items if length is None else self._items[:length]
    def __aiter__(self): return self
    async def __anext__(self):
        items = await self.to_list()
        if self._position >= len(items): raise StopAsyncIteration
        value = items[self._position]; self._position += 1
        return value
