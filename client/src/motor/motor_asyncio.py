"""Async Motor-shaped interface backed by the HTTPS PyMongo client."""
import asyncio

from pymongo.mongo_client import MongoClient


class AsyncIOMotorClient:
    def __init__(self, uri=None, **kwargs): self._sync = MongoClient(uri, **kwargs)
    def __getitem__(self, name): return AsyncIOMotorDatabase(self._sync[name])
    def get_database(self, name="test", **kwargs): return self[name]
    def close(self): self._sync.close()


class AsyncIOMotorDatabase:
    def __init__(self, sync): self._sync=sync
    def __getitem__(self, name): return AsyncIOMotorCollection(self._sync[name])
    def get_collection(self, name, **kwargs): return self[name]
    def __getattr__(self, name):
        if name.startswith("_"): raise AttributeError(name)
        return self[name]


class AsyncIOMotorCollection:
    def __init__(self, sync): self._sync=sync
    async def _call(self, fn, *args, **kwargs): return await asyncio.to_thread(fn, *args, **kwargs)
    async def find_one(self, filter=None, projection=None, **kwargs): return await self._call(self._sync.find_one, filter, projection, **kwargs)
    def find(self, filter=None, projection=None, **kwargs): return AsyncIOMotorCursor(self._sync.find(filter, projection, **kwargs))
    async def count_documents(self, filter, **kwargs): return await self._call(self._sync.count_documents, filter, **kwargs)
    async def insert_one(self, document, **kwargs): return await self._call(self._sync.insert_one, document, **kwargs)
    async def insert_many(self, documents, **kwargs): return await self._call(self._sync.insert_many, documents, **kwargs)
    async def delete_one(self, filter, **kwargs): return await self._call(self._sync.delete_one, filter, **kwargs)
    async def delete_many(self, filter, **kwargs): return await self._call(self._sync.delete_many, filter, **kwargs)
    async def update_one(self, filter, update, **kwargs): return await self._call(self._sync.update_one, filter, update, **kwargs)
    async def update_many(self, filter, update, **kwargs): return await self._call(self._sync.update_many, filter, update, **kwargs)
    async def replace_one(self, filter, replacement, **kwargs): return await self._call(self._sync.replace_one, filter, replacement, **kwargs)
    async def distinct(self, key, filter=None, **kwargs): return await self._call(self._sync.distinct, key, filter, **kwargs)


class AsyncIOMotorCursor:
    def __init__(self, cursor): self._cursor=cursor; self._values=None; self._position=0
    def skip(self, count): self._cursor.skip(count); return self
    def limit(self, count): self._cursor.limit(count); return self
    def sort(self, key_or_list, direction=None): self._cursor.sort(key_or_list, direction); return self
    async def to_list(self, length=None):
        values = await asyncio.to_thread(list, self._cursor)
        return values if length is None else values[:length]
    def __aiter__(self): return self
    async def __anext__(self):
        if self._values is None: self._values = await self.to_list()
        if self._position >= len(self._values): raise StopAsyncIteration
        value = self._values[self._position]; self._position += 1
        return value
