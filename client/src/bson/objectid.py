import re
import secrets

from .errors import InvalidId


class ObjectId:
    __slots__ = ("_hex",)

    def __init__(self, value=None):
        if value is None:
            value = secrets.token_hex(12)
        if isinstance(value, ObjectId):
            value = value._hex
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{24}", value):
            raise InvalidId(f"'{value}' is not a valid ObjectId")
        self._hex = value.lower()

    @property
    def binary(self): return bytes.fromhex(self._hex)
    @property
    def generation_time(self):
        from datetime import datetime, timezone
        return datetime.fromtimestamp(int(self._hex[:8], 16), timezone.utc)
    def __str__(self): return self._hex
    def __repr__(self): return f"ObjectId('{self._hex}')"
    def __hash__(self): return hash(self._hex)
    def __eq__(self, other): return isinstance(other, ObjectId) and self._hex == other._hex
    def __lt__(self, other): return self._hex < str(other)
