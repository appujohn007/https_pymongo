"""WriteConcern value type for common PyMongo import compatibility."""


class WriteConcern:
    def __init__(self, w=None, wtimeout=None, j=None, fsync=None):
        self.w, self.wtimeout, self.j, self.fsync = w, wtimeout, j, fsync

    @property
    def document(self):
        values = (("w", self.w), ("wtimeout", self.wtimeout), ("j", self.j), ("fsync", self.fsync))
        return {key: value for key, value in values if value is not None}

