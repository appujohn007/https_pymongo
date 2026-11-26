"""ReadConcern value type used by PyMongo-compatible callers."""


class ReadConcern:
    def __init__(self, level=None): self.level = level
    @property
    def document(self): return {"level": self.level} if self.level is not None else {}

