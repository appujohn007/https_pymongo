"""Session option types for import compatibility with libraries that annotate them.

The HTTPS facade cannot keep a MongoDB session alive across requests. These
options are data containers only; ``MongoClient.start_session`` remains an
explicitly unsupported operation.
"""


class TransactionOptions:
    def __init__(self, read_concern=None, write_concern=None, read_preference=None, max_commit_time_ms=None):
        self.read_concern = read_concern
        self.write_concern = write_concern
        self.read_preference = read_preference
        self.max_commit_time_ms = max_commit_time_ms

