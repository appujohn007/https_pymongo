"""Read preference marker types for import compatibility.

Read preference configuration is not yet applied by the stateless HTTP facade.
"""


class _ServerMode:
    mode = None
    def __init__(self, tag_sets=None, max_staleness=-1, hedge=None):
        self.tag_sets = tag_sets
        self.max_staleness = max_staleness
        self.hedge = hedge


class Primary(_ServerMode):
    mode = "primary"
    def __init__(self): super().__init__()


class PrimaryPreferred(_ServerMode): mode = "primaryPreferred"
class Secondary(_ServerMode): mode = "secondary"
class SecondaryPreferred(_ServerMode): mode = "secondaryPreferred"
class Nearest(_ServerMode): mode = "nearest"

