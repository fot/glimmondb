from .glimmondb import get_tdb, read_glimmon, DBDIR, TDBDIR
from .version import __version__

__all__ = [
    "get_tdb",
    "read_glimmon",
    "DBDIR",
    "TDBDIR",
    "create_db",
    "recreate_db",
    "merge_new_glimmon_to_db",
    "compute_fingerprints",
    "__version__",
]


def __getattr__(name):
    if name in {"create_db", "recreate_db", "merge_new_glimmon_to_db", "compute_fingerprints"}:
        from . import update
        return getattr(update, name)
    raise AttributeError(f"module 'glimmondb' has no attribute '{name}'")
