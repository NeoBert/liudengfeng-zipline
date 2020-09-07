# These imports are necessary to force module-scope register calls to happen.
from . import quandl  # noqa
from . import csvdir  # noqa
from . import cnquandl  # noqa

from .core import (
    UnknownBundle,
    bundles,
    clean,
    from_bundle_ingest_dirname,
    ingest,
    ingestions_for_bundle,
    load,
    register,
    to_bundle_ingest_dirname,
    unregister,
    most_recent_data,
)


__all__ = [
    'UnknownBundle',
    'bundles',
    'clean',
    'from_bundle_ingest_dirname',
    'ingest',
    'ingestions_for_bundle',
    'load',
    'register',
    'to_bundle_ingest_dirname',
    'unregister',
    'most_recent_data'
]
