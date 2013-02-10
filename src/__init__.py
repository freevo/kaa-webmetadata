import os
import kaa
import kaa.metadata
import kaa.beacon

initialized = False

WORKER_THREAD = 'WEBMETADATA'

kaa.metadata.enable_feature('VIDEO_SERIES_PARSER')
kaa.register_thread_pool(WORKER_THREAD, kaa.ThreadPool())

import tv
import movie

from tv.core import Series, Season, Episode
from movie.core import Movie

signals = kaa.Signals('sync', 'changed')

def init(base):
    """
    Initialize the kaa.webmetadata databases
    """
    global initialized
    if initialized:
        return
    for module in tv, movie:
        module.init(base)
    initialized = True
    for module in tv.backends.values() + movie.backends.values():
        module.signals['changed'].connect(signals['changed'].emit)

def parse(filename, metadata=None):
    """
    Parse the given filename and return information from the db. If
    metadata is None it will be created using kaa.metadata. Each
    dictionary-like object is allowed.
    """
    if not initialized:
        raise RuntimeError('kaa.webmetadata not initialized')
    if not os.path.isfile(filename):
        return None
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata.get('series', None):
        return tv.parse(filename, metadata)
    return movie.parse(filename, metadata)

@kaa.coroutine()
def search(filename, metadata=None):
    """
    Search the given filename in the web. If metadata is None it will
    be created using kaa.metadata. Each dictionary-like object is
    allowed.
    """
    if not initialized:
        raise RuntimeError('kaa.webmetadata not initialized')
    if not os.path.isfile(filename):
        yield {}
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata.get('series', None):
        yield (yield tv.search(filename, metadata))
    if metadata['length'] and metadata['length'] > 60 * 60: # at least one hour
        yield (yield movie.search(filename, metadata))
    yield {}

@kaa.coroutine()
def sync(force=False):
    """
    Sync the databases with their web counterparts
    """
    for module in tv.backends.values() + movie.backends.values():
        yield module.sync(force)
