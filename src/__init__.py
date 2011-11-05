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

def parse(filename, metadata=None):
    """
    Parse the given filename and return information from the db. If
    metadata is None it will be created using kaa.metadata. Each
    dictionary-like object is allowed.
    """
    if not initialized:
        raise RuntimeError('kaa.webmetadata not initialized')
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
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata.get('series', None):
        yield (yield tv.search(filename, metadata))
    if metadata['length'] and metadata['length'] > 60 * 60: # at least one hour
        yield (yield movie.search(filename, metadata))
    yield {}

@kaa.coroutine()
def match(self, filename, result, metadata=None):
    """
    Match the given filename with the id for future parsing. If
    metadata is None it will be created using kaa.metadata. Each
    dictionary-like object is allowed.
    """
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata.get('series'):
        yield (yield kaa.webmetadata.tv.add_series_by_search_result(result, alias=metadata.get('series')))
    yield (yield kaa.webmetadata.movie.match(filename, result.id, metadata))

@kaa.coroutine()
def sync():
    """
    Sync the databases with their web counterparts
    """
    for module in tv.backends.values() + movie.backends.values():
        yield module.sync()
