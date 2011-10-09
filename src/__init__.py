import os
import kaa
import kaa.metadata

signals = kaa.Signals('changed')

backends = []

WORKER_THREAD = 'WEBMETADATA'

kaa.metadata.enable_feature('VIDEO_SERIES_PARSER')
kaa.register_thread_pool(WORKER_THREAD, kaa.ThreadPool())


import tv
import movie

from tv.core import Series, Season, Episode
from movie.core import Movie


def init(base='~/.beacon'):
    """
    Initialize the kaa.webmetadata databases
    """
    if backends:
        return
    for module in tv, movie:
        module.init(base)
        backends.extend(module.backends.values())
    for backend in backends:
        backend.signals['changed'].connect(signals['changed'].emit)

def db_version():
    """
    Get database version
    """
    ver = 0
    for module in backends:
        ver += module.version
    return ver

def parse(filename, metadata=None):
    """
    Parse the given filename and return information from the db. If
    metadata is None it will be created using kaa.metadata. Each
    dictionary-like object is allowed.
    """
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata.get('series', None):
        return tv.parse(filename, metadata)
    return movie.parse(filename, metadata)

def search(filename, metadata=None):
    """
    Search the given filename in the web. If metadata is None it will
    be created using kaa.metadata. Each dictionary-like object is
    allowed.
    """
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata.get('series', None):
        return tv.search(filename, metadata)
    return movie.search(filename, metadata)

@kaa.coroutine()
def match(filename, id, metadata=None):
    """
    Match the given filename with the id for future parsing. If
    metadata is None it will be created using kaa.metadata. Each
    dictionary-like object is allowed.
    """
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    result = False
    for module in tv, movie:
        result = result or (yield module.match(filename, id, metadata))
    yield result

@kaa.coroutine()
def sync():
    """
    Sync the databases with their web counterparts
    """
    for module in backends:
        yield module.sync()

def set_metadata(key, value):
    """
    Store some metadata in the database
    """
    for module in backends:
        module.set_metadata(key, value)

def get_metadata(key):
    """
    Retrive stored metadata
    """
    for module in backends:
        value = module.get_metadata(key)
        if value is not None:
            return value
