import os
import kaa
import kaa.metadata

import core

backends = {}

def init(base):
    """
    Initialize the kaa.webmetadata databases
    """
    if backends:
        return
    import thetvdb as backend
    backends['thetvdb'] = backend.TVDB(os.path.expanduser(base + '/thetvdb'))

def parse(filename, metadata=None):
    """
    Parse the given filename and return information from the db. If
    metadata is None it will be created using kaa.metadata. Each
    dictionary-like object is allowed.
    """
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if not metadata.get('series', None):
        return None
    for db in backends.values():
        result = db.get_entry_from_metadata(metadata)
        if result and isinstance(result, core.Episode):
            return result

def search(filename, metadata=None, backend='thetvdb'):
    """
    Search the given filename in the web. If metadata is None it will
    be created using kaa.metadata. Each dictionary-like object is
    allowed.
    """
    if not backend in backends:
        return None
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata.get('series', None):
        return backends[backend].search(metadata.get('series'), filename, metadata)
    return None

@kaa.coroutine()
def add_series_by_search_result(result, alias=None):
    """
    Adds a new series given a SearchResult to the database.
    """
    module = backends.get(result.id.split(':')[0], None)
    if not module:
        raise ValueError('Search result is not valid')
    yield (yield module.add_series_by_search_result(result, alias))

def series(name):
    for db in backends.values():
        series = db.get_entry_from_metadata(dict(series=name))
        if series:
            return series
