import os
import kaa
import kaa.metadata

backends = {}

def init(base='~/.beacon'):
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
        info = db.parse(filename, metadata)
        if info:
            return info

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
def match(filename, id, metadata=None):
    """
    Match the given filename with the id for future parsing. If
    metadata is None it will be created using kaa.metadata. Each
    dictionary-like object is allowed.
    """
    parser, id = id.split(':')
    if not parser in backends:
        yield None
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    metadata.filesize = os.path.getsize(filename)
    yield (yield backends[parser].match(metadata, int(id)))

def series(name):
    for backend in backends.values():
        series = backend.parse(name)
        if series:
            return series
