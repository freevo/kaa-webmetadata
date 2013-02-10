import os
import kaa
import kaa.metadata

backends = {}

def init(base):
    """
    Initialize the kaa.webmetadata databases
    """
    if backends:
        return
    import themoviedb as backend
    backends['themoviedb'] = backend.MovieDB(os.path.expanduser(base + '/themoviedb'))

def parse(filename, metadata=None):
    """
    Parse the given filename and return information from the db. If
    metadata is None it will be created using kaa.metadata. Each
    dictionary-like object is allowed.
    """
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    for db in backends.values():
        info = db.parse(filename, metadata)
        if info:
            return info

@kaa.coroutine()
def search(filename, metadata=None, backend='themoviedb'):
    """
    Search the given filename in the web. If metadata is None it will
    be created using kaa.metadata. Each dictionary-like object is
    allowed.
    """
    if not backend in backends:
        yield []
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata['length'] and metadata['length'] > 60 * 60: # at least one hour
        yield (yield backends[backend].search(filename, metadata))
    yield []

@kaa.coroutine()
def add_movie_by_id(filename, id, metadata=None):
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
    yield (yield backends[parser].add_movie_by_id(filename, metadata, int(id)))
