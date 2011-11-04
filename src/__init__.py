import os
import kaa
import kaa.metadata
import kaa.beacon

signals = kaa.Signals('changed')

backends = []

WORKER_THREAD = 'WEBMETADATA'

kaa.metadata.enable_feature('VIDEO_SERIES_PARSER')
kaa.register_thread_pool(WORKER_THREAD, kaa.ThreadPool())

import tv
import movie

from tv.core import Series, Season, Episode
from movie.core import Movie

class BeaconItemWrapper(object):
    def __init__(self, item):
        self.item = item

    def __setitem__(self, attr, value):
        if isinstance(value, (str, unicode)) and value.startswith('http:'):
            if not self.item[attr]:
                self.item[attr] = value
        elif self.item[attr] != value:
            self.item[attr] = value

    def sync(self):
        if not self.item.filename:
            return
        metadata = parse(self.item.filename, self.item)
        if not metadata:
            return
        self['title'] = metadata.name
        self['description'] = metadata.overview
        if isinstance(metadata, kaa.webmetadata.Episode):
            self['series'] = metadata.series.name
            self['image'] = metadata.image
            self['poster'] = metadata.posters[0].url
        if isinstance(metadata, kaa.webmetadata.Movie):
            self['movie'] = True
            if metadata.posters:
                self['poster'] = metadata.posters[0].url
        else:
            self['movie'] = False
        
@kaa.coroutine()
def init():
    """
    Initialize the kaa.webmetadata databases
    """
    if backends:
        yield None
    base = (yield kaa.beacon.get_db_info())['directory']
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

@kaa.coroutine()
def search(filename, metadata=None):
    """
    Search the given filename in the web. If metadata is None it will
    be created using kaa.metadata. Each dictionary-like object is
    allowed.
    """
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata.get('series', None):
        yield (yield tv.search(filename, metadata))
    if metadata['length'] and metadata['length'] > 60 * 60: # at least one hour
        yield (yield movie.search(filename, metadata))
    yield {}

@kaa.coroutine()
def match(filename, result, metadata=None):
    """
    Match the given filename with the id for future parsing. If
    metadata is None it will be created using kaa.metadata. Each
    dictionary-like object is allowed.
    """
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    if metadata.get('series'):
        result = yield tv.add_series_by_search_result(result, alias=metadata.get('series'))
        if result:
            for item in (yield kaa.beacon.query(type='video', series=metadata.get('series'))):
                BeaconItemWrapper(item).sync()
    else:
        result = yield movie.match(filename, result.id, metadata)
    yield result

@kaa.coroutine()
def sync():
    """
    Sync the databases with their web counterparts
    """
    for module in backends:
        yield module.sync()
    for item in (yield kaa.beacon.query(type='video')):
        BeaconItemWrapper(item).sync()
