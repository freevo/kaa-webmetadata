import kaa.metadata
import kaa.webmetadata

class ItemWrapper(object):
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
        try:
            metadata = kaa.webmetadata.parse(self.item.filename, self.item)
        except Exception, e:
            return
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

class Plugin(object):

    def __init__(self, client):
        kaa.webmetadata.init(client.get_db_info()['directory'])
        self.client = client

    def parse(filename, metadata=None):
        """
        Parse the given filename and return information from the db. If
        metadata is None it will be created using kaa.metadata. Each
        dictionary-like object is allowed.
        """
        return kaa.webmetadata.parse(filename, metadata)

    def search(self, filename, metadata=None):
        """
        Search the given filename in the web. If metadata is None it will
        be created using kaa.metadata. Each dictionary-like object is
        allowed.
        """
        return kaa.webmetadata.search(filename, metadata)

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
            result = yield kaa.webmetadata.tv.add_series_by_search_result(result, alias=metadata.get('series'))
            if result:
                for item in (yield self.client.query(type='video', series=metadata.get('series'))):
                    ItemWrapper(item).sync()
        else:
            result = yield kaa.webmetadata.movie.match(filename, result.id, metadata)
        yield result

    @kaa.coroutine()
    def sync(self):
        yield self.client.rpc('webmetadata.sync')
        for item in (yield self.client.query(type='video')):
            ItemWrapper(item).sync()
            yield kaa.NotFinished

    @staticmethod
    def init(client):
        plugin = Plugin(client)
        for func in ('match', 'sync'):
            setattr(kaa.webmetadata, func, getattr(plugin, func))
        return dict(webmetadata=plugin)
