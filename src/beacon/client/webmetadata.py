import kaa.beacon
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
        if not metadata or not metadata.name:
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

    @kaa.coroutine()
    def sync(self, force=False):
        yield self.client.rpc('webmetadata.sync', force=force)
        for item in (yield self.client.query(type='video')):
            ItemWrapper(item).sync()
            yield kaa.NotFinished

    @kaa.rpc.expose('webmetadata.signal_sync')
    def _signal_sync(self, msg):
        if not kaa.beacon.is_server():
            kaa.webmetadata.signals['sync'].emit(msg)

    @staticmethod
    def init(client):
        plugin = Plugin(client)
        kaa.webmetadata.sync = plugin.sync
        client.channel.register(plugin)
        return dict(webmetadata=plugin)
