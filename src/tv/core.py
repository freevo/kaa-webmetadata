from ..core import Entry, Image, Database, WORKER_THREAD

class Series(Entry):

    _keys = ['name', 'overview', 'year', 'imdb' ]

    image = None
    poster = None
    banner = None
    
    def __str__(self):
        return str(self.name)


class Season(Entry):

    _keys = ['number', 'series' ]

    def __str__(self):
        return str(self.number)

    @property
    def image(self):
        return self.series.image

    @property
    def poster(self):
        return self.series.poster

    @property
    def banner(self):
        return self.series.banner


class Episode(Entry):

    _keys = ['series', 'season', 'number', 'name', 'overview', 'image', 'imdb' ]

    @property
    def image(self):
        return self.series.image

    @property
    def poster(self):
        return self.series.poster

    @property
    def banner(self):
        return self.series.poster

