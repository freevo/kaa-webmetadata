from ..core import Entry, Image, Database, WORKER_THREAD

class Series(Entry):

    _keys = ['title', 'overview', 'year' ]

    posters = []

    def __str__(self):
        return str(self.title)


class Season(Entry):

    _keys = ['number', 'series' ]

    def __str__(self):
        return str(self.number)

    @property
    def posters(self):
        return self.series.posters


class Episode(Entry):

    _keys = ['series', 'season', 'number', 'title', 'overview', 'image' ]

    @property
    def posters(self):
        return self.season.posters

