from ..core import Entry, Image, Database, WORKER_THREAD

class Movie(Entry):

    _keys = ['name', 'overview', 'year', 'rating', 'runtime', 'posters', 'images' ]
