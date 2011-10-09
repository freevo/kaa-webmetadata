from ..core import Entry, Image, Database, WORKER_THREAD

class Movie(Entry):

    _keys = ['title', 'overview', 'year', 'rating', 'runtime', 'posters', 'images' ]
