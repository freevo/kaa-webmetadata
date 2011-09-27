import os
import stat
import sys
import kaa.webmetadata
import kaa.webmetadata.thetvdb
import kaa.beacon

from xmlrpclib import ServerProxy, Error

server = ServerProxy("http://api.opensubtitles.org/xml-rpc")
session = server.LogIn("","","en","OS Test User Agent")
token = session["token"]

@kaa.coroutine()
def main():
    kaa.webmetadata.init()
    print 'check for missing tvdb mapping'
    print '------------------------------'
    tvdb = kaa.webmetadata.backends['thetvdb']
    missing = []
    for alias in (yield kaa.beacon.query(type='video', attr='series')):
        if alias in [ x['tvdb'] for x in tvdb._db.query(type='alias') ]:
            continue
        print 'Missing mapping for "%s"' % alias
        print 'Files:'
        imdb = None
        for item in (yield kaa.beacon.query(type='video', series=alias)):
            print ' ', item.filename
            if not imdb:
                searchlist = [{'moviehash': item.get('hash'), 'moviebytesize': os.stat(item.filename)[stat.ST_SIZE]}]
                try:
                    moviesList = server.SearchSubtitles(token, searchlist)
                except:
                    moviesList = None
                if moviesList and moviesList.get('data'):
                    guesses = { None: 0 }
                    for x in moviesList.get('data'):
                        if not x.get('IDMovieImdb', None) in guesses:
                            guesses[x.get('IDMovieImdb', None)] = 0
                        guesses[x.get('IDMovieImdb', None)] += 1
                    del guesses[None]
                    guesses = guesses.items()
                    guesses.sort(lambda x,y: cmp(x[1], y[1]))
                    if guesses:
                        imdb = 'tt%07d' % int(guesses[-1][0])
        results = yield tvdb.search(alias)
        if len(results) == 0:
            print 'No query results'
            continue
        if imdb:
            for data in results:
                if data.imdb == imdb:
                    print 'Auto-mapping to'
                    print '  id=%s name="%s" year="%s"' % (data.id, data.title, data.year)
                    yield kaa.webmetadata.match(item.filename, data.id, metadata=item)
                    break
            else:
                imdb = None
        if not imdb and len(results) == 1:
            data = results[0]
            print 'Auto-mapping to'
            print '  id=%s name="%s" year="%s"' % (data.id, data.title, data.year)
            yield kaa.webmetadata.match(item.filename, data.id, metadata=item)
    sys.exit(0)

main()
kaa.main.run()
