import os
import stat
import sys
import kaa.webmetadata
import kaa.webmetadata.tv
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
    missing = []

    # for alias in (yield kaa.beacon.query(type='video', attr='series')):
    #     if kaa.webmetadata.tv.series(alias):
    #         continue
    #     print 'Missing mapping for "%s"' % alias
    #     print 'Files:'
    #     # imdb = None
    #     for item in (yield kaa.beacon.query(type='video', series=alias)):
    #         print ' ', item.filename
    #         result = yield kaa.webmetadata.tv.search(item.filename, item)
    #         if len(result) == 1:
    #             yield kaa.webmetadata.tv.match(item.filename, result[0].id)
    #             print 'found'
    #             break
    #     else:
    #         print 'not found'

    for item in (yield kaa.beacon.query(type='video')):
        if not item.filename or item.get('series') or item.get('movie'):
            continue
        print item

    sys.exit(0)

main()
kaa.main.run()
