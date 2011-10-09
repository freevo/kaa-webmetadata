import os
import stat
import xmlrpclib

import kaa
import core

class Server(object):
    server = None
    token = None
    
    @kaa.threaded(core.WORKER_THREAD)
    def search(self, items):
        if self.server is None:
            self.server = xmlrpclib.ServerProxy("http://api.opensubtitles.org/xml-rpc")
            self.session = self.server.LogIn("","","en","OS Test User Agent")
            self.token = self.session["token"]
            kaa.OneShotTimer(self.disconnect).start(20)
        try:
            return self.server.CheckMovieHash(self.token, items)
        except Exception, e:
            return None

    @kaa.threaded(core.WORKER_THREAD)
    def disconnect(self):
        self.server.LogOut(self.token)
        self.server = self.session = self.token = None

opensubtitles = Server()

@kaa.coroutine()
def search(filename, metadata=None):
    if not metadata:
        metadata = kaa.metadata.parse(filename)
    search_list = [metadata.get('hash')]
    result = yield opensubtitles.search(search_list)
    if result and result['data'] and result['data'][metadata.get('hash')]:
        yield result['data'][metadata.get('hash')]['MovieImdbID']
    yield None
