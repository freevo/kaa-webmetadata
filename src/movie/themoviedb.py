# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# themoviedb.py - Access themoviedb.org
# -----------------------------------------------------------------------------
# $Id$
#
# -----------------------------------------------------------------------------
# kaa.webmetadata - Receive Metadata from the Web
# Copyright (C) 2009-2011 Dirk Meyer
#
# First Edition: Dirk Meyer <dischi@freevo.org>
# Maintainer:    Dirk Meyer <dischi@freevo.org>
#
# Please see the file AUTHORS for a complete list of authors.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MER-
# CHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
#
# -----------------------------------------------------------------------------

__all__ = [ 'MovieDB' ]

# python imports
import os
import re
import time
import xml.sax
import logging
import socket
import urllib
import urllib2

# kaa imports
import kaa
import kaa.db
from kaa.saxutils import ElementParser, Element

import core

# get logging object
log = logging.getLogger('webmetadata')

API_SERVER='api.themoviedb.org'

REMOVE_FROM_SEARCH = []
WORKER_THREAD = 'WEBMETADATA'

IMDB_REGEXP = re.compile('http://[a-z\.]*imdb.[a-z]+/[a-z/]+([0-9]+)')
IMAGE_REGEXP = re.compile('.*/([0-9]*)/')
TITLE_REGEXP = re.compile('(.*?)[\._\- ]([0-9]{4})[\._\- ].*')

class Movie(core.Movie):
    """
    Movie Information.
    """
    def __init__(self, data):
        self._data = data
        self.id = 'themoviedb:%s' % data['id']
        self.name = data['name']
        self.overview = data.get('overview')
        self.rating = data.get('rating')
        self.runtime = data.get('runtime')
        self.year = None
        self.imdb = data.get('imdb_id')
        if data.get('released') and len(data.get('released').split('-')) == 3:
            self.year = data.get('released').split('-')[0]

    def _images(self, tagname, size):
        result = []
        for id, image in self._data[tagname]:
            i = core.Image()
            for size in (size, 'mid', 'original', 'cover'):
                if size in image:
                    i.url = image[size]
                    break
            i.thumbnail = image.get('thumb')
            result.append(i)
        return result

    @property
    def posters(self):
        return self._images('poster', 'mid')

    @property
    def images(self):
        return self._images('backdrop', 'original')



class MovieDB(core.Database):

    scheme = 'themoviedb:'

    def __init__(self, database, apikey='21dfe870a9244b78b4ad0d4783251c63'):
        super(MovieDB, self).__init__(database)
        self._apikey = apikey
        self._db.register_object_type_attrs("metadata",
            metadata = (dict, kaa.db.ATTR_SIMPLE),
        )
        self._db.register_object_type_attrs("movie",
            moviedb = (int, kaa.db.ATTR_SEARCHABLE),
            imdb = (unicode, kaa.db.ATTR_SEARCHABLE),
            name = (unicode, kaa.db.ATTR_SEARCHABLE),
            data = (dict, kaa.db.ATTR_SIMPLE),
        )
        self._db.register_object_type_attrs("hash",
            moviedb = (int, kaa.db.ATTR_SIMPLE),
            value = (unicode, kaa.db.ATTR_SEARCHABLE),
        )
        if not self._db.query(type='metadata'):
            self._db.add('metadata', metadata={})

    @kaa.threaded(WORKER_THREAD)
    def download(self, url):
        results = []
        def handle(element):
            if not element.content:
                data = dict(categories=[], backdrop=[], poster=[])
                for child in element:
                    if child.tagname == 'categories' and child.type == 'genre':
                        data['categories'].append(child.name)
                    elif child.tagname == 'images':
                        for image in child:
                            if not image.type  in ('backdrop', 'poster'):
                                continue
                            for id, images in data[image.type]:
                                if id == image.id:
                                    images[image.size] = image.url
                                    break
                            else:
                                data[image.type].append((image.id, {image.size:image.url}))
                    elif child.content:
                        data[child.tagname] = child.content
                results.append(Movie(data))
        e = ElementParser('movie')
        e.handle = handle
        parser = xml.sax.make_parser()
        parser.setContentHandler(e)
        parser.parse(urllib2.urlopen(url, timeout=10))
        # request limit: 10 requests every 10 seconds per
        # IP. Just wait one second here we are OK
        time.sleep(1)
        return results

    def parse(self, filename, metadata):
        if not os.path.exists(filename):
            return
        data = []
        # search based on the movie hash
        if metadata.get('hash'):
            hash = u'%s|%s' % (metadata.get('hash'), os.path.getsize(filename))
            data = self._db.query(type='hash', value=hash)
            if data:
                data = self._db.query(type='movie', moviedb=data[0]['moviedb'])
        # search based on imdb id in nfo file
        if not data:
            nfo = os.path.splitext(filename)[0] + '.nfo'
            if os.path.exists(nfo):
                match = IMDB_REGEXP.search(open(nfo).read())
                if match:
                    data = self._db.query(type='movie', imdb=u'tt' + match.groups()[0])
        # not found
        if not data:
            return None
        # return result
        return Movie(data[0]['data'])

    @kaa.coroutine(policy=kaa.POLICY_SYNCHRONIZED)
    def search(self, filename, metadata):
        if not os.path.exists(filename):
            yield []
        apicall = 'http://api.themoviedb.org/2.1/%s/en/xml/' + self._apikey + '/%s'
        result = []
        nfo = os.path.splitext(filename)[0] + '.nfo'
        if os.path.exists(nfo) and not result:
            match = IMDB_REGEXP.search(open(nfo).read())
            if match:
                url = apicall % ('Movie.imdbLookup', 'tt' + match.groups()[0])
                result = yield self.download(url)
                for movie in result:
                    movie.likely = True
        if not result:
            # try guessing title and year
            m = TITLE_REGEXP.match(os.path.basename(filename))
            if m:
                name, year = m.groups()
                if int(year) > 1900 and int(year) <= time.localtime().tm_year:
                    # valid year
                    name = name.lower().replace('.', ' ').replace('-', ' ').replace('_', ' ')
                    url = apicall % ('Movie.search', urllib.quote('%s+%s' % (name, year)))
                    result = yield self.download(url)
                    for movie in result:
                        # mark the best matches
                        if movie.name.lower().replace('.', ' ').replace('-', ' ').replace('_', ' ') == name:
                            movie.likely = True
                        else:
                            movie.likely = False
        yield result

    @kaa.coroutine()
    def add_movie_by_id(self, filename, metadata, id):
        if not metadata.get('hash') or not metadata.get('filesize'):
            yield False
        # check if we already have that movie in the db
        data = self._db.query(type='movie', moviedb=id)
        if not data:
            # get information
            apicall = 'http://api.themoviedb.org/2.1/%s/en/xml/' + self._apikey + '/%s'
            url = apicall % ('Movie.getInfo', id)
            result = yield self.download(url)
            if result:
                self._db.add('movie', moviedb=int(result[0]._data['id']), name=result[0].name,
                    imdb=result[0]._data.get('imdb_id', u''), data=result[0]._data)
                self._db.commit()
                data = self._db.query(type='movie', moviedb=id)
        if data:
            self._db.add('hash', moviedb=id, value=u'%s|%s' % (metadata.get('hash'), metadata.filesize))
            self._db.commit()
        yield True
