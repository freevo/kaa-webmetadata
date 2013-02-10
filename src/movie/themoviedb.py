# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# Access themoviedb.org
# -----------------------------------------------------------------------------
# kaa.webmetadata - Receive Metadata from the Web
# Copyright (C) 2009-2013 Dirk Meyer
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
import logging
import urllib
import urllib2
import json

# kaa imports
import kaa
import kaa.db
import kaa.metadata

# kaa.webmetadata imports
import core

# get logging object
log = logging.getLogger('webmetadata')

REMOVE_FROM_SEARCH = []
WORKER_THREAD = 'WEBMETADATA'

# internal version
VERSION = 0.3

IMDB_REGEXP = re.compile('http://[a-z\.]*imdb.[a-z]+/[a-z/]+([0-9]+)')
IMAGE_REGEXP = re.compile('.*/([0-9]*)/')
TITLE_REGEXP = re.compile('(.*?)[\._\- ]([0-9]{4})[\._\- ].*')

class Movie(core.Movie):
    """
    Movie Information.
    """
    def __init__(self, data, moviedb):
        self._data = data
        self.moviedb = moviedb
        self.id = 'themoviedb:%s' % data['id']
        self.name = data.get('title', None)
        self.tageline = data.get('tagline', None)
        self.overview = data.get('overview', None)
        self.rating = data.get('vote_average', None)
        self.runtime = data.get('runtime', None)
        self.year = None
        self.imdb = data.get('imdb_id', u'')
        if data.get('release_date') and len(data.get('release_date').split('-')) == 3:
            self.year = data.get('release_date').split('-')[0]

    def _get_image_by_type(self, tagname, lang='en'):
        """
        Get the poster or background image
        """
        votes1 = []
        votes2 = []
        votes3 = []
        for image in self._data.get('/images', {}).get(tagname):
            if (image.get('iso_639_1', None) or lang) == lang:
                if image.get('vote_count') > 10:
                    votes1.append(image)
                elif image.get('vote_count') > 2:
                    votes2.append(image)
                else:
                    votes3.append(image)
        result = []
        for image in sorted(votes1, key=lambda i: i.get('vote_average', 0)) + \
                sorted(votes2, key=lambda i: i.get('vote_average', 0)) + \
                sorted(votes3, key=lambda i: i.get('vote_average', 0)):
            result.append(image)
        return result

    @kaa.coroutine()
    def get_all_images(self):
        """
        Get all possible background images
        """
        cfg = (yield self.moviedb._server_cfg())['images']
        size = 'original'
        if u'w1280' in cfg['backdrop_sizes']:
            size = u'w1280'
        images = []
        for entry in self._get_image_by_type('backdrops'):
            i = core.Image()
            i.url = cfg['base_url'] + size + entry['file_path']
            i.thumbnail = cfg['base_url'] + cfg['poster_sizes'][0] + entry['file_path']
            i.data = entry
            images.append(i)
        yield images

    @kaa.coroutine()
    def get_all_posters(self):
        """
        Get all possible background posters
        """
        cfg = (yield self.moviedb._server_cfg())['images']
        size = 'original'
        # use the largest width smaller 400
        # FIXME: configure this somehow
        for w in cfg['poster_sizes']:
            if w.startswith('w') and int(w[1:]) < 400:
                size = w
        images = []
        for entry in self._get_image_by_type('posters'):
            i = core.Image()
            i.url = cfg['base_url'] + size + entry['file_path']
            i.thumbnail = cfg['base_url'] + cfg['poster_sizes'][0] + entry['file_path']
            i.data = entry
            images.append(i)
        yield images

    @property
    def image(self):
        """
        Path in the local filesystem were the background image is stored
        """
        if self._get_image_by_type('backdrops'):
            return '%s/%s.image.jpg' % (self.moviedb.imagedir, self._data['id'])

    @property
    def poster(self):
        """
        Path in the local filesystem were the poster is stored
        """
        if self._get_image_by_type('posters'):
            return '%s/%s.poster.jpg' % (self.moviedb.imagedir, self._data['id'])



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
        if self.get_metadata('version') != VERSION:
            # database outdated, delete all entries (ugly, but easier
            # in this state of development). In future version we
            # should try to update the existing db
            for entry in self._db.query(type='movie') + self._db.query(type='hash'):
                self._db.delete(entry)
        self.set_metadata('version', VERSION)

    @kaa.threaded(WORKER_THREAD)
    def _server_call(self, command, **kwargs):
        """
        Call the given command.
        """
        kwargs['api_key'] = self._apikey
        url = 'http://api.themoviedb.org/3/' + command + '?' + urllib.urlencode(kwargs)
        request = urllib2.Request(url, headers={'Accept' : 'application/json'})
        return json.load(urllib2.urlopen(request))

    @kaa.coroutine()
    def _server_cfg(self):
        """
        Get the server config for images. Cache this information for 1
        hour even between various instances.
        """
        cfg = self.get_metadata('configuration')
        if not cfg or cfg[0] < int(time.time()) - 3600:
            config = yield self._server_call('configuration')
            if config and config.get('images'):
                config = int(time.time()), config
                self.set_metadata('configuration', config)
        yield self.get_metadata('configuration')[1]

    def parse(self, filename, metadata):
        """
        Return the Movie object for the filename if it is in the database
        """
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
        return Movie(data[0]['data'], self)

    @kaa.coroutine(policy=kaa.POLICY_SYNCHRONIZED)
    def search(self, filename, metadata):
        """
        Search for possible movie information for the given filename
        """
        if not os.path.exists(filename):
            yield []
        # We are limited to 30 requests per 10 seconds. Wait one
        # second before doing some searches.
        yield kaa.delay(1)
        result = []
        nfo = os.path.splitext(filename)[0] + '.nfo'
        if os.path.exists(nfo) and not result:
            match = IMDB_REGEXP.search(open(nfo).read())
            if match:
                result = yield self._server_call('movie/tt%s' % match.groups()[0])
                if result:
                    movie = Movie(result, self)
                    movie.likely = True
                    result = [ movie ]
        if not result:
            # try guessing title and year
            m = TITLE_REGEXP.match(os.path.basename(filename))
            data = None
            if m:
                name, year = m.groups()
                if int(year) > 1900 and int(year) <= time.localtime().tm_year:
                    # valid year
                    name = name.lower().replace('.', ' ').replace('-', ' ').replace('_', ' ')
                    data = yield self._server_call('search/movie', query=name, year=year)
                    if not data or not data.get('results', None) and name.find('3d') > 0:
                        name = name[:name.find('3d')]
                        data = yield self._server_call('search/movie', query=name, year=year)
            # guess by kaa.metadata title
            if not data or not data.get('results', None):
                name = kaa.metadata.parse(filename).title.lower().replace('.', ' ').\
                    replace('-', ' ').replace('_', ' ')
                data = yield self._server_call('search/movie', query=name)
                if not data or not data.get('results', None) and name.find('3d') > 0:
                    name = name[:name.find('3d')]
                    data = yield self._server_call('search/movie', query=name)
            if data and data.get('results', None):
                result = []
                for r in data.get('results'):
                    movie = Movie(r, self)
                    # mark the best matches
                    if movie.name.lower().replace('.', ' ').replace('-', ' ').replace('_', ' ') == name:
                        movie.likely = True
                    else:
                        movie.likely = False
                    result.append(movie)
        yield result

    @kaa.coroutine()
    def add_movie_by_id(self, filename, metadata, id):
        """
        Match movie id to the given filename for future lookups
        """
        if not metadata.get('hash') or not metadata.get('filesize'):
            yield False
        # check if we already have that movie in the db
        data = self._db.query(type='movie', moviedb=id)
        if not data:
            # We are limited to 30 requests per 10 seconds. Wait one
            # second before doing the searches.
            yield kaa.delay(1)
            result = yield self._server_call('movie/%s' % id)
            if result:
                result['/images'] = yield self._server_call('movie/%s/images' % id)
                result['/casts'] = yield self._server_call('movie/%s/casts' % id)
                result['/keywords'] = yield self._server_call('movie/%s/keywords' % id)
                movie = Movie(result, self)
                if movie.poster and not os.path.isfile(movie.poster):
                    data = yield (yield movie.get_all_posters())[0].fetch()
                    open(movie.poster, 'w').write(data)
                if movie.image and not os.path.isfile(movie.image):
                    data = yield (yield movie.get_all_images())[0].fetch()
                    open(movie.image, 'w').write(data)
                self._db.add('movie', moviedb=int(id), name=movie.name,
                    imdb=movie.imdb, data=movie._data)
                self._db.commit()
                data = self._db.query(type='movie', moviedb=id)
        if data:
            self._db.add('hash', moviedb=id, value=u'%s|%s' % (metadata.get('hash'), metadata.filesize))
            self._db.commit()
        yield True
