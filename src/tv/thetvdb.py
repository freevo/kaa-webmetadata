# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# tvdb.py - TVDB Database
# -----------------------------------------------------------------------------
# kaa.webmetadata - Receive Metadata from the Web
# Copyright (C) 2009-2013 Dirk Meyer
#
# First Edition: Dirk Meyer <https://github.com/Dischi>
# Maintainer:    Dirk Meyer <https://github.com/Dischi>
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

__all__ = [ 'TVDB' ]

# python imports
import os
import sys
import xml.sax
import urllib
import re
import time
import logging
import zipfile

# kaa imports
import kaa
import kaa.db
from kaa.saxutils import ElementParser

# kaa.webmetadata imports
import core
from .. import signals

# get logging object
log = logging.getLogger('beacon.tvdb')

# internal version
VERSION = 0.3

WORKER_THREAD = 'WEBMETADATA'

@kaa.threaded(WORKER_THREAD)
def parse(url):
    """
    Threaded XML parser
    """
    results = []
    def handle(element):
        info = {}
        if element.content:
            results.append((element.tagname, element.content))
        else:
            for child in element:
                if child.content:
                    info[child.tagname] = child.content
            results.append((element.tagname, info))
    e = ElementParser()
    e.handle = handle
    parser = xml.sax.make_parser()
    parser.setContentHandler(e)
    parser.parse(url)
    return e.attr, results


@kaa.threaded(WORKER_THREAD)
def download(url):
    return urllib.urlopen(url).read()


class Episode(core.Episode):
    """
    Object for an episode
    """

    image = None

    def __init__(self, tvdb, series, season, dbrow):
        super(Episode, self).__init__()
        self._dbrow = dbrow
        self.tvdb = tvdb
        self.series = series
        self.season = season
        self.number = dbrow['data'].get('EpisodeNumber')
        self.name = dbrow['name'] or 'Season %s, Episode %s' % (self.season.number, self.number)
        self.imdb = dbrow['data'].get('IMDB_ID')
        self.date = dbrow['data'].get('FirstAired', None)
        self.overview = dbrow['data'].get('Overview')
        if dbrow['data'].get('filename'):
            self.image = self.tvdb.hostname + '/banners/' + dbrow['data']['filename']


class Season(core.Season):
    """
    Object for a season
    """
    def __init__(self, tvdb, series, season):
        super(Season, self).__init__()
        self._episode_cache = []
        self._episode_cache_ver = None
        self.tvdb = tvdb
        self.series = series
        self.number = season

    @property
    def episodes(self):
        """
        A list of all episodes as Episode objects for this season.  The list is
        in order, such that the list index corresponds with the episode number,
        starting at 0 (i.e. episodes[0] is episode 1).
        """
        if self._episode_cache_ver == self.tvdb.version:
            return self._episode_cache
        dbrows = self.tvdb._db.query(type='episode', parent=self.series._dbrow, season=self.number)
        episodes = [Episode(self.tvdb, self.series, self, dbrow) for dbrow in dbrows]
        # We can't assume the episode list will be in order, and that we won't
        # have any gaps.  So determine the highest episode, populate a list of
        # Nones, and fill in each index based on the episode number.  XXX: this
        # assumes EpisodeNumber is always an int.  If it may not be, we should
        # construct a dict instead.
        highest = max(int(ep.number) for ep in episodes) if episodes else 0
        if highest > 1000:
            # Small sanity check to prevent us from constructing a massive list.
            raise ValueError('Highest episode # is %d which is unexpectedly high' % highest)
        self._episode_cache = [None for i in range(highest)]
        for ep in episodes:
            if int(ep.number):
                # if ep.number is 0 it is some kind of special episode
                # we cannot handle here
                self._episode_cache[int(ep.number)-1] = ep
        self._episode_cache_ver = self.tvdb.version
        return self._episode_cache

    def _get_banner(self, btype):
        """
        Get the image/poster/banner (btype)
        """
        banner = []
        for entry in self.series._get_banner(u'season'):
            if entry.data.get('Season', None) == str(self.number) and \
                    entry.data.get('BannerType2', None) == btype:
                banner.append(entry)
        return banner

    def get_all_posters(self):
        """
        Get all possible poster images
        """
        return self._get_banner('season')

    def get_all_banners(self):
        """
        Get all possible banner images
        """
        return self._get_banner('seasonwide')

    @property
    def poster(self):
        if self.get_all_posters():
            return '%s/%s.poster.%02d.jpg' % (self.tvdb.imagedir, self.series._dbrow['tvdb'], self.number)
        return self.series.poster

    @property
    def banner(self):
        if self.get_all_banners():
            return '%s/%s.banner.%02d.jpg' % (self.tvdb.imagedir, self.series._dbrow['tvdb'], self.number)
        return self.series.banner


class Series(core.Series):
    """
    Object for a series
    """
    def __init__(self, tvdb, dbrow):
        super(Series, self).__init__()
        self._keys = self._keys + [ 'banner', 'posters', 'images' ]
        self._dbrow = dbrow
        self._season_cache = []
        self._season_cache_ver = None
        self.name = dbrow['name']
        self.id = dbrow['id']
        self.tvdb = tvdb
        self.imdb = dbrow['data'].get('IMDB_ID')
        self.overview = dbrow['data'].get('Overview')

    @property
    def seasons(self):
        """
        A list of all seasons as Season objects for this series.  The list is
        in order, such that the list index corresponds with the series number,
        starting at 0 (i.e. seasons[0] is season 1).
        """
        if self._season_cache_ver == self.tvdb.version:
            return self._season_cache
        # Find out how many seasons in this series by fetching the highest season.
        seasons = self.tvdb._db.query(type='episode', parent=self._dbrow, attrs=['season'], distinct=True)
        highest = max(r['season'] for r in seasons) if seasons else 0
        self._season_cache = [Season(self.tvdb, self, n + 1) for n in range(highest)]
        self._season_cache_ver = self.tvdb.version
        return self._season_cache

    @property
    def episodes(self):
        """
        A list of episodes for all episodes in this series, for all seasons.
        """
        episodes = []
        for season in self.seasons:
            episodes.extend([e for e in season.episodes if e is not None])
        return episodes

    def _get_banner(self, btype):
        """
        Get the image/poster/banner (btype)
        """
        banner = []
        for r in self.tvdb._db.query(type='banner', parent=self._dbrow, btype=btype):
            entry = r.get('data')
            for key, value in entry.items():
                if key.lower().endswith('path'):
                    entry[key] = self.tvdb.hostname + '/banners/' + str(value)
            entry.pop('BannerType')
            entry.pop('id')
            i = core.Image()
            i.url = entry['BannerPath']
            i.thumbnail = entry.get('ThumbnailPath', i.url)
            i.data = entry
            banner.append(i)
        banner.sort(lambda x,y: -cmp(float(x.data.get('Rating', 0)), float(y.data.get('Rating', 0))))
        return banner

    def get_all_images(self):
        """
        Get all possible background images
        """
        return self._get_banner(u'fanart')

    def get_all_posters(self):
        """
        Get all possible poster images
        """
        return self._get_banner(u'poster')

    def get_all_banners(self):
        """
        Get all possible banner images
        """
        return self._get_banner(u'series')

    @property
    def image(self):
        """
        Path in the local filesystem were the background image is stored
        """
        if self.get_all_images():
            return '%s/%s.image.jpg' % (self.tvdb.imagedir, self._dbrow['tvdb'])

    @property
    def poster(self):
        """
        Path in the local filesystem were the poster image is stored
        """
        if self.get_all_posters():
            return '%s/%s.poster.jpg' % (self.tvdb.imagedir, self._dbrow['tvdb'])

    @property
    def banner(self):
        """
        Path in the local filesystem were the banner image is stored
        """
        if self.get_all_banners():
            return '%s/%s.banner.jpg' % (self.tvdb.imagedir, self._dbrow['tvdb'])


class SearchResult(core.Series):
    def __init__(self, id, name, overview, year, imdb):
        self.id = id
        self.name = name
        self.overview = overview
        self.year = None
        self.imdb = imdb
        if year and len(year.split('-')) == 3:
            self.year = year.split('-')[0]


class TVDB(core.Database):
    """
    Database object for thetvdb.org
    """

    scheme = 'thetvdb:'

    # cache for faster access
    __get_series_cache = None

    def __init__(self, database, apikey='1E9534A23E6D7DC0'):
        super(TVDB, self).__init__(database)
        self.hostname = 'http://www.thetvdb.com'
        self._apikey = apikey
        self._series_cache = []
        self._series_cache_ver = None
        self.api = '%s/api/%s/' % (self.hostname, self._apikey)
        # set up the database itself
        self._db.register_object_type_attrs("series",
            tvdb = (int, kaa.db.ATTR_SEARCHABLE),
            name = (unicode, kaa.db.ATTR_SEARCHABLE),
            data = (dict, kaa.db.ATTR_SIMPLE),
        )
        self._db.register_object_type_attrs("alias",
            tvdb = (unicode, kaa.db.ATTR_SEARCHABLE | kaa.db.ATTR_IGNORE_CASE),
        )
        self._db.register_object_type_attrs("episode",
            tvdb = (int, kaa.db.ATTR_SEARCHABLE),
            name = (unicode, kaa.db.ATTR_SEARCHABLE),
            season = (int, kaa.db.ATTR_SEARCHABLE),
            episode = (int, kaa.db.ATTR_SEARCHABLE),
            date = (unicode, kaa.db.ATTR_SEARCHABLE),
            data = (dict, kaa.db.ATTR_SIMPLE),
        )
        self._db.register_object_type_attrs("banner",
            tvdb = (int, kaa.db.ATTR_SEARCHABLE),
            btype = (unicode, kaa.db.ATTR_SEARCHABLE),
            data = (dict, kaa.db.ATTR_SIMPLE),
        )

    def _update_db(self, type, tvdb, parent=None, **kwargs):
        """
        Update the database, does not commit changes
        """
        if parent:
            current = self._db.query(type=type, tvdb=tvdb, parent=parent)
        else:
            current = self._db.query(type=type, tvdb=tvdb)
        if not current:
            if parent:
                kwargs['parent'] = parent
            return self._db.add(type, tvdb=tvdb, **kwargs)['id']
        self._db.update(current[0], **kwargs)
        return current[0]['id']

    @kaa.coroutine(policy=kaa.POLICY_SYNCHRONIZED)
    def _update_series(self, id):
        info = self._db.query_one(type='series', tvdb=id)
        if info:
            signals['sync'].emit(info.get('name'))
        else:
            signals['sync'].emit()
        # download thetvdb information
        f = open(kaa.tempfile('thetvdb-%s.zip' % id), 'w')
        f.write((yield download(self.api + 'series/%s/all/en.zip' % id)))
        f.close()
        # load zip data
        z = zipfile.ZipFile(f.name)
        parent = None
        for name, data in (yield parse(z.open('en.xml')))[1]:
            if name == 'Series':
                objid = self._update_db('series', int(data.get('id')), name=data.get('SeriesName'), data=data)
                parent = ('series', objid)
                # delete old entries
                for e in self._db.query(type='episode', parent=parent):
                    self._db.delete(e)
            elif name == 'Episode':
                if not parent:
                    raise ValueError('Unexpected parse error: got Episode element before Series')
                self._update_db('episode', int(data.get('id')), name=data.get('EpisodeName'), parent=parent,
                    season=int(data.get('SeasonNumber')), episode=int(data.get('EpisodeNumber')),
                    date=data.get('FirstAired', None), data=data)
            else:
                log.error('unknown element: %s', name)
        self._db.commit()
        # load image information
        for name, data in (yield parse(z.open('banners.xml')))[1]:
            if name == 'Banner':
                self._update_db('banner', int(data.get('id')), btype=data.get('BannerType'),
                    data=data, parent=parent)
            else:
                log.error('unknown element: %s', name)
        self._db.commit()
        os.unlink(f.name)
        # download metadata images
        info = self._db.query_one(type='series', tvdb=id)
        if not info:
            yield None
        serie = Series(self, info)
        if serie.image and not os.path.isfile(serie.image):
            data = yield serie.get_all_images()[0].fetch()
            open(serie.image, 'w').write(data)
        if serie.poster and not os.path.isfile(serie.poster):
            data = yield serie.get_all_posters()[0].fetch()
            open(serie.poster, 'w').write(data)
        if serie.banner and not os.path.isfile(serie.banner):
            data = yield serie.get_all_banners()[0].fetch()
            open(serie.banner, 'w').write(data)
        for season in serie.seasons:
            if season.poster and season.get_all_posters() and not os.path.isfile(season.poster):
                data = yield season.get_all_posters()[0].fetch()
                open(season.poster, 'w').write(data)
            if season.banner and season.get_all_banners() and not os.path.isfile(season.banner):
                data = yield season.get_all_banners()[0].fetch()
                open(season.banner, 'w').write(data)

    @property
    def series(self):
        """
        A list of Series objects of all series in the database.
        """
        if self._series_cache_ver != self.version:
            self._series_cache = [Series(self, data) for data in self._db.query(type='series')]
            self._series_cache_ver = self.version
        return self._series_cache

    def get_series(self, name):
        """
        Fetch a series by the series name or associated alias.
        """
        if self.__get_series_cache and self.__get_series_cache[0] == self.version and \
           self.__get_series_cache[1] == name:
            return self.__get_series_cache[2]
        obj, series = self._db.query_one(type='alias', tvdb=kaa.py3_str(name)), None
        if obj:
            series = Series(self, self._db.query_one(type='series', id=obj['parent_id']))
        self.__get_series_cache = self.version, name, series
        return series

    def get_entry_from_metadata(self, metadata, alias=None):
        """
        Get an Entry object based on the kaa.metadata object.  The returned
        entry may be a Series, Season, or Episode, depending on the granularity
        of metadata available.
        """
        result = self.get_series(metadata.get('series') or alias)
        if result and metadata.get('season'):
            result = Season(self, result, metadata['season'])
            if result and metadata.get('episode'):
                if metadata.get('episode') <= len(result.episodes):
                    result = result.episodes[metadata.get('episode')-1]
        return result

    @kaa.coroutine()
    def search(self, name, filename=None, metadata=None):
        """
        Search for a series
        """
        result = []
        name = urllib.quote(name.replace('.', ' ').replace('-', ' ').replace('_', ' '))
        url = self.hostname + '/api/GetSeries.php?seriesname=%s' % name
        for name, data in (yield parse(url))[1]:
            result.append(SearchResult('thetvdb:' + data['seriesid'], data['SeriesName'],
                data.get('Overview', None), data.get('FirstAired', None), data.get('IMDB_ID')))
        yield result

    @kaa.coroutine()
    def add_series_by_id(self, id, alias=None):
        """
        Adds the TV series specified by the TVDB id number to the local
        database.

        :param id: the TVDB id number
        :param alias: optional alias with which to associate this TV series
                      for later lookup.

        If the series is already added to the database, the given alias will be
        associated with it.
        """
        if isinstance(alias, basestring):
            alias = kaa.py3_str(alias)
        if id.startswith(TVDB.scheme):
            id = id[len(TVDB.scheme):]
        if not self._db.get_metadata('webmetadata::servertime'):
            # DB does not contain server time.  Fetch and set.
            attr, data = yield parse(self.hostname + '/api/Updates.php?type=none')
            data = dict(data)
            self._db.set_metadata('webmetadata::servertime', int(data['Time']))
            self._db.commit()
        series = self._db.query_one(type='series', tvdb=id)
        if not series:
            log.info('query thetvdb for %s' % id)
            for i in range(3):
                # try to get results three times before giving up
                yield self._update_series(id)
                series = self._db.query_one(type='series', tvdb=id)
                if series:
                    break
            else:
                log.error('TheTVDB failed to provide a result')
                yield False
        self._update_db('alias', series['name'], parent=series)
        if alias:
            for old in self._db.query(type='alias', tvdb=alias):
                # remove old (wrong) mapping (if given)
                self._db.delete(old)
            self._update_db('alias', alias, parent=series)
        self._db.commit()
        self.notify_resync()
        yield Series(self, series)

    @kaa.coroutine(policy=kaa.POLICY_SYNCHRONIZED)
    def sync(self, force=False):
        """
        Sync database with server
        """
        servertime = self._db.get_metadata('webmetadata::servertime')
        if not servertime:
            # No servertime stored, so there must not be any series in db.
            yield
        if str(self._db.get_metadata('webmetadata::version')) != str(VERSION):
            log.warning('kaa.webmetadata version change, force complete resync')
            force = True
        # Grab all series ids currently in the DB.
        series = [ record['tvdb'] for record in self._db.query(type='series') ]
        if force:
            for id in series:
                yield self._update_series(id)
            self._db.set_metadata('webmetadata::version', VERSION)
            self.notify_resync()
            yield
        # Fetch all updates since the last stored servertime
        url = self.hostname + '/api/Updates.php?type=all&time=%s' % servertime
        attr, updates = (yield parse(url))
        banners = []
        timeinfo = None
        for element, data in updates:
            if element == 'Series' and int(data) in series:
                log.info('Update series %s', data)
                yield self._update_series(data)
            elif element == 'Time':
                timeinfo = data
        if timeinfo:
            log.info('Set servertime %s', timeinfo)
            self._db_servertime = int(timeinfo)
            self._db.set_metadata('webmetadata::servertime', int(timeinfo))
        self._db.commit()
        self.notify_resync()

    @kaa.coroutine()
    def add_series_by_search_result(self, result, alias=None):
        """
        Adds a new series given a SearchResult to the database.
        """
        if not result.id.startswith('thetvdb:'):
            raise ValueError('Search result is not a valid TheTVDB result')
        yield (yield self.add_series_by_id(result.id, alias))

    def delete_series(self, series):
        """
        Deletes a series from the database.

        :param series: the series to remove
        :type series: Series object
        """
        self._db.delete_by_query(parent=series._dbrow)
        self._db.delete(series._dbrow)
        self._db.commit()
        self.notify_resync()
