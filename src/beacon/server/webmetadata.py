# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# webmetadata.py - Webmetadata Plugin
# -----------------------------------------------------------------------------
# This file provides a bridge between the kaa.webmetadata and Beacon.
# It will be installed in the kaa.beacon.server tree
#
# -----------------------------------------------------------------------------
# kaa.webmetadata - Receive Metadata from the Web
# Copyright (C) 2010-2012 Dirk Meyer
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

# python imports
import os
import time
import logging

# kaa imports
import kaa
import kaa.webmetadata
import kaa.beacon

# relative beacon server imports
from ..parser import register as beacon_register

# get logging object
log = logging.getLogger('beacon.webmetadata')

SYNC_INTERVAL = 24 * 60 * 60    # every 24 hours
PLUGIN_VERSION = 0.2

class Plugin(object):
    """
    This is class is used as a namespace and is exposed to beacon.
    """

    guessing = []
    guessing_failed = []

    def set_attribute(self, attributes, attr, value):
        """
        Set a new value. Only trigger the setter if the value changed
        """
        if attributes.get(attr) == value:
            # nothing changed
            return
        if isinstance(value, (str, unicode)) and value.startswith('http://'):
            if attributes.get(attr) == self.db.md5url(value, 'images'):
                # image already stored to disk
                return
            if attr == 'image' and attributes.get('thumbnail'):
                # hack: request a thumbnail here to force downloading
                t = attributes.get('thumbnail')
                t.create(t.PRIORITY_LOW)
                return
        # changed value
        attributes[attr] = value

    @kaa.coroutine(policy=kaa.POLICY_SYNCHRONIZED)
    def guess_metadata(self, filename, timestamp):
        """
        Guess metadata based on filename and attributes
        """
        if not filename in self.guessing:
            # already done
            yield None
        self.guessing.remove(filename)
        # Slow down guessing and make sure the file is in sync in the database
        if timestamp + 1 > time.time():
            log.info('wait for database sync')
            yield kaa.delay(1)
        try:
            attributes = (yield kaa.beacon.get(filename))
        except Exception, e:
            # something is wrong here, maybe the item does not exist
            # anymore.
            yield None
        try:
            metadata = kaa.webmetadata.parse(filename)
        except Exception, e:
            # Something went wrong here
            yield None
        if metadata and metadata.name:
            # Result from previous guessing (e.g. tv series)
            self.set_metadata(filename, attributes)
            yield None
        series = attributes.get('series', None)
        if series:
            if series in self.guessing_failed:
                log.info('skip guessing %s', filename)
                yield None
            log.info('guess %s', filename)
            result = (yield kaa.webmetadata.tv.search(filename, attributes))
            # mark file as guessed
            attributes['webmetadata'] = filename
            if len(result) == 1:
                # only one result, this has to be a match
                yield kaa.webmetadata.tv.add_series_by_search_result(result[0], alias=series)
                # now that we have data run set_metadata again
                self.set_metadata(filename, attributes)
            self.guessing_failed.append(series)
            yield None
        if not attributes.get('length') or attributes.get('length') < 60 * 60:
            # less than an hour does not look like a movie
            yield None
        # we use the movie hash here. Therefore, the file should
        # be finished downloading or copying. This slows us down,
        # but it is the guessing part that runs in the background
        # anyway.
        try:
            filesize = os.path.getsize(filename)
            yield kaa.delay(5)
            if filesize != os.path.getsize(filename):
                # still growing, we do not want to handle this
                # file. Beacon will catch it again and we will be
                # here again.
                yield None
        except OSError:
            # file is deleted while we were checking
            yield None
        # check the movie database
        log.info('guess %s', filename)
        try:
            result = yield kaa.webmetadata.movie.search(filename, attributes)
        except Exception, e:
            # something went wrong, maybe we have more luck next time
            log.exception('kaa.webmetadata.movie.search')
            yield None
        # mark file as guessed
        attributes['webmetadata'] = filename
        if len(result) > 1:
            # too many results, maybe only one is likely
            result = [ r for r in result if r.likely ]
        if len(result) == 1:
            # only one result, this has to be a match
            try:
                yield kaa.webmetadata.movie.add_movie_by_id(filename, result[0].id)
            except Exception, e:
                # something went wrong, maybe we have more luck next time
                log.exception('kaa.webmetadata.movie.search')
                attributes['webmetadata'] = ''
                yield None
            # now that we have data run set_metadata again
            self.set_metadata(filename, attributes)
        yield None

    def set_metadata(self, filename, attributes):
        """
        Sync with kaa.webmetadata databases
        """
        if not filename:
            # no metadata exists and we have no filename to get it
            return
        try:
            metadata = kaa.webmetadata.parse(filename, attributes)
        except Exception, e:
            # Something went wrong here
            return
        if not metadata or not metadata.name:
            # no metadata exists
            if not attributes.get('webmetadata') or filename != attributes.get('webmetadata'):
                # either never guessed or the filename changed
                if not filename in self.guessing:
                    # remember that we want to guess the filename in
                    # case the file is growing. In that case various
                    # guess_metadata calls may queue themselves
                    # because guess_metadata may take several seconds
                    # and has POLICY_SYNCHRONIZED.
                    self.guessing.append(filename)
                    self.guess_metadata(filename, time.time())
            return
        try:
            self.set_attribute(attributes, 'webmetadata', filename)
            self.set_attribute(attributes, 'title', metadata.name)
            self.set_attribute(attributes, 'description', metadata.overview)
            if isinstance(metadata, kaa.webmetadata.Episode):
                self.set_attribute(attributes, 'movie', False)
                self.set_attribute(attributes, 'series', metadata.series.name)
                self.set_attribute(attributes, 'image', metadata.image)
                self.set_attribute(attributes, 'poster', metadata.poster)
                if metadata.imdb:
                    self.set_attribute(attributes, 'imdb', metadata.imdb)
                else:
                    self.set_attribute(attributes, 'imdb', metadata.series.imdb)
            if isinstance(metadata, kaa.webmetadata.Movie):
                self.set_attribute(attributes, 'imdb', metadata.imdb)
                self.set_attribute(attributes, 'movie', True)
                if metadata.poster:
                    self.set_attribute(attributes, 'poster', metadata.poster)
        except Exception, e:
            log.exception('webmetadata assign error')

    def parser(self, item, attributes, type):
        """
        Plugin for the beacon.parser. This function is called when
        beacon gathers information about an item in its parse
        function.
        """
        if type != 'video' or not item.filename:
            return None
        if not attributes:
            attributes = item
        self.set_metadata(item.filename, attributes)
        return None

    @kaa.rpc.expose('webmetadata.sync')
    @kaa.coroutine(policy=kaa.POLICY_SINGLETON)
    def sync(self, force=False):
        """
        Sync the kaa.webmetadata databases
        """
        log.info('sync web metadata')
        for module in kaa.webmetadata.tv.backends.values() + kaa.webmetadata.movie.backends.values():
            yield module.sync(force)
        log.info('adjust items')
        for item in (yield kaa.beacon.query(type='video')):
            self.set_metadata(item.filename, item)
            yield kaa.NotFinished

    @kaa.coroutine()
    def auto_sync(self):
        """
        Check if the database requires a sync
        """
        if float(self.db.get_metadata('webmetadata::version', 0)) != PLUGIN_VERSION:
            log.info('force webmetadata resync')
            # force a complete resync here
            for item in (yield kaa.beacon.query(type='video')):
                item['webmetadata'] = ''
            self.db.set_metadata('webmetadata::lastsync', 0)
            self.db.set_metadata('webmetadata::version', PLUGIN_VERSION)
        while True:
            last = int(self.db.get_metadata('webmetadata::lastsync', 0))
            if time.time() + 10 < last + SYNC_INTERVAL:
                # wait until 24 hours are over
                yield kaa.delay(last + SYNC_INTERVAL - time.time())
            try:
                yield self.sync()
                self.db.set_metadata('webmetadata::lastsync', int(time.time()))
            except Exception:
                log.exception('sync error')
                # something went wrong, maybe network down or
                # something else. Try again in 60 seconds
                yield kaa.delay(60)

    def _signal_sync(self, msg=None):
        """
        Signal handler with the current status of the sync. The signal
        will be forwarded to all clients.
        """
        self.notify_client('webmetadata.signal_sync', msg)

    @staticmethod
    def init(server, db):
        """
        Init the plugin.
        """
        plugin = Plugin()
        plugin.db = db
        kaa.webmetadata.init(db.directory)

        beacon_register(None, plugin.parser)
        kaa.beacon.register_file_type_attrs('video',
            webmetadata = (str, kaa.beacon.ATTR_SIMPLE),
            imdb = (str, kaa.beacon.ATTR_SIMPLE),
            poster = (str, kaa.beacon.ATTR_SIMPLE),
            movie = (bool, kaa.beacon.ATTR_SEARCHABLE))

        server.ipc.register(plugin)
        plugin.notify_client = server.notify_client
        kaa.webmetadata.signals['sync'].connect(plugin._signal_sync)
        kaa.webmetadata.signals['changed'].connect(plugin.sync)

        # schedule sync() every day and call on startup if it was not
        # called in the last 24 hours
        plugin.auto_sync()
