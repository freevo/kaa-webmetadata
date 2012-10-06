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

class Plugin(object):
    """
    This is class is used as a namespace and is exposed to beacon.
    """

    def set_attribute(self, attributes, attr, value):
        """
        Set a new value. Only trigger the setter if the value changed
        """
        if attributes[attr] == value:
            # nothing changed
            return
        if value.startswith('http://') and attributes[attr] == self.db.md5url(value, 'images'):
            # image already stored to disk
            return
        # changed value
        attributes[attr] = value


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
            return
        try:
            self.set_attribute(attributes, 'title', metadata.name)
            self.set_attribute(attributes, 'description', metadata.overview)
            if isinstance(metadata, kaa.webmetadata.Episode):
                self.set_attribute(attributes, 'movie', False)
                self.set_attribute(attributes, 'series', metadata.series.name)
                self.set_attribute(attributes, 'image', metadata.image)
                self.set_attribute(attributes, 'poster', metadata.posters[0].url)
                if metadata.imdb:
                    self.set_attribute(attributes, 'imdb', metadata.imdb)
                else:
                    self.set_attribute(attributes, 'imdb', metadata.series.imdb)
            if isinstance(metadata, kaa.webmetadata.Movie):
                self.set_attribute(attributes, 'imdb', metadata.imdb)
                self.set_attribute(attributes, 'movie', True)
                if metadata.posters:
                    self.set_attribute(attributes, 'poster', metadata.posters[0].url)
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

    def _signal_sync(self, msg):
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
            imdb = (str, kaa.beacon.ATTR_SIMPLE),
            poster = (str, kaa.beacon.ATTR_SIMPLE),
            movie = (bool, kaa.beacon.ATTR_SEARCHABLE))

        server.ipc.register(plugin)
        plugin.notify_client = server.notify_client
        kaa.webmetadata.signals['sync'].connect(plugin._signal_sync)

        # schedule sync() every day and call on startup if it was not
        # called in the last 24 hours
        plugin.auto_sync()
