# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# webmetadata.py - Webmetadata Plugin
# -----------------------------------------------------------------------------
# $Id: tvdb.py 4205 2009-07-19 11:20:16Z dmeyer $
#
# This file provides a bridge between the kaa.webmetadata and Beacon.
# It will be installed in the kaa.beacon tree
#
# -----------------------------------------------------------------------------
# kaa.webmetadata - Receive Metadata from the Web
# Copyright (C) 2010-2011 Dirk Meyer
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
import logging

# kaa imports
import kaa
import kaa.webmetadata
import kaa.beacon

# relative beacon server imports
from ..parser import register as beacon_register
from ...plugins.webmetadata import ItemWrapper

# get logging object
log = logging.getLogger('beacon.webmetadata')

class Plugin(object):
    """
    This is class is used as a namespace and is exposed to beacon.
    """

    def parser(self, item, attributes, type):
        """
        Plugin for the beacon.parser
        """
        if type != 'video' or not item.filename:
            return None
        metadata = kaa.webmetadata.parse(item.filename, attributes)
        if not attributes:
            attributes = item
        if metadata:
            try:
                attributes['movie'] = False
                attributes['title'] = metadata.name
                attributes['description'] = metadata.overview
                if isinstance(metadata, kaa.webmetadata.Episode):
                    attributes['series'] = metadata.series.name
                    attributes['image'] = metadata.image
                    attributes['poster'] = metadata.posters[0].url
                if isinstance(metadata, kaa.webmetadata.Movie):
                    attributes['movie'] = True
                    if metadata.posters:
                        attributes['poster'] = metadata.posters[0].url
            except Exception, e:
                log.exception('webmetadata assign error')
            return None
        return None

    @kaa.rpc.expose('webmetadata.sync')
    @kaa.coroutine()
    def sync(self):
        log.info('sync web metadata')
        for module in kaa.webmetadata.tv.backends.values() + kaa.webmetadata.movie.backends.values():
            yield module.sync()
        log.info('adjust items')
        for item in (yield kaa.beacon.query(type='video')):
            ItemWrapper(item).sync()
            yield kaa.NotFinished

    @staticmethod
    def init(server, db):
        """
        Init the plugin.
        """
        plugin = Plugin()

        kaa.webmetadata.init(db.directory)

        # TODO: schedule sync() every day and call on startup if it
        # was not called in the last 24 hours

        beacon_register(None, plugin.parser)
        kaa.beacon.register_file_type_attrs('video',
            poster = (str, kaa.beacon.ATTR_SIMPLE),
            movie = (bool, kaa.beacon.ATTR_SEARCHABLE))
        server.ipc.register(plugin)
