# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# webmetadata.py - Webmetadata Plugin
# -----------------------------------------------------------------------------
# This file adds some webmetadata functions to beacon as well as pipes
# the kaa.webmetadata.sync command through beacon.
#
# -----------------------------------------------------------------------------
# kaa.webmetadata - Receive Metadata from the Web
# Copyright (C) 2011-2012 Dirk Meyer
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

import kaa.beacon
import kaa.metadata
import kaa.webmetadata

class Plugin(object):

    def __init__(self, client):
        """
        Init kaa.webmetadata based on beacon database
        """
        kaa.webmetadata.init(client.get_db_info()['directory'])
        self.client = client

    @kaa.coroutine()
    def sync(self, force=False):
        """
        Sync the database
        """
        yield self.client.rpc('webmetadata.sync', force=force)

    @kaa.rpc.expose('webmetadata.signal_sync')
    def _signal_sync(self, msg):
        """
        Server callback for sync progress signal
        """
        if not kaa.beacon.is_server():
            kaa.webmetadata.signals['sync'].emit(msg)

    @staticmethod
    def init(client):
        """
        Init the plugin (called from within beacon)
        """
        plugin = Plugin(client)
        kaa.webmetadata.sync = plugin.sync
        client.channel.register(plugin)
        return dict(webmetadata=plugin)
