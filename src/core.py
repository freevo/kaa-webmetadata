# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# Core classes for kaa.webmetadata
# -----------------------------------------------------------------------------
# kaa.webmetadata - Receive Metadata from the Web
# Copyright (C) 2011-2013 Dirk Meyer
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

import os
import urllib

import kaa
import kaa.db
from kaa.inotify import INotify

WORKER_THREAD = 'WEBMETADATA'

class MediaInfo(kaa.Object):

    def get_metadata(self, key):
        """
        Get database metadata
        """
        return None

    def set_metadata(self, key, value):
        """
        Set database metadata
        """
        pass


class Database(MediaInfo):

    __kaasignals__ = {
        'changed':
            '''
            Signal when the database on disc changes
            ''',
        }

    def __init__(self, database):
        super(Database, self).__init__()
        # set up the database and the version file
        if isinstance(database, kaa.db.Database):
            self._db = database
        else:
            dbdir = os.path.dirname(database)
            if dbdir and not os.path.exists(dbdir):
                os.makedirs(dbdir)
            self._db = kaa.db.Database(database + '.db')
        self._versionfile = self._db.filename + '.version'
        self.imagedir = self._db.filename + '.images'
        if not os.path.isdir(self.imagedir):
            os.makedirs(self.imagedir)
        if not os.path.exists(self._versionfile):
            open(self._versionfile, 'w').write('0')
        try:
            self.version = int(open(self._versionfile).read())
        except ValueError:
            self.version = 0
        # FIXME: we need to keep the INotify object. Why?
        self.__inotify = INotify()
        self.__inotify.watch(self._versionfile, INotify.CLOSE_WRITE).connect(self._db_updated)

    def _db_updated(self, *args):
        """
        Callback from INotify when the version file changed
        """
        try:
            version = int(open(self._versionfile).read())
        except ValueError:
            version = self.version + 1
        if version != self.version:
            self.version = version
            self.signals['changed'].emit()

    def notify_resync(self):
        """
        Notify all applications using the database that a resync is necessary by
        updating the version or the db.
        """
        self.version += 1
        f = open(self._versionfile, 'w')
        f.write(str(self.version))
        f.close()

    @kaa.coroutine()
    def sync(self, force=False):
        yield None

    def get_metadata(self, key):
        """
        Get database metadata
        """
        if not self._db.query(type='metadata'):
            return None
        metadata = self._db.query(type='metadata')[0]['metadata']
        if not metadata:
            return None
        return metadata.get(key)

    def set_metadata(self, key, value):
        """
        Set database metadata
        """
        if not self._db.query(type='metadata'):
            return None
        entry = self._db.query(type='metadata')[0]
        metadata = entry['metadata'] or {}
        metadata[key] = value
        self._db.update(entry, metadata=metadata)
        self._db.commit()


class Entry(object):

    _keys = []

    def __init__(self):
        self.id = None
        for key in self._keys:
            if not hasattr(self, key):
                setattr(self, key, None)

    def items(self):
        return [ (key, getattr(self, key)) for key in self._keys ]


class Image(object):
    url = thumbnail = id = filename = ''

    @kaa.threaded(WORKER_THREAD)
    def fetch(self):
        return urllib.urlopen(self.url).read()

    def __str__(self):
        return self.url or self.thumbnail
