# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# setup.py - setup script for kaa.webmetadata
# -----------------------------------------------------------------------------
# $Id$
#
# -----------------------------------------------------------------------------
# kaa.webmetadata - Receive Metadata from the Web
# Copyright (C) 2009 Dirk Meyer
#
# Please see the file AUTHORS for a complete list of authors.
#
# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version
# 2.1 as published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA
#
# -----------------------------------------------------------------------------

# python imports
import sys

try:
    # kaa base imports
    from kaa.distribution.core import Extension, setup
except ImportError:
    print 'kaa.base not installed'
    sys.exit(1)


setup(module       = 'webmetadata',
      version      = '0.1.0',
      license      = 'LGPL',
      summary      = 'Receive Metadata from the Web',
      scripts      = [ 'bin/webinfo' ],
      plugins      = { 'kaa.beacon.server.plugins': 'src/beacon' },
      rpminfo      = {
          'requires':       'python-kaa-base >= 0.1.2',
          'build_requires': 'python-kaa-base >= 0.1.2'
      }
      )
