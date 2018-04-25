#!/usr/bin/env python
#
# Copyright 2018 Odom Lab, CRUK-CI, University of Cambridge
#
# This file is part of the osqutil python package.
#
# The osqutil python package is free software: you can redistribute it
# and/or modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# The osqutil python package is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with the osqutil python package.  If not, see
# <http://www.gnu.org/licenses/>.

'''Simple module used to remove dependencies between the logging and
config systems. Config uses logging, but not the other way around.'''

import logging

# This logging function is used everywhere, so we need to define it
# before we try importing anything else (and be very careful about
# circular dependencies here). Do not import anything into this module
# without careful consideration (not even Config!).
def configure_logging(name=None,
                      handler=logging.StreamHandler(),
                      formatter=None,
                      level=logging.WARNING):  # defines the minimum log level

  '''Central configuration of all loggers. Provides default handlers
  and formatters, although these can be supplied as required.'''

  default = 'pipeline'
  if name is None:
    name = default
  elif name != default:
    name = default + '.' + name

  logger  = logging.getLogger(name)

  if formatter is None:
    frmt      = "[%%(asctime)s]%s_%%(levelname)s: %%(message)s" % (name,)
    formatter = logging.Formatter(frmt)

  handler.setFormatter(formatter)

  # In principle, we only want to add a single handler to top-level
  # loggers; all other loggers inherit this handler. We also set to
  # the lowest logging level requested.
  def_log = logging.getLogger(default)
  if len(def_log.handlers) == 0:
    def_log.addHandler(handler)
    def_log.setLevel(level)
  else:
    def_log.setLevel(min(def_log.level, level))

  return logger
