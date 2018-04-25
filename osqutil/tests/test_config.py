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

'''
Tests for loading the config file correctly.
'''

# Okay, so this is a non-core module dependency. It's only for testing
# though. FIXME maybe look at reimplementing these tests to remove the
# django requirement.
from django.test import TestCase
import os
from shutil import copy
import xml.etree.ElementTree as ET

from ..config import Config
import logging
from ..setup_logs import configure_logging
LOGGER = configure_logging()

THISDIR   = os.path.dirname( os.path.realpath( __file__ ) )
CLEAN_CFG = os.path.join(THISDIR, 'clean_config.xml')
TEST_CFG  = os.path.join(THISDIR, 'test_config.xml')

class TestConfig(TestCase):

  # Note that we will want to use this same set_up and tear_down for all
  # pipeline testing. Think about how this will all work with parallel
  # test processes! FIXME.
  def setUp(self):
    copy(CLEAN_CFG, TEST_CFG)
    LOGGER.setLevel(logging.FATAL) # For verbose testing, set this to DEBUG.

  def _parse_wrapper(self):
    cfg = Config(TEST_CFG, force_reload=True)
    self.assertEqual(cfg.aligner, 'bwa')
    self.assertEqual(cfg['aligner'], 'bwa')

    cfg.aligner = 'testing'
    self.assertEqual(cfg.aligner, 'testing')
    self.assertEqual(cfg['aligner'], 'testing')

    self.assertEqual(cfg.reallocation_factors, ['PolIII', 'TFIIIC'])

    with self.assertRaises(IndexError):
      val = cfg['not_in_this_config']

    with self.assertRaises(KeyError):
      cfg['not_in_this_config'] = True

  def test_parsing(self):
    '''
    Tests that the config parser and underlying singleton object behaves itself.
    '''
    # We have to wrap the config call so the object goes properly out
    # of scope.
    self._parse_wrapper()

    LOGGER.debug("Rereading test config file.")
    newcfg = ET.parse(TEST_CFG)
    opt = newcfg.getroot().find("./section[@name='Pipeline']/option[@name='aligner']")
    self.assertEqual(opt.text, 'testing')

  def tearDown(self):
    if os.path.exists(TEST_CFG):
      os.unlink(TEST_CFG)

