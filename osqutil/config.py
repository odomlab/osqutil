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
    
'''Class definition for a singleton config class used for parsing,
editing and writing config options back to disk.'''

import os
import weakref

import xml.etree.ElementTree as ET

from .setup_logs import configure_logging
LOGGER = configure_logging('config')

################################################################

class Config(object):

  '''Creates a singleton object which acts as a facade to the
  underlying config file. Config options are set and get using the
  usual syntax (e.g. confobj.gzip, confobj.gzip = "/bin/gzip") and
  writes back out to the config file upon object destruction. Settable
  options are constrained to those found in the input config file.'''

  _instance           = None
  _initialised_status = False
  _changed_status     = False
  _xml_docroot        = None
  _config_file        = None

  # A series of class methods used to store the singleton config data.
  @classmethod
  def _is_initialised(cls, status=None):
    '''Accessor/mutator method indicating if the class has an
    initialised config object.'''
    if status is not None:
      cls._initialised_status = status
    return cls._initialised_status

  @classmethod
  def _is_changed(cls, status=None):
    '''Accessor/mutator method indicating if the class config object
    has been changed at all.'''
    if status is not None:
      cls._changed_status = status
    return cls._changed_status

  @classmethod
  def _config(cls, obj=None):
    '''Accessor/mutator method for setting or retrieving the
    underlying class ET.ElementTree object.'''
    if obj:
      cls._xml_docroot = obj
    return cls._xml_docroot

  @classmethod
  def _conffile(cls, cfg=None):
    '''Accessor/mutator method for setting or retreiving the name of
    the underlying config file.'''
    if cfg:
      cls._config_file = cfg
    return cls._config_file

  def __new__(cls, *args, **kwargs):

    '''The core of the singleton implementation, this method only ever
    creates a single instance.'''

    inst = cls._instance

    if not inst:

      # Passing *args, **kwargs to object.__new__() is apparently
      # deprecated, so we don't.
      inst = super(Config, cls).__new__(cls)

      # Store instance with a weak reference to allow object destruction
      cls._instance = weakref.ref(inst)

    else:
      inst = inst()  # Temporary strong reference for outside use

    return inst

  def __init__(self, conffile=None, force_reload=False):
    '''
    Typically the Config class is instantiated without arguments. The
    conffile and force_reload options are used to control the class
    singleton behaviour during testing.
    '''
    if force_reload:
      self.__dict__.clear()
      self._is_initialised(False)

    if self._is_initialised():
      return

    if conffile is None:
      confname = 'osqpipe_config.xml'
      LOGGER.debug("Looking for config file %s...", confname)
      for loc in os.curdir, os.path.expanduser("~"), "/etc", \
            os.environ.get("OSQPIPE_CONFDIR"):
        if loc is not None:
          source = os.path.join(loc, confname)
          if os.path.exists(source):
            conffile = source
            LOGGER.info("Found config file at %s", conffile)
            break
      if not conffile:
        thisdir  = os.path.dirname( os.path.realpath( __file__ ) )
        conffile = os.path.join(thisdir, 'config', confname)
        LOGGER.warning("Site configuration file not found."
                       + " Falling back to package config %s.", conffile)
    if not os.path.exists(conffile):
      LOGGER.error("Configuration file not found (%s).", conffile)

    config = ET.parse(conffile)

    # Note that currently we assume no name collisions between
    # sections, to ease transition from the database cs_config table
    # to a config file. This may change in future.
    for section in config.getroot().findall('./section'):
      for option in section.findall('./option'):
        if 'name' not in option.attrib:
          raise ET.ParseError("Option tag has no name attribute.")
        key = option.attrib['name']
        if key in self.__dict__.keys():
          raise ET.ParseError(
            "Duplicate option name in config file: %s" % (key,))
        self.__dict__[key] = self._parse_value_elem(option)

    self._config(config)
    self._conffile(conffile)
    self._is_initialised(True)

  def _parse_value_elem(self, value):
    '''
    A recursive method used to extract a config value (dict, list, or
    scalar) of abritrary depth.
    '''
    # First, if the value elem has children, it must be list or dict.
    children = list(value)
    if len(children) > 0:

      # list or dict parsing; recursion necessary.
      if all('name' in e.attrib for e in children):

        # dict
        return dict( (e.attrib['name'], self._parse_value_elem(e))
                     for e in children )

      elif all('name' not in e.attrib for e in children):

        # list
        return [ self._parse_value_elem(e) for e in children ]

      else:
        raise ET.ParseError(
          "Value is ambiguous; neither list nor dict. Values must"
          + " all have 'name' attribute, or must all lack it.")

    # If no children, it must be scalar
    else:
      return value.text

  def _encode_value_elem(self, var, value):
    '''
    The converse of _parse_value_elem. Take the var variable, encode
    it and either insert it as a child of the 'option' Element
    argument (list, dict) or set the option.text attribute (scalar).
    '''
    # We rely on python's default recursion limit of 1000 to catch the
    # effects of circular references here.
    if type(var) in (list, tuple, set):
      for item in var:
        subelem = ET.Element('value')
        value.append(subelem)
        self._encode_value_elem(item, subelem)
    elif type(var) is dict:
      for (key, item) in var.iteritems():
        subelem = ET.Element('value', {'name':key})
        value.append(subelem)
        self._encode_value_elem(item, subelem)
    elif type(var) in (str, unicode, int, float):
      value.text = unicode(var)
    else:
      raise ValueError("Unsupported data type: must be list, tuple,"
                       + " set, dict, str, unicode, int or float")
    return value

  def __getitem__(self, key):
    if key in self.__dict__.keys():
      return self.__dict__[key]
    else:
      LOGGER.debug("Attempt to retrieve unknown config key '%s'", key)
      raise IndexError("Unknown config option '%s'" % (key,))

  def __setitem__(self, key, value):
    self.__setattr__(key, value)

  def __len__(self):
    return len(self.__dict__)

  def __delattr__(self, key):
    raise ValueError("Unsupported config deletion operation attempted.")

  def __delitem__(self, key):
    self.__delattr__(key)

  def __getattr__(self, key):
    # This is a small hack to allow autocomplete within ipython
    # without generating noisy log messages.
    if key in ['__methods__', '__members__',
               'trait_names', '_getAttributeNames']:
      raise AttributeError
    else:
      LOGGER.debug("Attempt to retrieve unknown config key '%s'", key)
      raise AttributeError("Unknown config option '%s'" % (key,))

  def __setattr__(self, key, value):
    if key in self.__dict__ and self.__dict__[key] == value:
      pass # nothing to do here...
    else:

      if type(self.__dict__[key]) != type(value):
        LOGGER.warning("Changing data type of config value '%s'", key)

      self.__dict__[key] = value

      tree   = self._config().getroot()
      query  = "./section/option[@name='%s']" % key
      option = tree.find(query)
      if option is not None:

        # Store the supplied value under this option Element.
        LOGGER.warning("Adding '%s' -> '%s' to permanent"
                       + " configuration data (section %s).",
                       key, value, tree.find(query + '/..').attrib['name'])
        option.clear()
        option.set('name', key)
        self._encode_value_elem(value, option)
        self._is_changed(True)
      else:
        LOGGER.error("Attempt to set config option not found"
                      + " in any section of the config file: %s", key)
        raise AttributeError("Config option not available: %s." % key)
    return

  def __del__(self):
    '''Write the config out to disk upon object destruction (but only
    if it's been altered).'''

    if LOGGER:
      LOGGER.debug("Destroying singleton config object.")

    if self._is_changed():
      if LOGGER:
        LOGGER.debug("Writing out changed options to disk.")
      config   = self._config()
      conffile = self._conffile()
      config.write(conffile, encoding='utf-8', xml_declaration=True)
