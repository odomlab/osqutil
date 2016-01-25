import os
import re

import multiprocessing # workaround for Python bug. See http://bugs.python.org/issue15881#msg170215

from setuptools import setup, find_packages

README  = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()
SCRIPTS = [ os.path.join('bin', x) for x in os.listdir('bin') if re.search('\.py$', x) ]

# I'm not planning on installing the util/*.py scripts by default.

# Allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='osqutil',
    version='0.1.1',
    packages=find_packages(),
    include_package_data=True,
    license='GPLv3 License',
    description='Basic utility code for logging, config, manipulating bam files etc. as used by the Odom lab.',
    long_description=README,
    url='http://openwetware.org/wiki/Odom_Lab',
    author='Tim Rayner',
    author_email='tim.rayner@cruk.cam.ac.uk',
    classifiers=[
        'Environment :: Command Line',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GPLv3 License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    test_suite='nose.collector',
    scripts=SCRIPTS,
  install_requires=[
# Do not add anything here! This package is supposed to rely on core python modules only.
  ],
  zip_safe=False,  # Prevents zipping of the installed egg, for accessing config defaults.
)
