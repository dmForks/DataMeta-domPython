#!/bin/env python
from __future__ import print_function
from setuptools import setup
from setuptools.command.test import test as TestCommand
import codecs
import os
import sys
import re
# to run tests:  ./scan_bigtable.py export PYTHONPATH=/aw/gits/ghub/googleCloudPlatform/gcloud-python/_testing

HERE = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    """Return multiple read calls to different readable objects as a single
    string."""
    # intentionally *not* adding an encoding option to open
    return codecs.open(os.path.join(HERE, *parts), 'r').read()

LONG_DESCRIPTION = read('README.txt')

# def find_version(*file_paths):
#     """Find the "__version__" string in files on *file_path*."""
#     version_file = read(*file_paths)
#     version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
#                               version_file, re.M)
#     if version_match:
#         return version_match.group(1)
#     raise RuntimeError("Unable to find version string.")


# noinspection PyAttributeOutsideInit
class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = [
            '--strict',
            '--verbose',
            '--tb=long',
            'tests']
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)

setup(
    name='ebay-datameta-core',
    version="1.0.1",
    url='https://github.com/eBayDataMeta/DataMeta',
    license='Apache-2.0',
    author='Michael Bergens',
    tests_require=['pytest'],

    install_requires=['bitarray>=0.8.1'
                    ], # https://github.com/tracelytics/python-hadoop/blob/master/setup.py

    cmdclass={'test': PyTest},
    author_email='michael.bergens@gmail.com',
    description='DataMeta DOM for Python',
    long_description = LONG_DESCRIPTION,
    zip_safe=False,
    packages=['ebay_datameta_core'],
    include_package_data=True,
    platforms='any',
    test_suite='tests.test_core',
    classifiers = [
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Natural Language :: English',
        'Environment :: Web Environment',
        'Intended Audience :: Data Scientists',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        ],
    extras_require={
        'testing': ['pytest'],
    }
)
