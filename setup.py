import os

from setuptools import setup, find_packages
from codecs import open


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md'), 'r') as f:
    long_description = f.read()

metadata = {\
    'name'              : 'Google2Pandas',
    'version'           : '0.2.1',
    'author'            : 'Matt S.',
    'author_email'      : 'sales@panalysis.com',
    'maintainer'        : 'Matt S.',
    'maintainer_email'  : 'sales@panalysis.com',
    'url'               : 'https://github.com/panalysis/Google2Pandas',
    'download_url'	    : 'https://github.com/DelciousHair/google2pandas/archive/0.1.0.tar.gz',
    'description'       : 'Google2Pandas',
    'long_description_content_type' : 'text/markdown',
    'long_description'  : long_description,
    'classifiers'       : [
                            'Development Status :: 4 - Beta',
                            'Programming Language :: Python',
                            'Programming Language :: Python :: 3',
                            'Intended Audience :: Science/Research',
                            'Topic :: Scientific/Engineering',
                            'Operating System :: OS Independent'],
    'license'           : 'MIT LICENCE',
    'install_requires'  : [
                            'numpy>=1.19',
                            'pandas>=1.1',
                            'google-api-python-client',
                            'httplib2',
                            'oauth2client'],
    'packages'          : find_packages()}

setup(**metadata)
