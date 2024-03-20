"""
This file can be used to generate a new version of the TCLIService package
using a TCLIService.thrift URL.

If no URL is specified, the file for Hive 2.3 will be downloaded.

Usage:

    python generate.py THRIFT_URL

or

    python generate.py
"""
import shutil
import sys
from os import path
from urllib.request import urlopen
import subprocess

here = path.abspath(path.dirname(__file__))

PACKAGE = 'TCLIService'
GENERATED = 'gen-py'

HIVE_SERVER2_URL = \
    'https://raw.githubusercontent.com/apache/hive/branch-2.3/service-rpc/if/TCLIService.thrift'


def save_url(url):
    data = urlopen(url).read()
    file_path = path.join(here, url.rsplit('/', 1)[-1])
    with open(file_path, 'wb') as f:
        f.write(data)


def main(hive_server2_url):
    save_url(hive_server2_url)
    hive_server2_path = path.join(here, hive_server2_url.rsplit('/', 1)[-1])

    subprocess.call(['thrift', '-r', '--gen', 'py', hive_server2_path])
    shutil.move(path.join(here, PACKAGE), path.join(here, PACKAGE + '.old'))
    shutil.move(path.join(here, GENERATED, PACKAGE), path.join(here, PACKAGE))
    shutil.rmtree(path.join(here, PACKAGE + '.old'))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = HIVE_SERVER2_URL
    main(url)
