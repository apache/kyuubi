#!/usr/bin/env python

from setuptools import setup
from setuptools.command.test import test as TestCommand
import pyhive
import sys


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


with open('README.rst') as readme:
    long_description = readme.read()

setup(
    name="PyHive",
    version=pyhive.__version__,
    description="Python interface to Hive",
    long_description=long_description,
    url='https://github.com/dropbox/PyHive',
    author="Jing Wang",
    author_email="jing@dropbox.com",
    license="Apache License, Version 2.0",
    packages=['pyhive', 'TCLIService'],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
    ],
    install_requires=[
        'future',
        'python-dateutil',
    ],
    extras_require={
        'presto': ['requests>=1.0.0'],
        'trino': ['requests>=1.0.0'],
        'hive': ['sasl>=0.2.1', 'thrift>=0.10.0', 'thrift_sasl>=0.1.0'],
        'hive_pure_sasl': ['pure-sasl>=0.6.2', 'thrift>=0.10.0', 'thrift_sasl>=0.1.0'],
        'sqlalchemy': ['sqlalchemy>=1.3.0'],
        'kerberos': ['requests_kerberos>=0.12.0'],
    },
    tests_require=[
        'mock>=1.0.0',
        'pytest',
        'pytest-cov',
        'requests>=1.0.0',
        'requests_kerberos>=0.12.0',
        'sasl>=0.2.1',
        'pure-sasl>=0.6.2',
        'kerberos>=1.3.0',
        'sqlalchemy>=1.3.0',
        'thrift>=0.10.0',
    ],
    cmdclass={'test': PyTest},
    package_data={
        '': ['*.rst'],
    },
    entry_points={
        'sqlalchemy.dialects': [
            'hive = pyhive.sqlalchemy_hive:HiveDialect',
            "hive.http = pyhive.sqlalchemy_hive:HiveHTTPDialect",
            "hive.https = pyhive.sqlalchemy_hive:HiveHTTPSDialect",
            'presto = pyhive.sqlalchemy_presto:PrestoDialect',
            'trino.pyhive = pyhive.sqlalchemy_trino:TrinoDialect',
        ],
    }
)
