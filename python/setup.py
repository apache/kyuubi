#!/usr/bin/env python

from setuptools import setup
import pyhive


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
        'hive-pure-sasl': ['pure-sasl>=0.6.2', 'thrift>=0.10.0', 'thrift_sasl>=0.1.0'],
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
