================================
Project is currently unsupported
================================




.. image:: https://travis-ci.org/dropbox/PyHive.svg?branch=master
    :target: https://travis-ci.org/dropbox/PyHive
.. image:: https://img.shields.io/codecov/c/github/dropbox/PyHive.svg

======
PyHive
======

PyHive is a collection of Python `DB-API <http://www.python.org/dev/peps/pep-0249/>`_ and
`SQLAlchemy <http://www.sqlalchemy.org/>`_ interfaces for `Presto <http://prestodb.io/>`_ ,
`Hive <http://hive.apache.org/>`_ and `Trino <https://trino.io/>`_.

Usage
=====

DB-API
------
.. code-block:: python

    from pyhive import presto  # or import hive or import trino
    cursor = presto.connect('localhost').cursor()  # or use hive.connect or use trino.connect
    cursor.execute('SELECT * FROM my_awesome_data LIMIT 10')
    print cursor.fetchone()
    print cursor.fetchall()

DB-API (asynchronous)
---------------------
.. code-block:: python

    from pyhive import hive
    from TCLIService.ttypes import TOperationState
    cursor = hive.connect('localhost').cursor()
    cursor.execute('SELECT * FROM my_awesome_data LIMIT 10', async=True)

    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        for message in logs:
            print message

        # If needed, an asynchronous query can be cancelled at any time with:
        # cursor.cancel()

        status = cursor.poll().operationState

    print cursor.fetchall()

In Python 3.7 `async` became a keyword; you can use `async_` instead:

.. code-block:: python

    cursor.execute('SELECT * FROM my_awesome_data LIMIT 10', async_=True)


SQLAlchemy
----------
First install this package to register it with SQLAlchemy, see ``entry_points`` in ``setup.py``.

.. code-block:: python

    from sqlalchemy import *
    from sqlalchemy.engine import create_engine
    from sqlalchemy.schema import *
    # Presto
    engine = create_engine('presto://localhost:8080/hive/default')
    # Trino
    engine = create_engine('trino+pyhive://localhost:8080/hive/default')
    # Hive
    engine = create_engine('hive://localhost:10000/default')

    # SQLAlchemy < 2.0
    logs = Table('my_awesome_data', MetaData(bind=engine), autoload=True)
    print select([func.count('*')], from_obj=logs).scalar()

    # Hive + HTTPS + LDAP or basic Auth
    engine = create_engine('hive+https://username:password@localhost:10000/')
    logs = Table('my_awesome_data', MetaData(bind=engine), autoload=True)
    print select([func.count('*')], from_obj=logs).scalar()

    # SQLAlchemy >= 2.0
    metadata_obj = MetaData()
    books = Table("books", metadata_obj, Column("id", Integer), Column("title", String), Column("primary_author", String))
    metadata_obj.create_all(engine)
    inspector = inspect(engine)
    inspector.get_columns('books')

    with engine.connect() as con:
        data = [{ "id": 1, "title": "The Hobbit", "primary_author": "Tolkien" }, 
                { "id": 2, "title": "The Silmarillion", "primary_author": "Tolkien" }]
        con.execute(books.insert(), data[0])
        result = con.execute(text("select * from books"))
        print(result.fetchall())

Note: query generation functionality is not exhaustive or fully tested, but there should be no
problem with raw SQL.

Passing session configuration
-----------------------------

.. code-block:: python

    # DB-API
    hive.connect('localhost', configuration={'hive.exec.reducers.max': '123'})
    presto.connect('localhost', session_props={'query_max_run_time': '1234m'})
    trino.connect('localhost',  session_props={'query_max_run_time': '1234m'})
    # SQLAlchemy
    create_engine(
        'presto://user@host:443/hive',
        connect_args={'protocol': 'https',
                      'session_props': {'query_max_run_time': '1234m'}}
    )
    create_engine(
        'trino+pyhive://user@host:443/hive',
        connect_args={'protocol': 'https',
                      'session_props': {'query_max_run_time': '1234m'}}
    )
    create_engine(
        'hive://user@host:10000/database',
        connect_args={'configuration': {'hive.exec.reducers.max': '123'}},
    )
    # SQLAlchemy with LDAP
    create_engine(
        'hive://user:password@host:10000/database',
        connect_args={'auth': 'LDAP'},
    )

Requirements
============

Install using

- ``pip install 'pyhive[hive]'`` or ``pip install 'pyhive[hive_pure_sasl]'`` for the Hive interface
- ``pip install 'pyhive[presto]'`` for the Presto interface
- ``pip install 'pyhive[trino]'`` for the Trino interface

Note: ``'pyhive[hive]'`` extras uses `sasl <https://pypi.org/project/sasl/>`_ that doesn't support Python 3.11, See `github issue <https://github.com/cloudera/python-sasl/issues/30>`_.
Hence PyHive also supports `pure-sasl <https://pypi.org/project/pure-sasl/>`_ via additional extras ``'pyhive[hive_pure_sasl]'`` which support Python 3.11.

PyHive works with

- Python 2.7 / Python 3
- For Presto: `Presto installation <https://prestodb.io/docs/current/installation.html>`_
- For Trino: `Trino installation <https://trino.io/docs/current/installation.html>`_
- For Hive: `HiveServer2 <https://cwiki.apache.org/confluence/display/Hive/Setting+up+HiveServer2>`_ daemon

Changelog
=========
See https://github.com/dropbox/PyHive/releases.

Contributing
============
- Please fill out the Dropbox Contributor License Agreement at https://opensource.dropbox.com/cla/ and note this in your pull request.
- Changes must come with tests, with the exception of trivial things like fixing comments. See .travis.yml for the test environment setup.
- Notes on project scope:

  - This project is intended to be a minimal Hive/Presto client that does that one thing and nothing else.
    Features that can be implemented on top of PyHive, such integration with your favorite data analysis library, are likely out of scope.
  - We prefer having a small number of generic features over a large number of specialized, inflexible features.
    For example, the Presto code takes an arbitrary ``requests_session`` argument for customizing HTTP calls, as opposed to having a separate parameter/branch for each ``requests`` option.

Tips for test environment setup
================================
You can setup test environment by following ``.travis.yaml`` in this repository. It uses `Cloudera's CDH 5 <https://docs.cloudera.com/documentation/enterprise/release-notes/topics/cdh_vd_cdh_download_510.html>`_ which requires username and password for download.
It may not be feasible for everyone to get those credentials. Hence below are alternative instructions to setup test environment.

You can clone `this repository <https://github.com/big-data-europe/docker-hive/blob/master/docker-compose.yml>`_ which has Docker Compose setup for Presto and Hive.
You can add below lines to its docker-compose.yaml to start Trino in same environment::
 
    trino:
        image: trinodb/trino:351    
        ports:     
            - "18080:18080"    
        volumes:    
            - ./trino:/etc/trino

Note: ``./trino`` for docker volume defined above is `trino config from PyHive repository <https://github.com/dropbox/PyHive/tree/master/scripts/travis-conf/trino>`_

Then run::
    docker-compose up -d

Testing
=======
.. image:: https://travis-ci.org/dropbox/PyHive.svg
    :target: https://travis-ci.org/dropbox/PyHive
.. image:: http://codecov.io/github/dropbox/PyHive/coverage.svg?branch=master
    :target: http://codecov.io/github/dropbox/PyHive?branch=master

Run the following in an environment with Hive/Presto::

    ./scripts/make_test_tables.sh
    virtualenv --no-site-packages env
    source env/bin/activate
    pip install -e .
    pip install -r dev_requirements.txt
    py.test

WARNING: This drops/creates tables named ``one_row``, ``one_row_complex``, and ``many_rows``, plus a
database called ``pyhive_test_database``.

Updating TCLIService
====================

The TCLIService module is autogenerated using a ``TCLIService.thrift`` file. To update it, the
``generate.py`` file can be used: ``python generate.py <TCLIServiceURL>``. When left blank, the
version for Hive 2.3 will be downloaded.
