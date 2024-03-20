"""DB-API implementation backed by Trino

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import requests

# Make all exceptions visible in this module per DB-API
from pyhive.common import DBAPITypeObject
from pyhive.exc import *  # noqa
from pyhive.presto import Connection as PrestoConnection, Cursor as PrestoCursor, PrestoParamEscaper

try:  # Python 3
    import urllib.parse as urlparse
except ImportError:  # Python 2
    import urlparse

# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s

_logger = logging.getLogger(__name__)


class TrinoParamEscaper(PrestoParamEscaper):
    pass


_escaper = TrinoParamEscaper()


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database. See class :py:class:`Connection` for
    arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection(PrestoConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def cursor(self):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(*self._args, **self._kwargs)


class Cursor(PrestoCursor):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """
        headers = {
            'X-Trino-Catalog': self._catalog,
            'X-Trino-Schema': self._schema,
            'X-Trino-Source': self._source,
            'X-Trino-User': self._username,
        }

        if self._session_props:
            headers['X-Trino-Session'] = ','.join(
                '{}={}'.format(propname, propval)
                for propname, propval in self._session_props.items()
            )

        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._reset_state()

        self._state = self._STATE_RUNNING
        url = urlparse.urlunparse((
            self._protocol,
            '{}:{}'.format(self._host, self._port), '/v1/statement', None, None, None))
        _logger.info('%s', sql)
        _logger.debug("Headers: %s", headers)
        response = self._requests_session.post(
            url, data=sql.encode('utf-8'), headers=headers, **self._requests_kwargs)
        self._process_response(response)

    def _process_response(self, response):
        """Given the JSON response from Trino's REST API, update the internal state with the next
        URI and any data from the response
        """
        # TODO handle HTTP 503
        if response.status_code != requests.codes.ok:
            fmt = "Unexpected status code {}\n{}"
            raise OperationalError(fmt.format(response.status_code, response.content))

        response_json = response.json()
        _logger.debug("Got response %s", response_json)
        assert self._state == self._STATE_RUNNING, "Should be running if processing response"
        self._nextUri = response_json.get('nextUri')
        self._columns = response_json.get('columns')
        if 'id' in response_json:
            self.last_query_id = response_json['id']
        if 'X-Trino-Clear-Session' in response.headers:
            propname = response.headers['X-Trino-Clear-Session']
            self._session_props.pop(propname, None)
        if 'X-Trino-Set-Session' in response.headers:
            propname, propval = response.headers['X-Trino-Set-Session'].split('=', 1)
            self._session_props[propname] = propval
        if 'data' in response_json:
            assert self._columns
            new_data = response_json['data']
            self._process_data(new_data)
            self._data += map(tuple, new_data)
        if 'nextUri' not in response_json:
            self._state = self._STATE_FINISHED
        if 'error' in response_json:
            raise DatabaseError(response_json['error'])


#
# Type Objects and Constructors
#


# See types in trino-main/src/main/java/com/facebook/trino/tuple/TupleInfo.java
FIXED_INT_64 = DBAPITypeObject(['bigint'])
VARIABLE_BINARY = DBAPITypeObject(['varchar'])
DOUBLE = DBAPITypeObject(['double'])
BOOLEAN = DBAPITypeObject(['boolean'])
