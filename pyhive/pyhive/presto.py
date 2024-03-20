"""DB-API implementation backed by Presto

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

from builtins import object
from decimal import Decimal

from pyhive import common
from pyhive.common import DBAPITypeObject
# Make all exceptions visible in this module per DB-API
from pyhive.exc import *  # noqa
import base64
import getpass
import datetime
import logging
import requests
from requests.auth import HTTPBasicAuth
import os

try:  # Python 3
    import urllib.parse as urlparse
except ImportError:  # Python 2
    import urlparse


# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s

_logger = logging.getLogger(__name__)

TYPES_CONVERTER = {
    "decimal": Decimal,
    # As of Presto 0.69, binary data is returned as the varbinary type in base64 format
    "varbinary": base64.b64decode
}

class PrestoParamEscaper(common.ParamEscaper):
    def escape_datetime(self, item, format):
        _type = "timestamp" if isinstance(item, datetime.datetime) else "date"
        formatted = super(PrestoParamEscaper, self).escape_datetime(item, format, 3)
        return "{} {}".format(_type, formatted)


_escaper = PrestoParamEscaper()


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database. See class :py:class:`Connection` for
    arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    """Presto does not have a notion of a persistent connection.

    Thus, these objects are small stateless factories for cursors, which do all the real work.
    """

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def close(self):
        """Presto does not have anything to close"""
        # TODO cancel outstanding queries?
        pass

    def commit(self):
        """Presto does not support transactions"""
        pass

    def cursor(self):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(*self._args, **self._kwargs)

    def rollback(self):
        raise NotSupportedError("Presto does not have transactions")  # pragma: no cover


class Cursor(common.DBAPICursor):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """

    def __init__(self, host, port='8080', username=None, principal_username=None, catalog='hive',
                 schema='default', poll_interval=1, source='pyhive', session_props=None,
                 protocol='http', password=None, requests_session=None, requests_kwargs=None,
                 KerberosRemoteServiceName=None, KerberosPrincipal=None,
                 KerberosConfigPath=None, KerberosKeytabPath=None,
                 KerberosCredentialCachePath=None, KerberosUseCanonicalHostname=None):
        """
        :param host: hostname to connect to, e.g. ``presto.example.com``
        :param port: int -- port, defaults to 8080
        :param username: string -- defaults to system user name
        :param principal_username: string -- defaults to ``username`` argument if it exists,
            else defaults to system user name
        :param catalog: string -- defaults to ``hive``
        :param schema: string -- defaults to ``default``
        :param poll_interval: float -- how often to ask the Presto REST interface for a progress
            update, defaults to a second
        :param source: string -- arbitrary identifier (shows up in the Presto monitoring page)
        :param protocol: string -- network protocol, valid options are ``http`` and ``https``.
            defaults to ``http``
        :param password: string -- Deprecated. Defaults to ``None``.
            Using BasicAuth, requires ``https``.
            Prefer ``requests_kwargs={'auth': HTTPBasicAuth(username, password)}``.
            May not be specified with ``requests_kwargs['auth']``.
        :param requests_session: a ``requests.Session`` object for advanced usage. If absent, this
            class will use the default requests behavior of making a new session per HTTP request.
            Caller is responsible for closing session.
        :param requests_kwargs: Additional ``**kwargs`` to pass to requests
        :param KerberosRemoteServiceName: string -- Presto coordinator Kerberos service name.
            This parameter is required for Kerberos authentiation.
        :param KerberosPrincipal: string -- The principal to use when authenticating to
            the Presto coordinator.
        :param KerberosConfigPath: string -- Kerberos configuration file.
            (default: /etc/krb5.conf)
        :param KerberosKeytabPath: string -- Kerberos keytab file.
        :param KerberosCredentialCachePath: string -- Kerberos credential cache.
        :param KerberosUseCanonicalHostname: boolean -- Use the canonical hostname of the
            Presto coordinator for the Kerberos service principal by first resolving the
            hostname to an IP address and then doing a reverse DNS lookup for that IP address.
            This is enabled by default.
        """
        super(Cursor, self).__init__(poll_interval)
        # Config
        self._host = host
        self._port = port
        """
        Presto User Impersonation: https://docs.starburstdata.com/latest/security/impersonation.html

        User impersonation allows the execution of queries in Presto based on principal_username
        argument, instead of executing the query as the account which authenticated against Presto.
        (Usually a service account)

        Allows for a service account to authenticate with Presto, and then leverage the
        principal_username as the user Presto will execute the query as. This is required by
        applications that leverage authentication methods like SAML, where the application has a
        username, but not a password to still leverage user specific Presto Resource Groups and
        Authorization rules that would not be applied when only using a shared service account.
        This also allows auditing of who is executing a query in these environments, instead of
        having all queryes run by the shared service account.
        """
        self._username = principal_username or username or getpass.getuser()
        self._catalog = catalog
        self._schema = schema
        self._arraysize = 1
        self._poll_interval = poll_interval
        self._source = source
        self._session_props = session_props if session_props is not None else {}
        self.last_query_id = None

        if protocol not in ('http', 'https'):
            raise ValueError("Protocol must be http/https, was {!r}".format(protocol))
        self._protocol = protocol

        self._requests_session = requests_session or requests

        requests_kwargs = dict(requests_kwargs) if requests_kwargs is not None else {}

        if KerberosRemoteServiceName is not None:
            from requests_kerberos import HTTPKerberosAuth, OPTIONAL

            hostname_override = None
            if KerberosUseCanonicalHostname is not None \
                    and KerberosUseCanonicalHostname.lower() == 'false':
                hostname_override = host
            if KerberosConfigPath is not None:
                os.environ['KRB5_CONFIG'] = KerberosConfigPath
            if KerberosKeytabPath is not None:
                os.environ['KRB5_CLIENT_KTNAME'] = KerberosKeytabPath
            if KerberosCredentialCachePath is not None:
                os.environ['KRB5CCNAME'] = KerberosCredentialCachePath

            requests_kwargs['auth'] = HTTPKerberosAuth(mutual_authentication=OPTIONAL,
                                                       principal=KerberosPrincipal,
                                                       service=KerberosRemoteServiceName,
                                                       hostname_override=hostname_override)

        else:
            if password is not None and 'auth' in requests_kwargs:
                raise ValueError("Cannot use both password and requests_kwargs authentication")
            for k in ('method', 'url', 'data', 'headers'):
                if k in requests_kwargs:
                    raise ValueError("Cannot override requests argument {}".format(k))
            if password is not None:
                requests_kwargs['auth'] = HTTPBasicAuth(username, password)
                if protocol != 'https':
                    raise ValueError("Protocol must be https when passing a password")
        self._requests_kwargs = requests_kwargs

        self._reset_state()

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        super(Cursor, self)._reset_state()
        self._nextUri = None
        self._columns = None

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        # Sleep until we're done or we got the columns
        self._fetch_while(
            lambda: self._columns is None and
            self._state not in (self._STATE_NONE, self._STATE_FINISHED)
        )
        if self._columns is None:
            return None
        return [
            # name, type_code, display_size, internal_size, precision, scale, null_ok
            (col['name'], col['type'], None, None, None, None, True)
            for col in self._columns
        ]

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """
        headers = {
            'X-Presto-Catalog': self._catalog,
            'X-Presto-Schema': self._schema,
            'X-Presto-Source': self._source,
            'X-Presto-User': self._username,
        }

        if self._session_props:
            headers['X-Presto-Session'] = ','.join(
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

    def cancel(self):
        if self._state == self._STATE_NONE:
            raise ProgrammingError("No query yet")
        if self._nextUri is None:
            assert self._state == self._STATE_FINISHED, "Should be finished if nextUri is None"
            return

        response = self._requests_session.delete(self._nextUri, **self._requests_kwargs)
        if response.status_code != requests.codes.no_content:
            fmt = "Unexpected status code after cancel {}\n{}"
            raise OperationalError(fmt.format(response.status_code, response.content))

        self._state = self._STATE_FINISHED
        self._nextUri = None

    def poll(self):
        """Poll for and return the raw status data provided by the Presto REST API.

        :returns: dict -- JSON status information or ``None`` if the query is done
        :raises: ``ProgrammingError`` when no query has been started

        .. note::
            This is not a part of DB-API.
        """
        if self._state == self._STATE_NONE:
            raise ProgrammingError("No query yet")
        if self._nextUri is None:
            assert self._state == self._STATE_FINISHED, "Should be finished if nextUri is None"
            return None
        response = self._requests_session.get(self._nextUri, **self._requests_kwargs)
        self._process_response(response)
        return response.json()

    def _fetch_more(self):
        """Fetch the next URI and update state"""
        self._process_response(self._requests_session.get(self._nextUri, **self._requests_kwargs))

    def _process_data(self, rows):
        for i, col in enumerate(self.description):
            col_type = col[1].split("(")[0].lower()
            if col_type in TYPES_CONVERTER:
                for row in rows:
                    if row[i] is not None:
                        row[i] = TYPES_CONVERTER[col_type](row[i])

    def _process_response(self, response):
        """Given the JSON response from Presto's REST API, update the internal state with the next
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
        if 'X-Presto-Clear-Session' in response.headers:
            propname = response.headers['X-Presto-Clear-Session']
            self._session_props.pop(propname, None)
        if 'X-Presto-Set-Session' in response.headers:
            propname, propval = response.headers['X-Presto-Set-Session'].split('=', 1)
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


# See types in presto-main/src/main/java/com/facebook/presto/tuple/TupleInfo.java
FIXED_INT_64 = DBAPITypeObject(['bigint'])
VARIABLE_BINARY = DBAPITypeObject(['varchar'])
DOUBLE = DBAPITypeObject(['double'])
BOOLEAN = DBAPITypeObject(['boolean'])
