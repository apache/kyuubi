"""Hive integration tests.

These rely on having a Hive+Hadoop cluster set up with HiveServer2 running.
They also require a tables created by make_test_tables.sh.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import datetime
import os
import socket
import subprocess
import time
import unittest
from decimal import Decimal
import ssl

import mock
import pytest
import thrift.transport.TSocket
import thrift.transport.TTransport
import thrift_sasl
from thrift.transport.TTransport import TTransportException

from TCLIService import ttypes
from pyhive import hive
from pyhive.tests.dbapi_test_case import DBAPITestCase
from pyhive.tests.dbapi_test_case import with_cursor

_HOST = 'localhost'


class TestHive(unittest.TestCase, DBAPITestCase):
    __test__ = True

    def connect(self):
        return hive.connect(host=_HOST, port=10000, configuration={'mapred.job.tracker': 'local'})

    @with_cursor
    def test_description(self, cursor):
        cursor.execute('SELECT * FROM one_row')

        desc = [('one_row.number_of_rows', 'INT_TYPE', None, None, None, None, True)]
        self.assertEqual(cursor.description, desc)

    @with_cursor
    def test_complex(self, cursor):
        cursor.execute('SELECT * FROM one_row_complex')
        self.assertEqual(cursor.description, [
            ('one_row_complex.boolean', 'BOOLEAN_TYPE', None, None, None, None, True),
            ('one_row_complex.tinyint', 'TINYINT_TYPE', None, None, None, None, True),
            ('one_row_complex.smallint', 'SMALLINT_TYPE', None, None, None, None, True),
            ('one_row_complex.int', 'INT_TYPE', None, None, None, None, True),
            ('one_row_complex.bigint', 'BIGINT_TYPE', None, None, None, None, True),
            ('one_row_complex.float', 'FLOAT_TYPE', None, None, None, None, True),
            ('one_row_complex.double', 'DOUBLE_TYPE', None, None, None, None, True),
            ('one_row_complex.string', 'STRING_TYPE', None, None, None, None, True),
            ('one_row_complex.timestamp', 'TIMESTAMP_TYPE', None, None, None, None, True),
            ('one_row_complex.binary', 'BINARY_TYPE', None, None, None, None, True),
            ('one_row_complex.array', 'ARRAY_TYPE', None, None, None, None, True),
            ('one_row_complex.map', 'MAP_TYPE', None, None, None, None, True),
            ('one_row_complex.struct', 'STRUCT_TYPE', None, None, None, None, True),
            ('one_row_complex.union', 'UNION_TYPE', None, None, None, None, True),
            ('one_row_complex.decimal', 'DECIMAL_TYPE', None, None, None, None, True),
        ])
        rows = cursor.fetchall()
        expected = [(
            True,
            127,
            32767,
            2147483647,
            9223372036854775807,
            0.5,
            0.25,
            'a string',
            datetime.datetime(1970, 1, 1, 0, 0),
            b'123',
            '[1,2]',
            '{1:2,3:4}',
            '{"a":1,"b":2}',
            '{0:1}',
            Decimal('0.1'),
        )]
        self.assertEqual(rows, expected)
        # catch unicode/str
        self.assertEqual(list(map(type, rows[0])), list(map(type, expected[0])))

    @with_cursor
    def test_async(self, cursor):
        cursor.execute('SELECT * FROM one_row', async_=True)
        unfinished_states = (
            ttypes.TOperationState.INITIALIZED_STATE,
            ttypes.TOperationState.RUNNING_STATE,
        )
        while cursor.poll().operationState in unfinished_states:
            cursor.fetch_logs()
        assert cursor.poll().operationState == ttypes.TOperationState.FINISHED_STATE

        self.assertEqual(len(cursor.fetchall()), 1)

    @with_cursor
    def test_cancel(self, cursor):
        # Need to do a JOIN to force a MR job. Without it, Hive optimizes the query to a fetch
        # operator and prematurely declares the query done.
        cursor.execute(
            "SELECT reflect('java.lang.Thread', 'sleep', 1000L * 1000L * 1000L) "
            "FROM one_row a JOIN one_row b",
            async_=True
        )
        self.assertEqual(cursor.poll().operationState, ttypes.TOperationState.RUNNING_STATE)
        cursor.cancel()
        self.assertEqual(cursor.poll().operationState, ttypes.TOperationState.CANCELED_STATE)

    def test_noops(self):
        """The DB-API specification requires that certain actions exist, even though they might not
        be applicable."""
        # Wohoo inflating coverage stats!
        with contextlib.closing(self.connect()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                self.assertEqual(cursor.rowcount, -1)
                cursor.setinputsizes([])
                cursor.setoutputsize(1, 'blah')
                connection.commit()

    @mock.patch('TCLIService.TCLIService.Client.OpenSession')
    def test_open_failed(self, open_session):
        open_session.return_value.serverProtocolVersion = \
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1
        self.assertRaises(hive.OperationalError, self.connect)

    def test_escape(self):
        # Hive thrift translates newlines into multiple rows. WTF.
        bad_str = '''`~!@#$%^&*()_+-={}[]|\\;:'",./<>?\t '''
        self.run_escape_case(bad_str)

    @pytest.mark.skip(reason="Currently failing")
    def test_newlines(self):
        """Verify that newlines are passed through correctly"""
        cursor = self.connect().cursor()
        orig = ' \r\n \r \n '
        cursor.execute(
            'SELECT %s FROM one_row',
            (orig,)
        )
        result = cursor.fetchall()
        self.assertEqual(result, [(orig,)])

    @with_cursor
    def test_no_result_set(self, cursor):
        cursor.execute('USE default')
        self.assertIsNone(cursor.description)
        self.assertRaises(hive.ProgrammingError, cursor.fetchone)

    @pytest.mark.skip(reason="Need a proper setup for ldap")
    def test_ldap_connection(self):
        rootdir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        orig_ldap = os.path.join(rootdir, 'scripts', 'conf', 'hive', 'hive-site-ldap.xml')
        orig_none = os.path.join(rootdir, 'scripts', 'conf', 'hive', 'hive-site.xml')
        des = os.path.join('/', 'etc', 'hive', 'conf', 'hive-site.xml')
        try:
            subprocess.check_call(['sudo', 'cp', orig_ldap, des])
            _restart_hs2()
            with contextlib.closing(hive.connect(
                    host=_HOST, username='existing', auth='LDAP', password='testpw')
            ) as connection:
                with contextlib.closing(connection.cursor()) as cursor:
                    cursor.execute('SELECT * FROM one_row')
                    self.assertEqual(cursor.fetchall(), [(1,)])

            self.assertRaisesRegexp(
                TTransportException, 'Error validating the login',
                lambda: hive.connect(
                    host=_HOST, username='existing', auth='LDAP', password='wrong')
            )

        finally:
            subprocess.check_call(['sudo', 'cp', orig_none, des])
            _restart_hs2()

    def test_invalid_ldap_config(self):
        """password should be set if and only if using LDAP"""
        self.assertRaisesRegexp(ValueError, 'Password.*LDAP',
                                lambda: hive.connect(_HOST, password=''))
        self.assertRaisesRegexp(ValueError, 'Password.*LDAP',
                                lambda: hive.connect(_HOST, auth='LDAP'))

    def test_invalid_kerberos_config(self):
        """kerberos_service_name should be set if and only if using KERBEROS"""
        self.assertRaisesRegexp(ValueError, 'kerberos_service_name.*KERBEROS',
                                lambda: hive.connect(_HOST, kerberos_service_name=''))
        self.assertRaisesRegexp(ValueError, 'kerberos_service_name.*KERBEROS',
                                lambda: hive.connect(_HOST, auth='KERBEROS'))

    def test_invalid_transport(self):
        """transport and auth are incompatible"""
        socket = thrift.transport.TSocket.TSocket('localhost', 10000)
        transport = thrift.transport.TTransport.TBufferedTransport(socket)
        self.assertRaisesRegexp(
            ValueError, 'thrift_transport cannot be used with',
            lambda: hive.connect(_HOST, thrift_transport=transport)
        )

    def test_custom_transport(self):
        socket = thrift.transport.TSocket.TSocket('localhost', 10000)
        sasl_auth = 'PLAIN'

        transport = thrift_sasl.TSaslClientTransport(lambda: hive.get_installed_sasl(host='localhost', sasl_auth=sasl_auth, username='test_username', password='x'), sasl_auth, socket)
        conn = hive.connect(thrift_transport=transport)
        with contextlib.closing(conn):
            with contextlib.closing(conn.cursor()) as cursor:
                cursor.execute('SELECT * FROM one_row')
                self.assertEqual(cursor.fetchall(), [(1,)])
    
    @pytest.mark.skip(reason="Need a proper setup for custom auth")
    def test_custom_connection(self):
        rootdir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        orig_ldap = os.path.join(rootdir, 'scripts', 'conf', 'hive', 'hive-site-custom.xml')
        orig_none = os.path.join(rootdir, 'scripts', 'conf', 'hive', 'hive-site.xml')
        des = os.path.join('/', 'etc', 'hive', 'conf', 'hive-site.xml')
        try:
            subprocess.check_call(['sudo', 'cp', orig_ldap, des])
            _restart_hs2()
            with contextlib.closing(hive.connect(
                    host=_HOST, username='the-user', auth='CUSTOM', password='p4ssw0rd')
            ) as connection:
                with contextlib.closing(connection.cursor()) as cursor:
                    cursor.execute('SELECT * FROM one_row')
                    self.assertEqual(cursor.fetchall(), [(1,)])

            self.assertRaisesRegexp(
                TTransportException, 'Error validating the login',
                lambda: hive.connect(
                    host=_HOST, username='the-user', auth='CUSTOM', password='wrong')
            )

        finally:
            subprocess.check_call(['sudo', 'cp', orig_none, des])
            _restart_hs2()

    @pytest.mark.skip(reason="Need a proper setup for SSL context testing")
    def test_basic_ssl_context(self):
        """Test that connection works with a custom SSL context that mimics the default behavior."""
        # Create an SSL context similar to what Connection creates by default
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Connect using the same parameters as self.connect() but with our custom context
        with contextlib.closing(hive.connect(
            host=_HOST,
            port=10000,
            configuration={'mapred.job.tracker': 'local'},
            ssl_context=ssl_context
        )) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                # Use the same query pattern as other tests
                cursor.execute('SELECT 1 FROM one_row')
                self.assertEqual(cursor.fetchall(), [(1,)])

    def test_connection_timeout(self):
        """Test that a connection timeout is set without error."""
        with contextlib.closing(hive.connect(
            host=_HOST,
            port=10000,
            connection_timeout=10 * 1000
        )) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                # Use the same query pattern as other tests
                cursor.execute('SELECT 1 FROM one_row')
                self.assertEqual(cursor.fetchall(), [(1,)])

def _restart_hs2():
    subprocess.check_call(['sudo', 'service', 'hive-server2', 'restart'])
    with contextlib.closing(socket.socket()) as s:
        while s.connect_ex(('localhost', 10000)) != 0:
            time.sleep(1)
