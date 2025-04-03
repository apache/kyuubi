.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

Configure Kyuubi to use Kerberos Authentication
===============================================

If you are deploying Kyuubi with a kerberized Hadoop cluster, it is strongly
recommended that ``kyuubi.authentication`` should be set to `KERBEROS` too.

Kerberos Overview
-----------------

Kerberos is a network authentication protocol that provides the tools of
authentication and strong cryptography over the network.
The Kerberos protocol uses strong cryptography so that a client or a server
can prove its identity to its server or client across an insecure network connection.
After a client and server have used Kerberos to prove their identity, they can
also encrypt all of their communications to assure privacy and data integrity as
they go about their business.

The Kerberos architecture is centered around a trusted authentication service
called the key distribution center, or KDC.
Users and services in a Kerberos environment are referred to as principals;
each principal shares a secret, such as a password, with the KDC.

Enable Kerberos Authentication
------------------------------

To enable the Kerberos authentication method, we need to

Create a Kerberos principal and keytab
**************************************

You can use the following commands in a Linux-based Kerberos environment to set up
the identity and update the keytab file:

The ``kyuubi.keytab`` file must be owned and readable by the Linux login user.

.. code-block::

   # kadmin
     : addprinc -randkey superuser/FQDN@REALM
     : ktadd -k ./kyuubi.keytab superuser/FQDN@REALM

.. note:: A widespread use case of kyuubi is to replace HiveServer2/Hive QL with
   Kyuubi/Spark SQL. If an existing HiveServer2 environment is already there,
   copying the environment and reusing the keytab and principal of HiveServer2 is
   a convenient way.

Enable `Hadoop Impersonation`_
*******************************

If background cluster is also an kerberized Hadoop cluster, we need to enable the
impersonation capability of the superuser we use to start kyuubi server.

You can configure proxy user using properties ``hadoop.proxyuser.$superuser.hosts``
along with either or both of ``hadoop.proxyuser.$superuser.groups`` and ``hadoop.proxyuser.$superuser.users``.

For instance, by specifying as below in ``core-site.xml``, the ``superuser`` named ``admin`` can connect
only from ``host1`` and ``host2`` to impersonate a user belonging to ``group1`` and ``group2``.

.. code-block:: xml

   <property>
     <name>hadoop.proxyuser.admin.hosts</name>
     <value>host1,host2</value>
   </property>
   <property>
     <name>hadoop.proxyuser.admin.groups</name>
     <value>group1,group2</value>
   </property>


Here,

- ``admin`` is the principal(short name) used to start kyuubi servers
- ``host1`` and ``host2`` are node addresses of kyuubi servers
- ``group1`` and ``group2`` are groups of client users

.. note:: These configurations need to be configured in the Hadoop cluster
   and refreshed to take effect.

.. note:: If you are using the keytab of existing HiveServer2, this step can
   also be omitted

Configure the authentication properties
***************************************

Configure the following properties to ``$KYUUBI_HOME/conf/kyuubi-defaults.conf``
on each node where kyuubi server is installed.

.. code-block:: properties

   kyuubi.authentication=KERBEROS
   kyuubi.kinit.principal=superuser/FQDN@REALM
   kyuubi.kinit.keytab=/path/to/kyuubi.keytab

These `configurations`_ also need to be set to enable KERBEROS authentication.

Refresh all the kyuubi server instances
***************************************

Restart all the kyuubi server instances or `Refresh Configurations`_ to activate the settings.

.. _Hadoop Impersonation: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/Superusers.html
.. _configurations: ../client/advanced/kerberos.html
.. _Refresh Configurations: ../tools/kyuubi-admin.html#refresh-config
