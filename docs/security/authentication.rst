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


Kyuubi Authentication Mechanism
=================================

In a secure cluster, services should be able to identify and authenticate
callers. As the fact that the user claims does not necessarily mean this
is true.

The authentication process of kyuubi is used to verify the user identity
that a client used to talk to the kyuubi server. Once done, a trusted
connection will be set up between the client and server if successful;
otherwise, rejected.

.. note:: This only authenticate whether a user or client can connect
   with Kyuubi server or not using the provided identity.
   For other secured services that this user wants to interact with, he/she
   also needs to pass the authentication process of each service, for instance,
   Hive Metastore, YARN, HDFS.

The related configurations can be found at `Authentication Configurations`_

.. toctree::
    :maxdepth: 2

    kerberos
    ../client/advanced/kerberos
    ldap
    jdbc
    ../extensions/server/authentication

.. _Authentication Configurations: ../deployment/settings.html#authentication
