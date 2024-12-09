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

Configure Kyuubi to use Custom Authentication
=============================================

Besides the `builtin authentication`_ methods, kyuubi supports custom
authentication implementations of `org.apache.kyuubi.service.authentication.PasswdAuthenticationProvider`
and `org.apache.kyuubi.service.authentication.TokenAuthenticationProvider`.

.. code-block:: scala

   package org.apache.kyuubi.service.authentication

   import javax.security.sasl.AuthenticationException

   trait PasswdAuthenticationProvider {

     /**
      * The authenticate method is called by the Kyuubi Server authentication layer
      * to authenticate users for their requests.
      * If a user is to be granted, return nothing/throw nothing.
      * When a user is to be disallowed, throw an appropriate [[AuthenticationException]].
      *
      * @param user     The username received over the connection request
      * @param password The password received over the connection request
      *
      * @throws AuthenticationException When a user is found to be invalid by the implementation
      */
     @throws[AuthenticationException]
     def authenticate(user: String, password: String): Unit
   }

Build A Custom Authenticator
----------------------------

To create custom Authenticator class derived from the above interface, we need to:

- Referencing the library

.. parsed-literal::

   <dependency>
      <groupId>org.apache.kyuubi</groupId>
      <artifactId>kyuubi-common_2.12</artifactId>
      <version>\ |release|\</version>
      <scope>provided</scope>
   </dependency>

- Implement PasswdAuthenticationProvider or TokenAuthenticationProvider - `Sample Code`_


Enable Custom Authentication
----------------------------

To enable the custom authentication method, we need to

- Put the jar package to ``$KYUUBI_HOME/jars`` directory to make it visible for
  the classpath of the kyuubi server.
- Configure the following properties to ``$KYUUBI_HOME/conf/kyuubi-defaults.conf``
  on each node where kyuubi server is installed.

.. code-block:: property

   kyuubi.authentication=CUSTOM
   kyuubi.authentication.custom.class=YourAuthenticationProvider
   kyuubi.authentication.custom.basic.class=YourBasicAuthenticationProvider
   kyuubi.authentication.custom.bearer.class=YourBearerAuthenticationProvider

- Restart all the kyuubi server instances

.. _builtin authentication: ../../security/authentication.html
.. _Sample Code: https://github.com/kyuubilab/example-custom-authentication/blob/main/src/main/scala/org/apache/kyuubi/example/MyAuthenticationProvider.scala