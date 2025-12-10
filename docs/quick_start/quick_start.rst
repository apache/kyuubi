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


Getting Started
===============

.. note::

   This page covers how to start with kyuubi quickly on your
   laptop in about 3~5 minutes.

Requirements
------------

For quick start deployment, we need to prepare the following stuffs:

- A **client** that connects and submits queries to the server. Here, we use the
  kyuubi beeline for demonstration.
- A **server** that serves clients and manages engines.
- An **engine** that is used to instantiate query execution environments. Here we
  use Spark for demonstration.

These essential components are JVM-based applications. So, the JRE needs to be
pre-installed and the ``JAVA_HOME`` is correctly set to each component.

 ================ ============ ==================== =======================================================
  Component        Role         Version              Remarks
 ================ ============ ==================== =======================================================
  **Java**         JRE          8/11/17              Officially released against JDK8
  **Kyuubi**       Gateway      \ |release| \        - Kyuubi Server
                   Engine lib                        - Kyuubi Engine
                   Beeline                           - Kyuubi Beeline
  **Spark**        Engine       3.3 to 3.5, 4.0      A Spark distribution
  **Flink**        Engine       1.17 to 1.20         A Flink distribution
  **Trino**        Engine       N/A                  A Trino cluster allows to access via trino-client v411
  **Doris**        Engine       N/A                  A Doris cluster
  **Hive**         Engine       - 2.1-cdh6/2.3/3.1   - A Hive distribution
                   Metastore    - N/A                - An optional and external metadata store,
                                                       whose version is decided by engines
  **Zookeeper**    HA           >=3.4.x
  **Disk**         Storage      N/A                  N/A
 ================ ============ ==================== =======================================================

The other internal or external parts listed in the above sheet can be used individually
or all together. For example, you can use Kyuubi, Spark and Flink to build a streaming
data warehouse. And then, you can use Zookeeper to enable the load balancing for high
availability. The data could be stored in Hive, Apache Iceberg, or other DBMSs.

In what follows, we will only use Kyuubi and Spark.

Installation
------------

.. note::
   :class: dropdown, toggle

   This following instructions are based on binary releases. If you start with
   source releases, please refer to the page for `building kyuubi`_.

Install Kyuubi
~~~~~~~~~~~~~~

The official releases, binary- and source-, are archived on the
`download page`_. Please download the most recent stable release
to start.

To install Kyuubi, you need to unpack the tarball. For example,

.. parsed-literal::

   $ tar zxf apache-kyuubi-\ |release|\-bin.tgz

.. code-block::
   :class: toggle

   ├── LICENSE
   ├── NOTICE
   ├── RELEASE
   ├── beeline-jars
   ├── bin
   ├── charts
   │   └── kyuubi
   ├── conf
   |   ├── kyuubi-defaults.conf.template
   │   ├── kyuubi-env.sh.template
   │   └── log4j2.xml.template
   ├── db-scripts
   │   ├── mysql
   │   ├── postgresql
   │   └── sqlite
   ├── docker
   │   ├── Dockerfile
   │   └── playground
   ├── externals
   │  └── engines
   ├── jars
   ├── licenses
   ├── logs
   ├── pid
   ├── web-ui
   └── work

From top to bottom are:

- LICENSE: the APACHE `LICENSE`_, VERSION 2.0 we claim to obey.
- RELEASE: the build information of this package.
- NOTICE: the notice made by Apache Kyuubi Community about its project and dependencies.
- bin: the entry of the Kyuubi server with `kyuubi` as the startup script.
- conf: all the defaults used by Kyuubi Server itself or creating a session with engines.
- externals
  - engines: contains all kinds of SQL engines that we support
- licenses: a bunch of licenses included.
- jars: packages needed by the Kyuubi server.
- logs: where the logs of the Kyuubi server locates.
- pid: stores the PID file of the Kyuubi server instance.
- work: the root of the working directories of all the forked sub-processes, a.k.a. SQL engines.

Install Spark
~~~~~~~~~~~~~

The official releases, binary- and source-, are archived on the
`spark download page`_. Please download the most recent stable
release to start.

.. note::
   :class: dropdown, toggle

   Currently, Kyuubi is compiled and pre-built against Spark 3 and Scala 2.12
   You will probably meet runtime exceptions if you use Spark 2 or Spark with
   unsupported Scala versions.

To install Spark, you need to unpack the tarball. For example,

.. code-block::

   $ tar zxf spark-3.4.2-bin-hadoop3.tgz

Configuration
~~~~~~~~~~~~~

The `kyuubi-env.sh` file is used to set system environment variables to the kyuubi
server process and engine processes it creates.

The `kyuubi-defaults.conf` file is used to set system properties to the kyuubi server
process and engine processes it creates.

Each file has a template lays in `conf` directory for your information. The following
are examples of the parameters necessary for a quick start with Spark.

- **JAVA_HOME**

.. code-block::

  $ echo 'export JAVA_HOME=/path/to/java' >> conf/kyuubi-env.sh

- **SPARK_HOME**

.. code-block::

   $ echo 'export SPARK_HOME=/path/to/spark' >> conf/kyuubi-env.sh


Start Kyuubi
------------

.. code-block::

   $ bin/kyuubi start

If script above runs successfully, it will store the `PID` of the server instance
into `pid/kyuubi-<username>-org.apache.kyuubi.server.KyuubiServer.pid`.
And you are able to get the JDBC connection URL from the log file -
`logs/kyuubi-<username>-org.apache.kyuubi.server.KyuubiServer-<hostname>.out`.

For example,

  Starting and exposing JDBC connection at: jdbc:kyuubi://localhost:10009/

If something goes wrong, you shall be able to find some clues in the log file too.

.. note::
   :class: toggle

   Alternatively, it can run in the foreground, with the logs and other output
   written to stdout/stderr. Both streams should be captured if using a
   supervision system like `supervisord`.

   .. code-block::

      bin/kyuubi run


Operate Clients
---------------

Kyuubi delivers a kyuubi-beeline client, enabling a similar experience to Apache Hive use cases.

Open Connections
~~~~~~~~~~~~~~~~

Replace the `host` and `port` with the actual ones you've got in the step of server startup
for the following JDBC URL. The case below open a session for user named `apache`.

.. code-block::

   $ bin/kyuubi-beeline -u 'jdbc:kyuubi://localhost:10009/' -n apache

.. note::
   :class: toggle

   Use `--help` to display the usage guide for the kyuubi-beeline tool.

   .. code-block::

      $ bin/kyuubi-beeline --help

Execute Statements
~~~~~~~~~~~~~~~~~~

After successfully connected with the server, you can run sql queries in the kyuubi-beeline
console. For instance,

.. code-block::
   :class: sql

   > SHOW DATABASES;

You will see a wall of operation logs, and a result table in the kyuubi-beeline console.

.. code-block::

   omitted logs
   +------------+
   | namespace  |
   +------------+
   | default    |
   +------------+
   1 row selected (0.2 seconds)

Start Engines
~~~~~~~~~~~~~

Engines are launched by the server automatically without end users' attention.

If you use the same user in the above case to create another connection, the
engine will be reused. You may notice that the time cost for connection here is
much shorter than the last round.

If you use a different user to create a new connection, another engine will be
started.

.. code-block::

   $ bin/kyuubi-beeline -u 'jdbc:kyuubi://localhost:10009/' -n kentyao

This may change depending on the `engine share level`_ you set.

Close Connections
~~~~~~~~~~~~~~~~~

Close the session between kyuubi-beeline and Kyuubi server by executing `!quit`, for example,

.. code-block::

   > !quit
   Closing: 0: jdbc:kyuubi://localhost:10009/

Stop Engines
~~~~~~~~~~~~

Engines are stop by the server automatically according `engine lifecycle`_
without end users' attention. Terminations of connections do not necessarily
mean terminations of engines. It depends on both the `engine share level`_ and
`engine lifecycle`_.

Stop Kyuubi
-----------

Stop Kyuubi which running at the background by performing the following in the `$KYUUBI_HOME` directory:

.. code-block::

   $ bin/kyuubi stop

And then, you will see the Kyuubi server waving goodbye to you.

The Kyuubi server will be stopped immediately while
the engine will still be alive for a while.

If you start Kyuubi again before the engine terminates itself,
it will reconnect to the newly created one.

.. _DOWNLOAD PAGE: https://kyuubi.apache.org/releases.html
.. _BUILDING KYUUBI: ../develop_tools/distribution.html
.. _SPARK DOWNLOAD PAGE: https://spark.apache.org/downloads.html
.. _LICENSE: https://www.apache.org/licenses/LICENSE-2.0
.. _ENGINE SHARE LEVEL: ../deployment/engine_share_level.html
.. _ENGINE LIFECYCLE: ../deployment/engine_lifecycle.html
