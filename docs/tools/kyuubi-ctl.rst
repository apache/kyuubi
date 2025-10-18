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

Administrator CLI
=================

Usage
-----
.. code-block:: bash

   bin/kyuubi-ctl --help

Output

.. parsed-literal::

    kyuubi |release|
    Usage: kyuubi-ctl [create|get|delete|list] [options]

      -zk, --zk-quorum <value>
                               The connection string for the zookeeper ensemble, using zk quorum manually.
      -n, --namespace <value>  The namespace, using kyuubi-defaults/conf if absent.
      -s, --host <value>       Hostname or IP address of a service.
      -p, --port <value>       Listening port of a service.
      -v, --version <value>    Using the compiled KYUUBI_VERSION default, change it if the active service is running in another.
      -b, --verbose            Print additional debug output.

    Command: create [server]

    Command: create server
    	    Expose Kyuubi server instance to another domain.

    Command: get [server|engine] [options]
    	    Get the service/engine node info, host and port needed.
    Command: get server
    	    Get Kyuubi server info of domain
    Command: get engine
    	    Get Kyuubi engine info belong to a user.
      -u, --user <value>       The user name this engine belong to.
      -et, --engine-type <value>
                               The engine type this engine belong to.
      -es, --engine-subdomain <value>
                               The engine subdomain this engine belong to.
      -esl, --engine-share-level <value>
                               The engine share level this engine belong to.

    Command: delete [server|engine] [options]
    	    Delete the specified service/engine node, host and port needed.
    Command: delete server
    	    Delete the specified service node for a domain
    Command: delete engine
    	    Delete the specified engine node for user.
      -u, --user <value>       The user name this engine belong to.
      -et, --engine-type <value>
                               The engine type this engine belong to.
      -es, --engine-subdomain <value>
                               The engine subdomain this engine belong to.
      -esl, --engine-share-level <value>
                               The engine share level this engine belong to.

    Command: list [server|engine] [options]
    	    List all the service/engine nodes for a particular domain.
    Command: list server
    	    List all the service nodes for a particular domain
    Command: list engine
    	    List all the engine nodes for a user
      -u, --user <value>       The user name this engine belong to.
      -et, --engine-type <value>
                               The engine type this engine belong to.
      -es, --engine-subdomain <value>
                               The engine subdomain this engine belong to.
      -esl, --engine-share-level <value>
                               The engine share level this engine belong to.

      -h, --help               Show help message and exit.

Manage kyuubi servers
---------------------

You can specify the zookeeper address(``--zk-quorum``) and namespace(``--namespace``), version(``--version``) parameters to query a specific kyuubi server cluster.

List server
***********

List all the service nodes for a particular domain.

.. code-block:: bash

   bin/kyuubi-ctl list server

Create server
*************

Expose Kyuubi server instance to another domain.

First read ``kyuubi.ha.namespace`` in ``conf/kyuubi-defaults.conf``, if there are server instances under this namespace, register them in the new namespace specified by the ``--namespace`` parameter.

.. code-block:: bash

   bin/kyuubi-ctl create server --namespace XXX

Get server
**********

Get Kyuubi server info of domain.

.. code-block:: bash

   bin/kyuubi-ctl get server --host XXX --port YYY

Delete server
*************

Delete the specified service node for a domain.

After the server node is deleted, the kyuubi server stops opening new sessions and waits for all currently open sessions to be closed before the process exits.

.. code-block:: bash

   bin/kyuubi-ctl delete server --host XXX --port YYY

Manage kyuubi engines
---------------------

You can also specify the engine type(``--engine-type``), engine share level subdomain(``--engine-subdomain``) and engine share level(``--engine-share-level``).

If not specified, the configuration item ``kyuubi.engine.type`` of ``kyuubi-defaults.conf`` read, the default value is ``SPARK_SQL``, ``kyuubi.engine.share.level.subdomain``, the default value is ``default``, ``kyuubi.engine.share.level``, the default value is ``USER``.

If the engine pool mode is enabled through ``kyuubi.engine.pool.size``, the subdomain consists of ``kyuubi.engine.pool.name`` and a number below size, e.g. ``engine-pool-0`` .

``--engine-share-level`` supports the following enum values.

- CONNECTION

The engine Ref Id (UUID) must be specified via ``--engine-subdomain``.

- USER:

Default Value.

- GROUP:

The ``--user`` parameter is the group name corresponding to the user.

- SERVER:

The ``--user`` parameter is the user who started the kyuubi server.

List engine
***********

List all the engine nodes for a user.

.. code-block:: bash

   bin/kyuubi-ctl list engine --user AAA

The management share level is SERVER, the user who starts the kyuubi server is A, the engine is TRINO, and the subdomain is adhoc.

.. code-block:: bash

   bin/kyuubi-ctl list engine --user A --engine-type TRINO --engine-subdomain adhoc --engine-share-level SERVER

Get engine
**********

Get Kyuubi engine info belong to a user.

.. code-block:: bash

   bin/kyuubi-ctl get engine --user AAA --host XXX --port YYY

Delete engine
*************

Delete the specified engine node for user.

After the engine node is deleted, the kyuubi engine stops opening new sessions and waits for all currently open sessions to be closed before the process exits.

.. code-block:: bash

   bin/kyuubi-ctl delete engine --user AAA --host XXX --port YYY
