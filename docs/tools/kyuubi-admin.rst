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

Kyuubi Administer Tool
=======================

.. versionadded:: 1.6.0

Kyuubi administer tool(kyuubi-admin) provides administrators with some maintenance operations against a kyuubi server or cluster.

.. _installation:

Installation
-------------------------------------
To install kyuubi-admin, you need to unpack the tarball. For example,

.. parsed-literal::

   tar zxf apache-kyuubi-\ |release|\ -bin.tgz

This will result in the creation of a subdirectory named apache-kyuubi-|release|-bin shown below,

.. parsed-literal::

   apache-kyuubi-\ |release|\ -bin
   ├── ...
   ├── bin
   |   ├── kyuubi-admin
   │   ├── ...
   ├── ...


.. _usage:

Usage
-------------------------------------
.. code-block:: bash

   bin/kyuubi-admin --help



.. _refresh_config:

Refresh config
-------------------------------------

Refresh the config with specified type.

Usage: ``bin/kyuubi-admin refresh config [options] [<configType>]``

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Config Type
     - Description
   * - hadoopConf
     - The hadoop conf used for proxy user verification.

.. _list_engine:

List Engines
-------------------------------------

Prints a table of the key information about the specified engines.

Usage: ``bin/kyuubi-admin list engine [options]``

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Options
     - Description
   * - -et, --engine-type
     - The engine type. If not specified, it will read the configuration item kyuubi.engine.type from kyuubi-defaults.conf.
   * - -esl, --engine-share-level
     - The engine share level. If not specified, it will read the configuration item kyuubi.engine.share.level from kyuubi-defaults.conf.
   * - -es, --engine-subdomain
     - The subdomain for the share level of an engine. If not specified, it will read the configuration item kyuubi.engine.share.level.subdomain from kyuubi-defaults.conf.
   * - --hs2ProxyUser
     - The proxy user to impersonate. When specified, it will list engines for the hs2ProxyUser.

.. _delete_engine:

Delete an Engine
-------------------------------------

Delete the specified engine.

Usage: ``bin/kyuubi-admin delete engine [options]``

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Options
     - Description
   * - -et, --engine-type
     - The engine type. If not specified, it will read the configuration item kyuubi.engine.type from kyuubi-defaults.conf.
   * - -esl, --engine-share-level
     - The engine share level. If not specified, it will read the configuration item kyuubi.engine.share.level from kyuubi-defaults.conf.
   * - -es, --engine-subdomain
     - The subdomain for the share level of an engine. If not specified, it will read the configuration item kyuubi.engine.share.level.subdomain from kyuubi-defaults.conf. Default value is "default".
   * - --hs2ProxyUser
     - The proxy user to impersonate. When specified, it will delete engines for the hs2ProxyUser.
