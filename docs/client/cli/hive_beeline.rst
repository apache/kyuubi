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

Hive Beeline
============

Kyuubi supports Apache Hive beeline that works with Kyuubi server.
Hive beeline is a `SQLLine CLI <https://sqlline.sourceforge.net/>`_ based on the `Hive JDBC Driver <../jdbc/hive_jdbc.html>`_.

Prerequisites
-------------

- Kyuubi server installed and launched.
- Hive beeline installed

.. important:: Kyuubi does not support embedded mode which beeline and server run in the same process.
   It always uses remote mode for connecting beeline with a separate server process over thrift.

.. warning:: The document you are visiting now is incomplete, please help kyuubi community to fix it if appropriate for you.
