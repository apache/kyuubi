<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Kyuubi Migration Guide

## Upgrading from Kyuubi 1.7 to 1.8

* Since Kyuubi 1.8, SQLite is added and becomes the default database type of Kyuubi metastore, as Derby has been deprecated.
  Both Derby and SQLite are mainly for testing purposes, and they're not supposed to be used in production.
  To restore previous behavior, set `kyuubi.metadata.store.jdbc.database.type=DERBY` and
  `kyuubi.metadata.store.jdbc.url=jdbc:derby:memory:kyuubi_state_store_db;create=true`.

## Upgrading from Kyuubi 1.7.1 to 1.7.2

* Since Kyuubi 1.7.2, for Kyuubi BeeLine, please use `--python-mode` option to run python code or script.

## Upgrading from Kyuubi 1.7.0 to 1.7.1

* Since Kyuubi 1.7.1, `protocolVersion` is removed from the request parameters of the REST API `Open(create) a session`. All removed or unknown parameters will be silently ignored and affects nothing.
* Since Kyuubi 1.7.1, `confOverlay` is supported in the request parameters of the REST API `Create an operation with EXECUTE_STATEMENT type`.

## Upgrading from Kyuubi 1.6 to 1.7

* In Kyuubi 1.7, `kyuubi.ha.zookeeper.engine.auth.type` does not fallback to `kyuubi.ha.zookeeper.auth.type`.  
  When Kyuubi engine does Kerberos authentication with Zookeeper, user needs to explicitly set `kyuubi.ha.zookeeper.engine.auth.type` to `KERBEROS`.
* Since Kyuubi 1.7, Kyuubi returns engine's information for `GetInfo` request instead of server. To restore the previous behavior, set `kyuubi.server.info.provider` to `SERVER`.
* Since Kyuubi 1.7, Kyuubi session type `SQL` is refactored to `INTERACTIVE`, because Kyuubi supports not only `SQL` session, but also `SCALA` and `PYTHON` sessions.
  User need to use `INTERACTIVE` sessionType to look up the session event.
* Since Kyuubi 1.7, the REST API of `Open(create) a session` will not contain parameters `user` `password` and `IpAddr`. User and password should be set in `Authorization` of http request if needed.

## Upgrading from Kyuubi 1.6.0 to 1.6.1

* Since Kyuubi 1.6.1, `kyuubi.ha.zookeeper.engine.auth.type` does not fallback to `kyuubi.ha.zookeeper.auth.type`.  
  When Kyuubi engine does Kerberos authentication with Zookeeper, user needs to explicitly set `kyuubi.ha.zookeeper.engine.auth.type` to `KERBEROS`.

## Upgrading from Kyuubi 1.5 to 1.6

* Kyuubi engine gets Zookeeper principal & keytab from `kyuubi.ha.zookeeper.auth.principal` & `kyuubi.ha.zookeeper.auth.keytab`.    
  `kyuubi.ha.zookeeper.auth.principal` & `kyuubi.ha.zookeeper.auth.keytab` fallback to `kyuubi.kinit.principal` & `kyuubi.kinit.keytab` when not set.    
  Since Kyuubi 1.6, `kyuubi.kinit.principal` & `kyuubi.kinit.keytab` are filtered out from Kyuubi engine's conf for better security.  
  When Kyuubi engine does Kerberos authentication with Zookeeper, user needs to explicitly set `kyuubi.ha.zookeeper.auth.principal` & `kyuubi.ha.zookeeper.auth.keytab`.

