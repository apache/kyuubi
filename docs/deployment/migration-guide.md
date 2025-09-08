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

## Upgrading from Kyuubi 1.10 to 1.11

* Since Kyuubi 1.11, the configuration `spark.sql.watchdog.forcedMaxOutputRows` provided by Kyuubi Spark extension is removed, consider using `kyuubi.operation.result.max.rows` instead. Note, the latter works without requirement of installing Kyuubi Spark extension.
* Since Kyuubi 1.11, if the engine is running in cluster mode, Kyuubi will respect the `kyuubi.session.engine.startup.waitCompletion` config to determine whether to wait for the engine completion or not. If the engine is running in client mode, Kyuubi will always wait for the engine completion. And for Spark engine, Kyuubi will append the `spark.yarn.submit.waitAppCompletion` and `spark.kubernetes.submission.waitAppCompletion` configs to the engine conf based on the value of `kyuubi.session.engine.startup.waitCompletion`.
* Since Kyuubi 1.11, the configuration `kyuubi.session.engine.spark.initialize.sql` set by the client (via session configuration) is now correctly applied to every session in shared engines (USER, GROUP, SERVER). Previously, only the value set on the server side was applied and only for the first session when the engine started. Now, session-level settings provided by each client are respected.
* Since Kyuubi 1.11, the support of Flink engine for Flink 1.17 and 1.18 are deprecated, and will be removed in the future.

## Upgrading from Kyuubi 1.9 to 1.10

* Since Kyuubi 1.10, `beeline` is deprecated and will be removed in the future, please use `kyuubi-beeline` instead.
* Since Kyuubi 1.10, the support of Spark engine for Spark 3.1 is removed.
* Since Kyuubi 1.10, the support of Spark engine for Spark 3.2 is deprecated, and will be removed in the future.
* Since Kyuubi 1.10, the support of Flink engine for Flink 1.16 is removed.

## Upgrading from Kyuubi 1.8 to 1.9

* Since Kyuubi 1.9, `kyuubi.session.conf.advisor` can be set as a sequence, Kyuubi supported chaining SessionConfAdvisors.
* Since Kyuubi 1.9, the support of Derby is removal for Kyuubi metastore.
* Since Kyuubi 1.9, the support of Spark SQL engine for Spark 3.1 is deprecated, and will be removed in the future.
* Since Kyuubi 1.9, the support of Spark extensions for Spark 3.1 is removed, please use Spark 3.2 or higher versions.
* Since Kyuubi 1.9, `kyuubi.frontend.login.timeout`, `kyuubi.frontend.thrift.login.timeout`, `kyuubi.frontend.backoff.slot.length`, `kyuubi.frontend.thrift.backoff.slot.length` are removed.
* Since Kyuubi 1.9, the support of Flink engine for Flink 1.16 is deprecated, and will be removed in the future.

## Upgrading from Kyuubi 1.8.0 to 1.8.1

* Since Kyuubi 1.8.1, for `DELETE /batches/${batchId}`, `hive.server2.proxy.user` is not needed in the request parameters.
* Since Kyuubi 1.8.1, the default SQLite file `kyuubi_state_store.db` for Metadata store is located under `$KYUUBI_HOME` instead of `$PWD`. To restore previous behavior, set `kyuubi.metadata.store.jdbc.url` to `jdbc:sqlite:kyuubi_state_store.db`.

## Upgrading from Kyuubi 1.7 to 1.8

* Since Kyuubi 1.8, SQLite is added and becomes the default database type of Kyuubi metastore, as Derby has been deprecated.
  Both Derby and SQLite are mainly for testing purposes, and they're not supposed to be used in production.
  To restore previous behavior, set `kyuubi.metadata.store.jdbc.database.type=DERBY` and
  `kyuubi.metadata.store.jdbc.url=jdbc:derby:memory:kyuubi_state_store_db;create=true`.
* Since Kyuubi 1.8, if the directory of the embedded zookeeper configuration (`kyuubi.zookeeper.embedded.directory`
  & `kyuubi.zookeeper.embedded.data.dir` & `kyuubi.zookeeper.embedded.data.log.dir`) is a relative path, it is resolved
  relative to `$KYUUBI_HOME` instead of `$PWD`.
* Since Kyuubi 1.8, PROMETHEUS is changed as the default metrics reporter. To restore previous behavior,
  set `kyuubi.metrics.reporters=JSON`.

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

