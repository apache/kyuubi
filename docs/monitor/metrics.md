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

# Monitoring Kyuubi - Server Metrics

Kyuubi has a configurable metrics system based on the [Dropwizard Metrics Library](https://metrics.dropwizard.io/).
This allows users to report Kyuubi metrics to a variety of `kyuubi.metrics.reporters`. 
The metrics provide instrumentation for specific activities and Kyuubi server.

## Configurations

The metrics system is configured via `$KYUUBI_HOME/conf/kyuubi-defaults.conf`.

Key | Default | Meaning | Type | Since
--- | --- | --- | --- | ---
kyuubi\.metrics<br>\.enabled|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>true</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Set to true to enable kyuubi metrics system</div>|<div style='width: 30pt'>boolean</div>|<div style='width: 20pt'>1.2.0</div>
kyuubi\.metrics<br>\.reporters|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>JSON</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>A comma separated list for all metrics reporters<ul> <li>CONSOLE - ConsoleReporter which outputs measurements to CONSOLE periodically.</li> <li>JMX - JmxReporter which listens for new metrics and exposes them as MBeans.</li>  <li>JSON - JsonReporter which outputs measurements to json file periodically.</li> <li>PROMETHEUS - PrometheusReporter which exposes metrics in prometheus format.</li> <li>SLF4J - Slf4jReporter which outputs measurements to system log periodically.</li></ul></div>|<div style='width: 30pt'>seq</div>|<div style='width: 20pt'>1.2.0</div>
kyuubi\.metrics<br>\.console\.interval|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often should report metrics to console</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.2.0</div>
kyuubi\.metrics\.json<br>\.interval|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often should report metrics to json file</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.2.0</div>
kyuubi\.metrics\.json<br>\.location|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>metrics</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Where the json metrics file located</div>|<div style='width: 30pt'>string</div>|<div style='width: 20pt'>1.2.0</div>
kyuubi\.metrics<br>\.prometheus\.path|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>/metrics</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>URI context path of prometheus metrics HTTP server</div>|<div style='width: 30pt'>string</div>|<div style='width: 20pt'>1.2.0</div>
kyuubi\.metrics<br>\.prometheus\.port|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>10019</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Prometheus metrics HTTP server port</div>|<div style='width: 30pt'>int</div>|<div style='width: 20pt'>1.2.0</div>
kyuubi\.metrics\.slf4j<br>\.interval|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often should report metrics to SLF4J logger</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.2.0</div>

## Metrics

These metrics include:

Metrics Prefix | Metrics Suffix | Type | Since | Description
---|---|---|---|---
kyuubi<br/>.exec.pool<br/>.threads.alive  | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads keepAlive in the backend executive thread pool</div>
kyuubi<br/>.exec.pool<br/>.threads.active | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads active in the backend executive thread pool</div>
kyuubi<br/>.connection.total   | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative connection count</div>
kyuubi<br/>.connection.opened  | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> current active connection count</div>
kyuubi<br/>.connection.opened  | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> current active connections count requested by a `${user}`</div>
kyuubi<br/>.connection.failed  | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed connection count</div>
kyuubi<br/>.connection.failed  | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative failed connections for a `${user}`</div>
kyuubi<br/>.operation.total    | | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative opened operation count</div>
kyuubi<br/>.operation.total    | `${operationType}` | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative opened count for the operation `${operationType}`</div>
kyuubi<br/>.operation.opened   | | gauge | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  current opened operation count</div>
kyuubi<br/>.operation.opened   | `${operationType}` | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  current opened count for the operation `${operationType}`</div>
kyuubi<br/>.operation.failed   | `${operationType}`.`${errorType}` | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed count for the operation `${operationType}` with a particular `${errorType}`, e.g. `execute_statement.AnalysisException`</div>
kyuubi<br/>.engine.total       | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative created engines</div>
kyuubi<br/>.engine.timeout     | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative timeout engines</div>
kyuubi<br/>.engine.failed      | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative explicitly failed engine count for a `${user}`</div>
kyuubi<br/>.engine.failed      | `${errorType}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative explicitly failed engine count for a particular `${errorType}`, e.g. `ClassNotFoundException`</div>
kyuubi<br/>.backend_service.open_session_ms            | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `openSession` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.close_session_ms           | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `closeSession` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_info_ms                | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getInfo` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.execute_statement_ms       | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `executeStatement` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_type_info_ms           | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getTypeInfo` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_catalogs_ms            | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getCatalogs` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_schemas_ms             | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getSchemas` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_tables_ms              | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getTables` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_table_types_ms         | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getTableTypes` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_columns_ms             | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getColumns` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_functions_ms           | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getFunctions` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_operation_status_ms    | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getOperationStatus` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.cancel_operation_ms        | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `cancelOperation` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.close_operation_ms         | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `closeOperation` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.get_result_set_metadata_ms | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getResultSetMetadata` method time cost histogram statistics </div>
kyuubi<br/>.backend_service.fetch_results_ms           | | histogram | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `fetchResults` method time cost histogram statistics </div>

