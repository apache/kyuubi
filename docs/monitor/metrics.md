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
<code>kyuubi.metrics.enabled</code>|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>true</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Set to true to enable kyuubi metrics system</div>|<div style='width: 30pt'>boolean</div>|<div style='width: 20pt'>1.2.0</div>
<code>kyuubi.metrics.reporters</code>|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>JSON</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>A comma separated list for all metrics reporters<ul> <li>CONSOLE - ConsoleReporter which outputs measurements to CONSOLE periodically.</li> <li>JMX - JmxReporter which listens for new metrics and exposes them as MBeans.</li>  <li>JSON - JsonReporter which outputs measurements to json file periodically.</li> <li>PROMETHEUS - PrometheusReporter which exposes metrics in prometheus format.</li> <li>SLF4J - Slf4jReporter which outputs measurements to system log periodically.</li></ul></div>|<div style='width: 30pt'>seq</div>|<div style='width: 20pt'>1.2.0</div>
<code>kyuubi.metrics.console.interval</code>|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often should report metrics to console</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.2.0</div>
<code>kyuubi.metrics.json.interval</code>|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often should report metrics to json file</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.2.0</div>
<code>kyuubi.metrics.json.location</code>|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>metrics</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Where the json metrics file located</div>|<div style='width: 30pt'>string</div>|<div style='width: 20pt'>1.2.0</div>
<code>kyuubi.metrics.prometheus.path</code>|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>/metrics</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>URI context path of prometheus metrics HTTP server</div>|<div style='width: 30pt'>string</div>|<div style='width: 20pt'>1.2.0</div>
<code>kyuubi.metrics.prometheus.port</code>|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>10019</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Prometheus metrics HTTP server port</div>|<div style='width: 30pt'>int</div>|<div style='width: 20pt'>1.2.0</div>
<code>kyuubi.metrics.slf4j.interval</code>|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often should report metrics to SLF4J logger</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.2.0</div>

## Metrics

These metrics include:

Metrics Prefix | Metrics Suffix | Type | Since | Description
---|---|---|---|---
<code>kyuubi.exec.pool.threads.alive</code>  | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads keepAlive in the backend executive thread pool</div>
<code>kyuubi.exec.pool.threads.active</code> | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads active in the backend executive thread pool</div>
<code>kyuubi.connection.total</code>   | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative connection count</div>
<code>kyuubi.connection.opened</code>  | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> current active connection count</div>
<code>kyuubi.connection.opened</code>  | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> current active connections count requested by a `${user}`</div>
<code>kyuubi.connection.failed</code>  | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed connection count</div>
<code>kyuubi.connection.failed</code>  | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative failed connections for a `${user}`</div>
<code>kyuubi.operation.total</code>    | | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative opened operation count</div>
<code>kyuubi.operation.total</code>    | `${operationType}` | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative opened count for the operation `${operationType}`</div>
<code>kyuubi.operation.opened</code>   | | gauge | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  current opened operation count</div>
<code>kyuubi.operation.opened</code>   | `${operationType}` | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  current opened count for the operation `${operationType}`</div>
<code>kyuubi.operation.failed</code>   | `${operationType}`<br/>`.${errorType}` | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed count for the operation `${operationType}` with a particular `${errorType}`, e.g. `execute_statement.AnalysisException`</div>
<code>kyuubi.operation.state</code>    | `${operationState}` | meter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  kyuubi operation state rate </div>
<code>kyuubi.engine.total</code>       | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative created engines</div>
<code>kyuubi.engine.timeout</code>     | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative timeout engines</div>
<code>kyuubi.engine.failed</code>      | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative explicitly failed engine count for a `${user}`</div>
<code>kyuubi.engine.failed</code>      | `${errorType}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative explicitly failed engine count for a particular `${errorType}`, e.g. `ClassNotFoundException`</div>
<code>kyuubi.backend_service.open_session</code>            | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `openSession` method execution time and rate </div>
<code>kyuubi.backend_service.close_session</code>           | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `closeSession` method execution time and rate </div>
<code>kyuubi.backend_service.get_info</code>                | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getInfo` method execution time and rate </div>
<code>kyuubi.backend_service.execute_statement</code>       | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `executeStatement` method execution time and rate </div>
<code>kyuubi.backend_service.get_type_info</code>           | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getTypeInfo` method execution time and rate </div>
<code>kyuubi.backend_service.get_catalogs</code>            | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getCatalogs` method execution time and rate </div>
<code>kyuubi.backend_service.get_schemas</code>             | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getSchemas` method execution time and rate </div>
<code>kyuubi.backend_service.get_tables</code>              | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getTables` method execution time and rate </div>
<code>kyuubi.backend_service.get_table_types</code>         | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getTableTypes` method execution time and rate </div>
<code>kyuubi.backend_service.get_columns</code>             | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getColumns` method execution time and rate </div>
<code>kyuubi.backend_service.get_functions</code>           | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getFunctions` method execution time and rate </div>
<code>kyuubi.backend_service.get_operation_status</code>    | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getOperationStatus` method execution time and rate </div>
<code>kyuubi.backend_service.cancel_operation</code>        | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `cancelOperation` method execution time and rate </div>
<code>kyuubi.backend_service.close_operation</code>         | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `closeOperation` method execution time and rate </div>
<code>kyuubi.backend_service.get_result_set_metadata</code> | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getResultSetMetadata` method execution time and rate </div>
<code>kyuubi.backend_service.fetch_results</code>           | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `fetchResults` method execution time and rate </div>
<code>kyuubi.backend_service.fetch_log_rows_rate</code>     | | meter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `fetchResults` method that fetch log rows rate </div>
<code>kyuubi.backend_service.fetch_result_rows_rate</code>  | | meter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `fetchResults` method that fetch result rows rate </div>

Before v1.5.0, if you use these metrics:
- `kyuubi.statement.total`
- `kyuubi.statement.opened`
- `kyuubi.statement.failed.${errorType}`

Since v1.5.0, you can use the following metrics to replace:
- `kyuubi.operation.total.ExecuteStatement`
- `kyuubi.operation.opened.ExecuteStatement`
- `kyuubi.operation.failed.ExecuteStatement.${errorType}`
