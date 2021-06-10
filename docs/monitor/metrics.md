# Kyuubi Server Metrics

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
kyuubi<br/>.exec.pool<br/>.threads.alive ||gauge|1.2.0|<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads keepAlive in the backend executive thread pool</div>
kyuubi<br/>.exec.pool<br/>.threads.active ||gauge|1.2.0|<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads active in the backend executive thread pool</div>
kyuubi<br/>.connection.total   | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative connection count</div>
kyuubi<br/>.connection.opened  | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> current active connection count</div>
kyuubi<br/>.connection.opened  | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative connections requested by a `${user}`</div>
kyuubi<br/>.connection.failed  | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed connection count</div>
kyuubi<br/>.connection.failed  | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed connections for a `${user}`</div>
kyuubi<br/>.statement.total    | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative opened statement count</div>
kyuubi<br/>.statement.opened   | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  current opened statement count</div>
kyuubi<br/>.statement.failed   | `${errorType}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed statement for a particular `${errorType}`, e.g. `AnalysisException`</div>
kyuubi<br/>.engine.total       | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative created engines</div>
kyuubi<br/>.engine.timeout     | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative timeout engines</div>
kyuubi<br/>.engine.failed      | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative explicitly failed engine count for a `${user}`</div>
kyuubi<br/>.engine.failed      | `${errorType}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative explicitly failed engine count for a particular `${errorType}`, e.g. `ClassNotFoundException`</div>
