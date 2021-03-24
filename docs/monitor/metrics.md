# Kyuubi Server Metrics

Kyuubi has a configurable metrics system based on the [Dropwizard Metrics Library](https://metrics.dropwizard.io/).
This allows users to report Kyuubi metrics to a variety of `kyuubi.metrics.reporters`. 
The metrics provide instrumentation for specific activities and Kyuubi server.

## Configurations

The metrics system is configured via `$KYUUBI_HOME/conf/kyuubi-defaults.conf`.

Key | Default | Meaning | Since
--- | --- | --- | ---
kyuubi\.metrics<br>\.enabled|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>true</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Set to true to enable kyuubi metrics system</div>|<div style='width: 20pt'>1.1.0</div>
kyuubi\.metrics\.json<br>\.report\.location|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>metrics</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Where the json metrics file located</div>|<div style='width: 20pt'>1.1.0</div>
kyuubi\.metrics\.report<br>\.interval|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>How often should report metrics to json/console. no effect on JMX</div>|<div style='width: 20pt'>1.1.0</div>
kyuubi\.metrics<br>\.reporters|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>JSON</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>A comma separated list for all metrics reporters<ul> <li>JSON - default reporter which outputs measurements to json file periodically</li> <li>CONSOLE - ConsoleReporter which outputs measurements to CONSOLE.</li> <li>SLF4J - Slf4jReporter which outputs measurements to system log.</li> <li>JMX - JmxReporter which listens for new metrics and exposes them as namespaced MBeans.</li> </ul></div>|<div style='width: 20pt'>1.1.0</div>


## Metrics

These metrics include:

Metrics Prefix | Metrics Suffix | Type | Since | Description
---|---|---|---|---
kyuubi<br/>.exec.pool<br/>.threads.alive ||gauge|1.1.0|<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads keepAlive in the backend executive thread pool</div>
kyuubi<br/>.exec.pool<br/>.threads.active ||gauge|1.1.0|<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads active in the backend executive thread pool</div>
kyuubi<br/>.connection.total   | | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative connection count</div>
kyuubi<br/>.connection.opened  | | gauge | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> current active connection count</div>
kyuubi<br/>.connection.opened  | `${user}` | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative connections requested by a `${user}`</div>
kyuubi<br/>.connection.failed  | | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed connection count</div>
kyuubi<br/>.connection.failed  | `${user}` | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed connections for a `${user}`</div>
kyuubi<br/>.statement.total    | | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative opened statement count</div>
kyuubi<br/>.statement.opened   | | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  current opened statement count</div>
kyuubi<br/>.statement.failed   | `${errorType}` | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed statement for a particular `${errorType}`, e.g. `AnalysisException`</div>
kyuubi<br/>.engine.total       | | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative created engines</div>
kyuubi<br/>.engine.timeout     | | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative timeout engines</div>
kyuubi<br/>.engine.failed      | `${user}` | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative explicitly failed engine count for a `${user}`</div>
kyuubi<br/>.engine.failed      | `${errorType}` | counter | 1.1.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative explicitly failed engine count for a particular `${errorType}`, e.g. `ClassNotFoundException`</div>
