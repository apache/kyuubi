# Kyuubi Server Metrics

## Configurations

Key | Default | Meaning | Since
--- | --- | --- | ---
kyuubi\.metrics<br>\.enabled|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>true</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Set to true to enable kyuubi metrics system</div>|<div style='width: 20pt'>1.1.0</div>
kyuubi\.metrics\.json<br>\.report\.location|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>metrics</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Where the json metrics file located</div>|<div style='width: 20pt'>1.1.0</div>
kyuubi\.metrics\.report<br>\.interval|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>How often should report metrics to json/console. no effect on JMX</div>|<div style='width: 20pt'>1.1.0</div>
kyuubi\.metrics<br>\.reporters|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>JSON</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>A comma separated list for all metrics reporters<ul> <li>JSON - default reporter which outputs measurements to json file periodically</li> <li>CONSOLE - ConsoleReporter which outputs measurements to CONSOLE.</li> <li>SLF4J - Slf4jReporter which outputs measurements to system log.</li> <li>JMX - JmxReporter which listens for new metrics and exposes them as namespaced MBeans.</li> </ul></div>|<div style='width: 20pt'>1.1.0</div>


## Metrics

Metrics Name|Type|Added In|Description
---|---|---|---
exec_async_pool_size|gauge|0.7.0| backend service executive threadpool size
exec_async_queue_size|gauge|0.7.0| backend service executive threadpool wait queue size
error_queries|counter|0.7.0| total failed queris
open_connections|counter|0.7.0| current opened connections
open_operations|counter|0.7.0| current opened operations
running_queries|counter|0.7.0| current running sql queries
spark_session_cache_size|gauge|0.7.0| current caches for SparkSession/SparkContext
total_connections|counter|0.7.0| cumulative connection count
total_queries|counter|0.7.0| cumulative sql quries 
