# Kyuubi Server Metrics

The metrics that Kyuubi collects can be found at `spark.kyuubi.metrics.report.location` as a json format file.

## Configurations

Name|Default|Added In|Description
---|---|---|---
spark.kyuubi.<br />metrics.enabled|true|0.7.0|Whether to enable kyuubi metrics system
spark.kyuubi.<br />metrics.reporter|JSON|0.7.0|Comma separated list of reporters for kyuubi metrics system, candidates: JMX,CONSOLE,JSON
spark.kyuubi.<br />metrics.report.interval|5s|0.7.0|How often should report metrics to json/console. no effect on JMX
spark.kyuubi.<br />metrics.report.location|${KYUUBI_HOME}/metrics/report.json|0.7.0|Where the json metrics file located


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
