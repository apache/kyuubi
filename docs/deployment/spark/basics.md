```properties
## Spark Configurations, they will override those in $SPARK_HOME/conf/spark-defaults.conf
## Dummy Ones
# spark.master                      local
# spark.submit.deployMode           client
# spark.ui.enabled                  false
# spark.ui.port                     0
# spark.scheduler.mode              FAIR
# spark.serializer                  org.apache.spark.serializer.KryoSerializer
# spark.kryoserializer.buffer.max   128m
# spark.buffer.size                 131072
# spark.local.dir                   ./local
# spark.network.timeout             120s
# spark.cleaner.periodicGC.interval 10min
```