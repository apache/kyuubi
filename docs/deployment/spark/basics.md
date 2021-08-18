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