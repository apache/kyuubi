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

<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Auxiliary SQL extension for Spark SQL

Kyuubi provides SQL extension out of box. Due to the version compatibility with Apache Spark, currently we only support Apache Spark branch-3.1 (i.e 3.1.1 and 3.1.2).
And don't worry, Kyuubi will support the new Apache Spark version in future. Thanks to the adaptive query execution framework (AQE), Kyuubi can do these optimization.

## What feature does Kyuubi SQL extension provide
- merging small files automatically
  
  Small files is a long time issue with Apache Spark. Kyuubi can merge small files by adding an extra shuffle.
  Currently, Kyuubi supports handle small files with datasource table and hive table, and also Kyuubi support optimize dynamic partition insertion.
  For example, a common write query `INSERT INTO TABLE $table1 SELECT * FROM $table2`, Kyuubi will introduce an extra shuffle before write and then the small files will go away.


- insert shuffle node before Join to make AQE OptimizeSkewedJoin work

  In current implementation, Apache Spark can only optimize skewed join by the standard join which means a join must have two sort and shuffle node.
  However, in complex scenario this assuming will be broken easily. Kyuubi can guarantee the join is standard by adding an extra shuffle node before join.
  So that, OptimizeSkewedJoin can work better.


- stage level config isolation in AQE

  As we know, `spark.sql.adaptive.advisoryPartitionSizeInBytes` is a key config in Apache Spark AQE.
  It controls how big data size per-task should handle during shuffle, so we always use a 64MB or a smaller value to make parallelism enough.
  However, in general, we expect a file is big enough like 256MB or 512MB. Kyuubi can make the config isolation to solve the conflict so that
  we can make staging partition data size small and last partition data size big.


## How to use Kyuubi SQL extension

| Kyuubi Spark SQL extension | Supported Spark version(s) | Available since  | EOL              | Bundled in Binary release tarball | Maven profile
| -------------------------- | -------------------------- | ---------------- | ---------------- | --------------------------------- | -------------
| kyuubi-extension-spark-3-1 | 3.1.x                      | 1.3.0-incubating | N/A              | 1.3.0-incubating                  | spark-3.1
| kyuubi-extension-spark-3-2 | 3.2.x                      | 1.4.0-incubating | N/A              | 1.4.0-incubating                  | spark-3.2

1. Check the matrix that if you are using the supported Spark version, and find the cooperated Kyuubi Spark SQL Extension jar
2. Get the Kyuubi Spark SQL Extension jar
   1. Each Kyuubi binary release tarball only contains one default version of Kyuubi Spark SQL Extension jar, if you are looking for such version, you can find it under `$KYUUBI_HOME/extension`
   2. All supported versions of Kyuubi Spark SQL Extension jar will be deployed to [Maven Central](https://search.maven.org/search?q=kyuubi-extension-spark)
   3. If you like, you can compile Kyuubi Spark SQL Extension jar by yourself, please activate the cooperated Maven's profile on you compile command, i.e. you can get Kyuubi Spark SQL Extension jar for Spark 3.1 under `dev/kyuubi-extension-spark-3-1/target` when `-Pspark-3.1` is activated
3. Put the Kyuubi Spark SQL extension jar `kyuubi-extension-spark-*.jar` into `$SPARK_HOME/jars`
4. Enable `KyuubiSparkSQLExtension`, i.e. add a config into `$SPARK_HOME/conf/spark-defaults.conf`, `spark.sql.extensions=org.apache.kyuubi.sql.KyuubiSparkSQLExtension`

Now, you can enjoy the Kyuubi SQL Extension, and also Kyuubi provides some configs to make these feature easy to use.

Name | Default Value | Description | Since
--- | --- | --- | ---
spark.sql.optimizer.insertRepartitionBeforeWrite.enabled | true | Add repartition node at the top of query plan. An approach of merging small files. | 1.2.0
spark.sql.optimizer.insertRepartitionNum | none | The partition number if `spark.sql.optimizer.insertRepartitionBeforeWrite.enabled` is enabled. If AQE is disabled, the default value is `spark.sql.shuffle.partitions`. If AQE is enabled, the default value is none that means depend on AQE. | 1.2.0
spark.sql.optimizer.dynamicPartitionInsertionRepartitionNum | 100 | The partition number of each dynamic partition if `spark.sql.optimizer.insertRepartitionBeforeWrite.enabled` is enabled. We will repartition by dynamic partition columns to reduce the small file but that can cause data skew. This config is to extend the partition of dynamic partition column to avoid skew but may generate some small files. | 1.2.0
spark.sql.optimizer.forceShuffleBeforeJoin.enabled | false | Ensure shuffle node exists before shuffled join (shj and smj) to make AQE `OptimizeSkewedJoin` works (complex scenario join, multi table join). | 1.2.0
spark.sql.optimizer.finalStageConfigIsolation.enabled | false | If true, the final stage support use different config with previous stage. The prefix of final stage config key should be `spark.sql.finalStage.`. For example, the raw spark config: `spark.sql.adaptive.advisoryPartitionSizeInBytes`, then the final stage config should be: `spark.sql.finalStage.adaptive.advisoryPartitionSizeInBytes`. | 1.2.0
spark.sql.analyzer.classification.enabled | false | When true, allows Kyuubi engine to judge this SQL's classification and set `spark.sql.analyzer.classification` back into sessionConf. Through this configuration item, Spark can optimizing configuration dynamic. | 1.4.0
spark.sql.optimizer.insertZorderBeforeWriting.enabled | true | When true, we will follow target table properties to insert zorder or not. The key properties are: 1) `kyuubi.zorder.enabled`: if this property is true, we will insert zorder before writing data. 2) `kyuubi.zorder.cols`: string split by comma, we will zorder by these cols. | 1.4.0
spark.sql.watchdog.maxHivePartitions | none | Add maxHivePartitions Strategy to avoid scan excessive hive partitions on partitioned table, it's optional that works with defined. | 1.4.0
