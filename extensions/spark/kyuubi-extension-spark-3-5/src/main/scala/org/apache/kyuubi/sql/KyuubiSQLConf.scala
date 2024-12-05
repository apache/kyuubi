/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.sql

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf._

object KyuubiSQLConf {

  val INSERT_REPARTITION_BEFORE_WRITE =
    buildConf("spark.sql.optimizer.insertRepartitionBeforeWrite.enabled")
      .doc("Add repartition node at the top of query plan. An approach of merging small files.")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)

  val FORCE_SHUFFLE_BEFORE_JOIN =
    buildConf("spark.sql.optimizer.forceShuffleBeforeJoin.enabled")
      .doc("Ensure shuffle node exists before shuffled join (shj and smj) to make AQE " +
        "`OptimizeSkewedJoin` works (complex scenario join, multi table join).")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(false)

  val FINAL_STAGE_CONFIG_ISOLATION =
    buildConf("spark.sql.optimizer.finalStageConfigIsolation.enabled")
      .doc("If true, the final stage support use different config with previous stage. " +
        "The prefix of final stage config key should be `spark.sql.finalStage.`." +
        "For example, the raw spark config: `spark.sql.adaptive.advisoryPartitionSizeInBytes`, " +
        "then the final stage config should be: " +
        "`spark.sql.finalStage.adaptive.advisoryPartitionSizeInBytes`.")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(false)

  val INSERT_ZORDER_BEFORE_WRITING =
    buildConf("spark.sql.optimizer.insertZorderBeforeWriting.enabled")
      .doc("When true, we will follow target table properties to insert zorder or not. " +
        "The key properties are: 1) kyuubi.zorder.enabled; if this property is true, we will " +
        "insert zorder before writing data. 2) kyuubi.zorder.cols; string split by comma, we " +
        "will zorder by these cols.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(true)

  val ZORDER_GLOBAL_SORT_ENABLED =
    buildConf("spark.sql.optimizer.zorderGlobalSort.enabled")
      .doc("When true, we do a global sort using zorder. Note that, it can cause data skew " +
        "issue if the zorder columns have less cardinality. When false, we only do local sort " +
        "using zorder.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(true)

  val REBALANCE_BEFORE_ZORDER =
    buildConf("spark.sql.optimizer.rebalanceBeforeZorder.enabled")
      .doc("When true, we do a rebalance before zorder in case data skew. " +
        "Note that, if the insertion is dynamic partition we will use the partition " +
        "columns to rebalance.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val REBALANCE_ZORDER_COLUMNS_ENABLED =
    buildConf("spark.sql.optimizer.rebalanceZorderColumns.enabled")
      .doc(s"When true and ${REBALANCE_BEFORE_ZORDER.key} is true, we do rebalance before " +
        s"Z-Order. If it's dynamic partition insert, the rebalance expression will include " +
        s"both partition columns and Z-Order columns.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val TWO_PHASE_REBALANCE_BEFORE_ZORDER =
    buildConf("spark.sql.optimizer.twoPhaseRebalanceBeforeZorder.enabled")
      .doc(s"When true and ${REBALANCE_BEFORE_ZORDER.key} is true, we do two phase rebalance " +
        s"before Z-Order for the dynamic partition write. The first phase rebalance using " +
        s"dynamic partition column; The second phase rebalance using dynamic partition column + " +
        s"Z-Order columns.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val ZORDER_USING_ORIGINAL_ORDERING_ENABLED =
    buildConf("spark.sql.optimizer.zorderUsingOriginalOrdering.enabled")
      .doc(s"When true and ${REBALANCE_BEFORE_ZORDER.key} is true, we do sort by " +
        s"the original ordering i.e. lexicographical order.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val REBALANCE_ADVISORY_PARTITION_SIZE_IN_BYTES =
    buildConf("spark.sql.optimizer.rebalanceAdvisoryPartitionSize")
      .doc("The advisory size in bytes of the shuffle partition during merge small files. " +
        s"It takes effect when Spark coalesces small shuffle partitions or splits skewed " +
        s"shuffle partition.")
      .version("1.10.1")
      .fallbackConf(ADVISORY_PARTITION_SIZE_IN_BYTES)

  val WATCHDOG_MAX_PARTITIONS =
    buildConf("spark.sql.watchdog.maxPartitions")
      .doc("Set the max partition number when spark scans a data source. " +
        "Enable maxPartitions Strategy by specifying this configuration. " +
        "Add maxPartitions Strategy to avoid scan excessive partitions " +
        "on partitioned table, it's optional that works with defined")
      .version("1.4.0")
      .intConf
      .createOptional

  val WATCHDOG_MAX_FILE_SIZE =
    buildConf("spark.sql.watchdog.maxFileSize")
      .doc("Set the maximum size in bytes of files when spark scans a data source. " +
        "Enable maxFileSize Strategy by specifying this configuration. " +
        "Add maxFileSize Strategy to avoid scan excessive size of files," +
        " it's optional that works with defined")
      .version("1.8.0")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val WATCHDOG_FORCED_MAXOUTPUTROWS =
    buildConf("spark.sql.watchdog.forcedMaxOutputRows")
      .doc("Add ForcedMaxOutputRows rule to avoid huge output rows of non-limit query " +
        "unexpectedly, it's optional that works with defined")
      .version("1.4.0")
      .intConf
      .createOptional

  val DROP_IGNORE_NONEXISTENT =
    buildConf("spark.sql.optimizer.dropIgnoreNonExistent")
      .doc("Do not report an error if DROP DATABASE/TABLE/VIEW/FUNCTION/PARTITION specifies " +
        "a non-existent database/table/view/function/partition")
      .version("1.5.0")
      .booleanConf
      .createWithDefault(false)

  val INFER_REBALANCE_AND_SORT_ORDERS =
    buildConf("spark.sql.optimizer.inferRebalanceAndSortOrders.enabled")
      .doc("When ture, infer columns for rebalance and sort orders from original query, " +
        "e.g. the join keys from join. It can avoid compression ratio regression.")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val INFER_REBALANCE_AND_SORT_ORDERS_MAX_COLUMNS =
    buildConf("spark.sql.optimizer.inferRebalanceAndSortOrdersMaxColumns")
      .doc("The max columns of inferred columns.")
      .version("1.7.0")
      .intConf
      .checkValue(_ > 0, "must be positive number")
      .createWithDefault(3)

  val INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE =
    buildConf("spark.sql.optimizer.insertRepartitionBeforeWriteIfNoShuffle.enabled")
      .doc("When true, add repartition even if the original plan does not have shuffle.")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val FINAL_STAGE_CONFIG_ISOLATION_WRITE_ONLY =
    buildConf("spark.sql.optimizer.finalStageConfigIsolationWriteOnly.enabled")
      .doc("When true, only enable final stage isolation for writing.")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(true)

  val FINAL_WRITE_STAGE_EAGERLY_KILL_EXECUTORS_ENABLED =
    buildConf("spark.sql.finalWriteStage.eagerlyKillExecutors.enabled")
      .doc("When true, eagerly kill redundant executors before running final write stage.")
      .version("1.8.0")
      .booleanConf
      .createWithDefault(false)

  val FINAL_WRITE_STAGE_EAGERLY_KILL_EXECUTORS_KILL_ALL =
    buildConf("spark.sql.finalWriteStage.eagerlyKillExecutors.killAll")
      .doc("When true, eagerly kill all executors before running final write stage. " +
        "Mainly for test.")
      .version("1.8.0")
      .booleanConf
      .createWithDefault(false)

  val FINAL_WRITE_STAGE_SKIP_KILLING_EXECUTORS_FOR_TABLE_CACHE =
    buildConf("spark.sql.finalWriteStage.skipKillingExecutorsForTableCache")
      .doc("When true, skip killing executors if the plan has table caches.")
      .version("1.8.0")
      .booleanConf
      .createWithDefault(true)

  val FINAL_WRITE_STAGE_PARTITION_FACTOR =
    buildConf("spark.sql.finalWriteStage.retainExecutorsFactor")
      .doc("If the target executors * factor < active executors, and " +
        "target executors * factor > min executors, then kill redundant executors.")
      .version("1.8.0")
      .doubleConf
      .checkValue(_ >= 1, "must be bigger than or equal to 1")
      .createWithDefault(1.2)

  val FINAL_WRITE_STAGE_RESOURCE_ISOLATION_ENABLED =
    buildConf("spark.sql.finalWriteStage.resourceIsolation.enabled")
      .doc(
        "When true, make final write stage resource isolation using custom RDD resource profile.")
      .version("1.8.0")
      .booleanConf
      .createWithDefault(false)

  val FINAL_WRITE_STAGE_EXECUTOR_CORES =
    buildConf("spark.sql.finalWriteStage.executorCores")
      .doc("Specify the executor core request for final write stage. " +
        "It would be passed to the RDD resource profile.")
      .version("1.8.0")
      .intConf
      .createOptional

  val FINAL_WRITE_STAGE_EXECUTOR_MEMORY =
    buildConf("spark.sql.finalWriteStage.executorMemory")
      .doc("Specify the executor on heap memory request for final write stage. " +
        "It would be passed to the RDD resource profile.")
      .version("1.8.0")
      .stringConf
      .createOptional

  val FINAL_WRITE_STAGE_EXECUTOR_MEMORY_OVERHEAD =
    buildConf("spark.sql.finalWriteStage.executorMemoryOverhead")
      .doc("Specify the executor memory overhead request for final write stage. " +
        "It would be passed to the RDD resource profile.")
      .version("1.8.0")
      .stringConf
      .createOptional

  val FINAL_WRITE_STAGE_EXECUTOR_OFF_HEAP_MEMORY =
    buildConf("spark.sql.finalWriteStage.executorOffHeapMemory")
      .doc("Specify the executor off heap memory request for final write stage. " +
        "It would be passed to the RDD resource profile.")
      .version("1.8.0")
      .stringConf
      .createOptional

  val DYNAMIC_SHUFFLE_PARTITIONS =
    buildConf("spark.sql.optimizer.dynamicShufflePartitions")
      .doc("If true, adjust the number of shuffle partitions dynamically based on the job" +
        " input size. The new number of partitions is the maximum input size" +
        " divided by `spark.sql.adaptive.advisoryPartitionSizeInBytes`.")
      .version("1.9.0")
      .booleanConf
      .createWithDefault(false)

  val DYNAMIC_SHUFFLE_PARTITIONS_MAX_NUM =
    buildConf("spark.sql.optimizer.dynamicShufflePartitions.maxNum")
      .doc("The maximum partition number of DynamicShufflePartitions.")
      .version("1.9.0")
      .intConf
      .createWithDefault(2000)

  val SCRIPT_TRANSFORMATION_ENABLED =
    buildConf("spark.sql.execution.scriptTransformation.enabled")
      .doc("When false, script transformation is not allowed.")
      .version("1.9.0")
      .booleanConf
      .createWithDefault(true)
}
