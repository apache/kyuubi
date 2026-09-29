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

package org.apache.spark.sql.execution

import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceProfileBuilder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.kyuubi.sql.KyuubiSQLConf._

/**
 * This node wraps the final executed plan and inject custom resource profile to the RDD.
 * It assumes that, the produced RDD would create the `ResultStage` in `DAGScheduler`,
 * so it makes resource isolation between previous and final stage.
 *
 * Note that, Spark does not support config `minExecutors` for each resource profile.
 * Which means, it would retain `minExecutors` for each resource profile.
 * So, suggest set `spark.dynamicAllocation.minExecutors` to 0 if enable this feature.
 */
case class CustomResourceProfileExec(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def supportsColumnar: Boolean = child.supportsColumnar
  override def supportsRowBased: Boolean = child.supportsRowBased
  override protected def doCanonicalize(): SparkPlan = child.canonicalized

  private val executorCores = conf.getConf(FINAL_WRITE_STAGE_EXECUTOR_CORES).getOrElse(
    sparkContext.getConf.getInt("spark.executor.cores", 1))
  private val executorMemory = conf.getConf(FINAL_WRITE_STAGE_EXECUTOR_MEMORY).getOrElse(
    sparkContext.getConf.get("spark.executor.memory", "2G"))
  private val executorMemoryOverhead =
    conf.getConf(FINAL_WRITE_STAGE_EXECUTOR_MEMORY_OVERHEAD)
      .getOrElse(sparkContext.getConf.get("spark.executor.memoryOverhead", "1G"))
  private val executorOffHeapMemory =
    if (sparkContext.getConf.getBoolean("spark.memory.offHeap.enabled", false)) {
      conf.getConf(FINAL_WRITE_STAGE_EXECUTOR_OFF_HEAP_MEMORY)
    } else {
      None
    }

  override lazy val metrics: Map[String, SQLMetric] = {
    val base = Map(
      "executorCores" -> SQLMetrics.createMetric(sparkContext, "executor cores"),
      "executorMemory" -> SQLMetrics.createMetric(sparkContext, "executor memory (MiB)"),
      "executorMemoryOverhead" -> SQLMetrics.createMetric(
        sparkContext,
        "executor memory overhead (MiB)"))
    val addition = executorOffHeapMemory.map(_ =>
      "executorOffHeapMemory" ->
        SQLMetrics.createMetric(sparkContext, "executor off heap memory (MiB)")).toMap
    base ++ addition
  }

  private def wrapResourceProfile[T](rdd: RDD[T]): RDD[T] = {
    if (Utils.isTesting) {
      // do nothing for local testing
      return rdd
    }

    metrics("executorCores") += executorCores
    metrics("executorMemory") += JavaUtils.byteStringAs(executorMemory, ByteUnit.MiB)
    metrics("executorMemoryOverhead") += JavaUtils.byteStringAs(
      executorMemoryOverhead,
      ByteUnit.MiB)
    executorOffHeapMemory.foreach(m =>
      metrics("executorOffHeapMemory") += JavaUtils.byteStringAs(m, ByteUnit.MiB))

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

    val resourceProfileBuilder = new ResourceProfileBuilder()
    val executorResourceRequests = new ExecutorResourceRequests()
    executorResourceRequests.cores(executorCores)
    executorResourceRequests.memory(executorMemory)
    executorResourceRequests.memoryOverhead(executorMemoryOverhead)
    executorOffHeapMemory.foreach(executorResourceRequests.offHeapMemory)
    resourceProfileBuilder.require(executorResourceRequests)
    rdd.withResources(resourceProfileBuilder.build())
    rdd
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()
    wrapResourceProfile(rdd)
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val rdd = child.executeColumnar()
    wrapResourceProfile(rdd)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    this.copy(child = newChild)
  }
}
