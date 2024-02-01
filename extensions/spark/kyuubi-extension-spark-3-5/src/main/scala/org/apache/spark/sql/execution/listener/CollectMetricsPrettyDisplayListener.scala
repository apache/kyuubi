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
package org.apache.spark.sql.execution.listener

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphClusterWrapper, SparkPlanGraphNode, SparkPlanGraphNodeWrapper, SparkPlanGraphWrapper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.status.ElementTrackingStore

import org.apache.kyuubi.sql.KyuubiSQLConf.COLLECT_METRICS_PRETTY_DISPLAY_ENABLED

private class CollectMetricsPrettyDisplayListener extends SparkListener {

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionEnd
          if e.qe.sparkSession.conf.get(COLLECT_METRICS_PRETTY_DISPLAY_ENABLED) =>
        val qe = e.qe
        if (qe.observedMetrics.nonEmpty) {
          val session = qe.sparkSession
          val executionId =
            Option(session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).map(
              _.toLong).getOrElse(e.executionId)

          val sparkPlanInfo = fromSparkPlan(qe.executedPlan)

          val planGraph = SparkPlanGraph(sparkPlanInfo)
          val graphToStore = new SparkPlanGraphWrapper(
            executionId,
            toStoredNodes(planGraph.nodes),
            planGraph.edges)

          val kvstore: ElementTrackingStore =
            session.sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
          kvstore.write(graphToStore)
        }
      case _ =>
    }
  }

  private def fromSparkPlan(plan: SparkPlan): SparkPlanInfo = {
    val children = plan match {
      case ReusedExchangeExec(_, child) => child :: Nil
      case ReusedSubqueryExec(child) => child :: Nil
      case a: AdaptiveSparkPlanExec => a.executedPlan :: Nil
      case stage: QueryStageExec => stage.plan :: Nil
      case inMemTab: InMemoryTableScanExec => inMemTab.relation.cachedPlan :: Nil
      case _ => plan.children ++ plan.subqueries
    }
    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }

    // dump the file scan metadata (e.g file path) to event log
    val metadata = plan match {
      case fileScan: FileSourceScanExec => fileScan.metadata
      case _ => Map[String, String]()
    }
    val simpleString = plan match {
      case c: CollectMetricsExec =>
        val metrics: Map[String, Any] =
          c.collectedMetrics.getValuesMap[Any](c.metricsSchema.fieldNames)
        val metricsString = redactMapString(metrics, SQLConf.get.maxToStringFields)
        s"CollectMetrics(${c.name}) $metricsString"
      case p => p.simpleString(SQLConf.get.maxToStringFields)
    }
    new SparkPlanInfo(
      plan.nodeName,
      simpleString,
      children.map(fromSparkPlan),
      metadata,
      metrics)
  }

  private def toStoredNodes(
      nodes: collection.Seq[SparkPlanGraphNode]): collection.Seq[SparkPlanGraphNodeWrapper] = {
    nodes.map {
      case cluster: SparkPlanGraphCluster =>
        val storedCluster = new SparkPlanGraphClusterWrapper(
          cluster.id,
          cluster.name,
          cluster.desc,
          toStoredNodes(cluster.nodes.toSeq),
          cluster.metrics)
        new SparkPlanGraphNodeWrapper(null, storedCluster)

      case node =>
        new SparkPlanGraphNodeWrapper(node, null)
    }
  }

  private def redactMapString(map: Map[String, Any], maxFields: Int): String = {
    // For security reason, redact the map value if the key is in certain patterns
    val redactedMap = SQLConf.get.redactOptions(map)
    // construct the redacted map as strings of the format "key=value"
    val keyValuePairs = redactedMap.toSeq.map { item =>
      item._1 + "=" + item._2
    }
    truncatedString(keyValuePairs, "[", ", ", "]", maxFields)
  }
}

object CollectMetricsPrettyDisplayListener {

  private val registered = new AtomicBoolean(false)

  def register(): Unit = {
    SparkContext.getActive.foreach { sc =>
      if (registered.compareAndSet(false, true)) {
        sc.addSparkListener(new CollectMetricsPrettyDisplayListener)
      }
    }
  }

}
