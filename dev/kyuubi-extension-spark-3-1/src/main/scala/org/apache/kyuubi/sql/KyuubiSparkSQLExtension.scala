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

import org.apache.spark.sql.SparkSessionExtensions

import org.apache.kyuubi.sql.sqlclassification.KyuubiSqlClassification
import org.apache.kyuubi.sql.watchdog.{ForcedMaxOutputRowsRule, MarkAggregateOrderRule, MaxHivePartitionStrategy}
import org.apache.kyuubi.sql.zorder.{InsertZorderBeforeWritingDatasource, InsertZorderBeforeWritingHive}
import org.apache.kyuubi.sql.zorder.ResolveZorder
import org.apache.kyuubi.sql.zorder.ZorderSparkSqlExtensionsParser

// scalastyle:off line.size.limit
/**
 * Depend on Spark SQL Extension framework, we can use this extension follow steps
 *   1. move this jar into $SPARK_HOME/jars
 *   2. add config into `spark-defaults.conf`: `spark.sql.extensions=org.apache.kyuubi.sql.KyuubiSparkSQLExtension`
 */
// scalastyle:on line.size.limit
class KyuubiSparkSQLExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // inject zorder parser and related rules
    extensions.injectParser{ case (_, parser) => new ZorderSparkSqlExtensionsParser(parser) }
    extensions.injectResolutionRule(ResolveZorder)

    // a help rule for ForcedMaxOutputRowsRule
    extensions.injectResolutionRule(MarkAggregateOrderRule)

    // Note that:
    // InsertZorderBeforeWritingDatasource and InsertZorderBeforeWritingHive
    // should be applied before
    // RepartitionBeforeWrite and RepartitionBeforeWriteHive
    // because we can only apply one of them (i.e. Global Sort or Repartition)
    extensions.injectPostHocResolutionRule(InsertZorderBeforeWritingDatasource)
    extensions.injectPostHocResolutionRule(InsertZorderBeforeWritingHive)
    extensions.injectPostHocResolutionRule(KyuubiSqlClassification)
    extensions.injectPostHocResolutionRule(RepartitionBeforeWrite)
    extensions.injectPostHocResolutionRule(RepartitionBeforeWriteHive)
    extensions.injectPostHocResolutionRule(FinalStageConfigIsolationCleanRule)
    extensions.injectPostHocResolutionRule(ForcedMaxOutputRowsRule)

    extensions.injectQueryStagePrepRule(_ => InsertShuffleNodeBeforeJoin)
    extensions.injectQueryStagePrepRule(FinalStageConfigIsolation(_))
    extensions.injectPlannerStrategy(MaxHivePartitionStrategy)
  }
}
