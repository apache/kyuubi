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

import org.apache.spark.sql.{FinalStageResourceManager, InjectCustomResourceProfile, SparkSessionExtensions}

import org.apache.kyuubi.sql.watchdog.{KyuubiUnsupportedOperationsCheck, MaxScanStrategy}
import org.apache.kyuubi.sql.zorder.{InsertZorderBeforeWritingDatasource, InsertZorderBeforeWritingHive, ResolveZorder}

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
    extensions.injectParser { case (_, parser) => new SparkKyuubiSparkSQLParser(parser) }
    extensions.injectResolutionRule(ResolveZorder)

    // Note that:
    // InsertZorderBeforeWriting* should be applied before RebalanceBeforeWriting*
    // because we can only apply one of them (i.e. GlobalSort or Rebalance)
    extensions.injectPostHocResolutionRule(InsertZorderBeforeWritingDatasource)
    extensions.injectPostHocResolutionRule(InsertZorderBeforeWritingHive)
    extensions.injectPostHocResolutionRule(FinalStageConfigIsolationCleanRule)
    extensions.injectPostHocResolutionRule(RebalanceBeforeWritingDatasource)
    extensions.injectPostHocResolutionRule(RebalanceBeforeWritingHive)
    extensions.injectPostHocResolutionRule(DropIgnoreNonexistent)

    // watchdog extension
    extensions.injectCheckRule(_ => KyuubiUnsupportedOperationsCheck)
    extensions.injectPlannerStrategy(MaxScanStrategy)

    extensions.injectQueryStagePrepRule(_ => InsertShuffleNodeBeforeJoin)
    extensions.injectQueryStagePrepRule(DynamicShufflePartitions)
    extensions.injectQueryStagePrepRule(FinalStageConfigIsolation(_))
    extensions.injectQueryStagePrepRule(FinalStageResourceManager(_))
    extensions.injectQueryStagePrepRule(InjectCustomResourceProfile)
  }
}
