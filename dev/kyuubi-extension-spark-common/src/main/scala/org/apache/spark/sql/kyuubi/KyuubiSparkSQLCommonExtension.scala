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

package org.apache.spark.sql.kyuubi

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.kyuubi.command.{KyuubiCommandsSparkSqlExtensionsParser, ResolveKyuubiProcedures}
import org.apache.spark.sql.kyuubi.zorder.{InsertZorderBeforeWritingDatasource, InsertZorderBeforeWritingHive, ResolveZorder, ZorderSparkSqlExtensionsParser}

class KyuubiSparkSQLCommonExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    KyuubiSparkSQLCommonExtension.injectCommonExtensions(extensions)
  }
}

object KyuubiSparkSQLCommonExtension {
  def injectCommonExtensions(extensions: SparkSessionExtensions): Unit = {
    // inject zorder parser and related rules
    extensions.injectParser{ case (_, parser) => new ZorderSparkSqlExtensionsParser(parser) }
    extensions.injectResolutionRule(ResolveZorder)

    // inject kyuubi commands parser
    extensions.injectParser{ case (_, parser) => new KyuubiCommandsSparkSqlExtensionsParser(parser)}
    extensions.injectResolutionRule(ResolveKyuubiProcedures)

    // Note that:
    // InsertZorderBeforeWritingDatasource and InsertZorderBeforeWritingHive
    // should be applied before
    // RepartitionBeforeWriting and RebalanceBeforeWriting
    // because we can only apply one of them (i.e. Global Sort or Repartition/Rebalance)
    extensions.injectPostHocResolutionRule(InsertZorderBeforeWritingDatasource)
    extensions.injectPostHocResolutionRule(InsertZorderBeforeWritingHive)
    extensions.injectPostHocResolutionRule(FinalStageConfigIsolationCleanRule)

    extensions.injectQueryStagePrepRule(_ => InsertShuffleNodeBeforeJoin)
    extensions.injectQueryStagePrepRule(FinalStageConfigIsolation(_))
  }
}
