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

package org.apache.spark.kyuubi.lineage

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.plugin.lineage.LineageDispatcherType

object LineageConf {

  val SKIP_PARSING_PERMANENT_VIEW_ENABLED =
    ConfigBuilder("spark.kyuubi.plugin.lineage.skip.parsing.permanent.view.enabled")
      .doc("Whether to skip the lineage parsing of permanent views")
      .version("1.8.0")
      .booleanConf
      .createWithDefault(false)

  val DISPATCHERS = ConfigBuilder("spark.kyuubi.plugin.lineage.dispatchers")
    .doc("The lineage dispatchers are implementations of " +
      "`org.apache.kyuubi.plugin.lineage.LineageDispatcher` for dispatching lineage events.<ul>" +
      "<li>SPARK_EVENT: send lineage event to spark event bus</li>" +
      "<li>KYUUBI_EVENT: send lineage event to kyuubi event bus</li>" +
      "<li>ATLAS: send lineage to apache atlas</li>" +
      "</ul>")
    .version("1.8.0")
    .stringConf
    .toSequence
    .checkValue(
      _.toSet.subsetOf(LineageDispatcherType.values.map(_.toString)),
      "Unsupported lineage dispatchers")
    .createWithDefault(Seq(LineageDispatcherType.SPARK_EVENT.toString))

  val LEGACY_COLLECT_INPUT_TABLES_ENABLED =
    ConfigBuilder("spark.kyuubi.plugin.lineage.legacy.collectInputTablesByColumn")
      .doc(" When true collect input tables by column lineage." +
        " When false collect all the input tables by the plan.")
      .version("1.11.0")
      .booleanConf
      .createWithDefault(false)

  val DEFAULT_CATALOG: String = SQLConf.get.getConf(SQLConf.DEFAULT_CATALOG)

}
