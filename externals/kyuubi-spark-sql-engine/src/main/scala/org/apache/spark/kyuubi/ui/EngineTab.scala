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

package org.apache.spark.kyuubi.ui

import org.apache.spark.SparkContext
import org.apache.spark.kyuubi.ui.EngineTab.ui
import org.apache.spark.ui.{SparkUI, SparkUITab}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.spark.SparkSQLEngine

/**
 * Note that [[SparkUITab]] is private for Spark
 */
case class EngineTab(engine: SparkSQLEngine) extends SparkUITab(ui, "kyuubi") {
  override val name: String = "Kyuubi Query Engine"
  if (ui != null) {
    this.attachPage(EnginePage(this))
    ui.attachTab(this)
    Utils.addShutdownHook(() => ui.detachTab(this))
  }
}

object EngineTab {
  lazy val ui: SparkUI = SparkContext.getActive.flatMap(_.ui).orNull
}
