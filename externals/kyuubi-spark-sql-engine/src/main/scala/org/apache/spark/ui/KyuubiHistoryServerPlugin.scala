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

package org.apache.spark.ui

import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.SparkSQLEngineEventListener
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.events.EngineEventsStore

// scalastyle:off line.size.limit
/**
 * HistoryServer plugin for Kyuubi, It can be used as a plugin in SparkHistoryServer to make SparkHistoryServer UI display Kyuubi's Tab.
 * We can use it like:
 *  - Copy the kyuubi-spark-sql-engine jar to $SPARK_HOME/jars and restart SparkHistoryServer.
 *  - In addition, we can add kyuubi configurations to spark-defaults.conf prefixed with "spark.kyuubi.".
 */
// scalastyle:on line.size.limit
class KyuubiHistoryServerPlugin extends AppHistoryServerPlugin {

  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    val kyuubiConf = mergedKyuubiConf(conf)
    Seq(new SparkSQLEngineEventListener(store, kyuubiConf))
  }

  override def setupUI(ui: SparkUI): Unit = {
    val store = new EngineEventsStore(ui.store.store)
    if (store.getSessionCount > 0) {
      val kyuubiConf = mergedKyuubiConf(ui.conf)
      kyuubiConf.set(KyuubiConf.ENGINE_UI_STOP_ENABLED, false)
      EngineTab(
        None,
        Some(ui),
        store,
        kyuubiConf)
    }
  }

  private def mergedKyuubiConf(sparkConf: SparkConf): KyuubiConf = {
    val kyuubiConf = KyuubiConf()
    val sparkToKyuubiPrefix = "spark.kyuubi."
    sparkConf.getAllWithPrefix(sparkToKyuubiPrefix).foreach { case (k, v) =>
      kyuubiConf.set(s"kyuubi.$k", v)
    }
    kyuubiConf
  }

}
