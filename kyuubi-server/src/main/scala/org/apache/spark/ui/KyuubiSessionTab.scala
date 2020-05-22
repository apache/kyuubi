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

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.ui.KyuubiSessionTab._

import yaooqinn.kyuubi.ui.{KyuubiServerListener, KyuubiServerMonitor}

/**
 * Spark Web UI tab that shows statistics of jobs running in the thrift server.
 * This assumes the given SparkContext has enabled its SparkUI.
 */
class KyuubiSessionTab(userName: String, sparkContext: SparkContext)
  extends SparkUITab(getSparkUI(sparkContext), userName) {

  override val name = s"Kyuubi Tab 4 $userName"

  val parent = getSparkUI(sparkContext)

  // KyuubiSessionTab renders by different listener's content, identified by user.
  val listener = KyuubiServerMonitor.getListener(userName).getOrElse {
    val lr = new KyuubiServerListener(sparkContext.conf)
    KyuubiServerMonitor.setListener(userName, lr)
    lr
  }

  attachPage(new KyuubiSessionPage(this))
  attachPage(new KyuubiSessionSubPage(this))
  parent.attachTab(this)

  def detach() {
    getSparkUI(sparkContext).detachTab(this)
  }
}

object KyuubiSessionTab {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
