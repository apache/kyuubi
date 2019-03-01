/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ui

import scala.collection.mutable.HashMap

import org.apache.spark.SparkException
import org.apache.spark.ui.KyuubiSessionTab


object KyuubiServerMonitor {

  private[this] val uiTabs = new HashMap[String, KyuubiSessionTab]()

  private[this] val listeners = new HashMap[String, KyuubiServerListener]()

  def setListener(user: String, sparkListener: KyuubiServerListener): Unit = {
    listeners.put(user, sparkListener)
  }

  def getListener(user: String): Option[KyuubiServerListener] = {
    listeners.get(user)
  }

  def addUITab(user: String, ui: KyuubiSessionTab): Unit = {
    uiTabs.put(user, ui)
  }

  def detachUITab(user: String): Unit = {
    listeners.remove(user)
    uiTabs.get(user).foreach(_.detach())
  }

  def detachAllUITabs(): Unit = {
    uiTabs.values.foreach(_.detach())
  }
}
