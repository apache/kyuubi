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

package org.apache.kyuubi.server

import java.util.concurrent.TimeUnit

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.ThreadUtils
import org.apache.kyuubi.util.ThreadUtils.{newDaemonSingleThreadScheduledExecutor, scheduleTolerableRunnableWithFixedDelay}

class PeriodicGCService(name: String) extends AbstractService(name) {
  def this() = this(classOf[PeriodicGCService].getSimpleName)

  private lazy val gcTrigger = newDaemonSingleThreadScheduledExecutor("periodic-gc-trigger")

  override def start(): Unit = {
    startGcTrigger()
    super.start()
  }

  override def stop(): Unit = {
    super.stop()
    ThreadUtils.shutdown(gcTrigger)
  }

  private def startGcTrigger(): Unit = {
    val interval = conf.get(KyuubiConf.SERVER_PERIODIC_GC_INTERVAL)
    if (interval > 0) {
      scheduleTolerableRunnableWithFixedDelay(
        gcTrigger,
        () => System.gc(),
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
  }
}
