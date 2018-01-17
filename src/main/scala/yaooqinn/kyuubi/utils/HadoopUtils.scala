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

package yaooqinn.kyuubi.utils

import scala.collection.JavaConverters._

import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

object HadoopUtils {

  // YarnClient is thread safe. Create once, share it across threads.
  private[this] lazy val yarnClient = {
    val c = YarnClient.createYarnClient()
    c.init(new YarnConfiguration())
    c.start()
    c
  }
  private[this] val appType = Set("SPARK").asJava

  private[this] def killYarnApp(report: ApplicationReport): Unit = {
    yarnClient.killApplication(report.getApplicationId)
  }

  def listYarnApps(): Seq[ApplicationReport] = {
    yarnClient.getApplications(appType).asScala
  }

  def killYarnAppByName(appName: String): Unit = {
    listYarnApps().filter(app => app.getName.equals(appName)).foreach(killYarnApp)
  }
}
