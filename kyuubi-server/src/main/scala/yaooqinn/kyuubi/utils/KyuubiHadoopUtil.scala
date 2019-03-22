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

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.KyuubiSparkUtil

import yaooqinn.kyuubi.Logging

private[kyuubi] object KyuubiHadoopUtil extends Logging {

  // YarnClient is thread safe. Create once, share it across threads.
  private lazy val yarnClient = {
    val c = YarnClient.createYarnClient()
    c.init(new YarnConfiguration())
    c.start()
    c
  }

  def killYarnApp(report: ApplicationReport): Unit = {
    try {
      yarnClient.killApplication(report.getApplicationId)
    } catch {
      case e: Exception => error("Failed to kill Application: " + report.getApplicationId, e)
    }
  }

  def getApplications: Seq[ApplicationReport] = {
    yarnClient.getApplications(Set("SPARK").asJava).asScala.filter { p =>
      p.getYarnApplicationState match {
        case ACCEPTED | NEW | NEW_SAVING | SUBMITTED | RUNNING => true
        case _ => false
      }
    }
  }

  def killYarnAppByName(appName: String): Unit = {
    getApplications.filter(app => app.getName.equals(appName)).foreach(killYarnApp)
  }

  def doAs[T](user: UserGroupInformation)(f: => T): T = {
    try {
      user.doAs(new PrivilegedExceptionAction[T] {
        override def run(): T = f
      })
    } catch {
      case NonFatal(e) => throw KyuubiSparkUtil.findCause(e)
    }
  }

  def doAsAndLogNonFatal(user: UserGroupInformation)(f: => Unit): Unit = {
    try {
      doAs(user)(f)
    } catch {
      case NonFatal(e) =>
        error(s"Failed to operate as ${user.getShortUserName}", e)
    }
  }

  /**
   * Run some code as the real logged in user (which may differ from the current user, for
   * example, when using proxying).
   */
  def doAsRealUser[T](f: => T): T = {
    val currentUser = UserGroupInformation.getCurrentUser
    val realUser = Option(currentUser.getRealUser).getOrElse(currentUser)
    doAs(realUser)(f)
  }
}
