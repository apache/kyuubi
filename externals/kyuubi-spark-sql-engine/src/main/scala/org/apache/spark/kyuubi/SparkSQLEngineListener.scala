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

// Some object likes `JobFailed` is only accessible in org.apache.spark package
package org.apache.spark.kyuubi

import java.lang.reflect.{InvocationTargetException, UndeclaredThrowableException}

import scala.annotation.tailrec

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{JobFailed, SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_DEREGISTER_EXCEPTION_CLASSES, ENGINE_DEREGISTER_EXCEPTION_MESSAGES, ENGINE_DEREGISTER_EXCEPTION_STACKTRACES}
import org.apache.kyuubi.ha.client.EngineServiceDiscovery
import org.apache.kyuubi.service.{Serverable, ServiceState}

class SparkSQLEngineListener(server: Serverable) extends SparkListener with Logging {

  // the conf of server is null before initialized, use lazy val here
  lazy val deregisterExceptions = server.getConf.get(ENGINE_DEREGISTER_EXCEPTION_CLASSES)
  lazy val deregisterMessages = server.getConf.get(ENGINE_DEREGISTER_EXCEPTION_MESSAGES)
  lazy val deregisterStacktraces = server.getConf.get(ENGINE_DEREGISTER_EXCEPTION_STACKTRACES)

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    server.getServiceState match {
      case ServiceState.STOPPED => debug("Received ApplicationEnd Message form Spark after the" +
        " engine has stopped")
      case state =>
        info(s"Received ApplicationEnd Message from Spark at $state, stopping")
        server.stop()
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
   jobEnd.jobResult match {
     case JobFailed(e) if e != null =>
       val cause = findCause(e)
       var deregisterInfo: Option[String] = None
       if (deregisterExceptions.exists(_.equals(cause.getClass.getCanonicalName))) {
         deregisterInfo = Some("Job failed exception class is in the set of " +
           s"${ENGINE_DEREGISTER_EXCEPTION_CLASSES.key}, deregistering the engine.")
       } else if (cause.getMessage != null &&
         deregisterMessages.exists(cause.getMessage.contains)) {
         deregisterInfo = Some("Job failed exception message matches the specified " +
           s"${ENGINE_DEREGISTER_EXCEPTION_MESSAGES.key}, deregistering the engine.")
       } else if (cause.getStackTrace != null &&
         deregisterStacktraces.exists(cause.getStackTrace.mkString("\n").contains)) {
         deregisterInfo = Some("Job failed exception stacktrace matches the specified " +
           s"${ENGINE_DEREGISTER_EXCEPTION_STACKTRACES.key}, deregistering the engine.")
       }

       deregisterInfo.foreach { info =>
         error(info, cause)
         server.discoveryService.asInstanceOf[EngineServiceDiscovery].stop()
       }

     case _ =>
   }
  }

  @tailrec
  private def findCause(t: Throwable): Throwable = t match {
    case e @ (_: SparkException | _: UndeclaredThrowableException | _: InvocationTargetException)
      if e.getCause != null => findCause(e.getCause)
    case e => e
  }
}
