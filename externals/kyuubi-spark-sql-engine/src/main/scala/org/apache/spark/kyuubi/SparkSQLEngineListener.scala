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
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec

import org.apache.spark.SparkException
import org.apache.spark.scheduler._

import org.apache.kyuubi.KyuubiSparkUtils.KYUUBI_STATEMENT_ID_KEY
import org.apache.kyuubi.Logging
import org.apache.kyuubi.Utils.stringifyException
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.events.{EngineEventsStore, SessionEvent}
import org.apache.kyuubi.engine.spark.monitor.KyuubiStatementMonitor
import org.apache.kyuubi.engine.spark.monitor.entity.KyuubiJobInfo
import org.apache.kyuubi.ha.client.EngineServiceDiscovery
import org.apache.kyuubi.service.{Serverable, ServiceState}

/**
 * A [[SparkListener]] for engine level events handling
 *
 * @param server the corresponding engine
 */
class SparkSQLEngineListener(
    server: Serverable,
    store: EngineEventsStore) extends SparkListener with Logging {

  // the conf of server is null before initialized, use lazy val here
  private lazy val deregisterExceptions: Seq[String] =
    server.getConf.get(ENGINE_DEREGISTER_EXCEPTION_CLASSES)
  private lazy val deregisterMessages: Seq[String] =
    server.getConf.get(ENGINE_DEREGISTER_EXCEPTION_MESSAGES)
  private lazy val deregisterExceptionTTL: Long =
    server.getConf.get(ENGINE_DEREGISTER_EXCEPTION_TTL)
  private lazy val jobMaxFailures: Int =
    server.getConf.get(ENGINE_DEREGISTER_JOB_MAX_FAILURES)

  private val jobFailureNum = new AtomicInteger(0)
  @volatile private var lastFailureTime: Long = 0

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    server.getServiceState match {
      case ServiceState.STOPPED => debug("Received ApplicationEnd Message form Spark after the" +
        " engine has stopped")
      case state =>
        info(s"Received ApplicationEnd Message from Spark at $state, stopping")
        server.stop()
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val statementId = jobStart.properties.getProperty(KYUUBI_STATEMENT_ID_KEY)
    val kyuubiJobInfo = KyuubiJobInfo(
      jobStart.jobId, statementId, jobStart.stageIds, jobStart.time)
    KyuubiStatementMonitor.putJobInfoIntoMap(kyuubiJobInfo)
    debug(s"Add jobStartInfo. Query [$statementId]: Job ${jobStart.jobId} started with " +
      s"${jobStart.stageIds.length} stages")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    KyuubiStatementMonitor.insertJobEndTimeAndResult(jobEnd)
    info(s"Job end. Job ${jobEnd.jobId} state is ${jobEnd.jobResult.toString}")
    jobEnd.jobResult match {
     case JobFailed(e) if e != null =>
       val cause = findCause(e)
       var deregisterInfo: Option[String] = None
       if (deregisterExceptions.exists(_.equals(cause.getClass.getCanonicalName))) {
         deregisterInfo = Some("Job failed exception class is in the set of " +
           s"${ENGINE_DEREGISTER_EXCEPTION_CLASSES.key}, deregistering the engine.")
       } else if (deregisterMessages.exists(stringifyException(cause).contains)) {
         deregisterInfo = Some("Job failed exception message matches the specified " +
           s"${ENGINE_DEREGISTER_EXCEPTION_MESSAGES.key}, deregistering the engine.")
       }

       deregisterInfo.foreach { din =>
         val currentTime = System.currentTimeMillis()
         if (lastFailureTime == 0 || currentTime - lastFailureTime < deregisterExceptionTTL) {
           jobFailureNum.incrementAndGet()
         } else {
           info(s"It has been more than one deregister exception ttl [$deregisterExceptionTTL ms]" +
             " since last failure, restart counting.")
           jobFailureNum.set(1)
         }
         lastFailureTime = currentTime
         val curFailures = jobFailureNum.get()
         error(s"$din, current job failure number is [$curFailures]", e)
         if (curFailures >= jobMaxFailures) {
           error(s"Job failed $curFailures times; deregistering the engine")
           server.discoveryService.asInstanceOf[EngineServiceDiscovery].stop()
         }
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

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SessionEvent => updateSession(e)
      case _ => // Ignore
    }
  }

  private def updateSession(event: SessionEvent): Unit = {
    store.saveSession(event)
  }
}
