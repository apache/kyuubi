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

package yaooqinn.kyuubi.ui

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SparkConf, KyuubiSparkUtil}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.internal.SQLConf

class KyuubiServerListener(conf: SparkConf) extends SparkListener {

  private[this] var onlineSessionNum: Int = 0
  private[this] val sessionList = new mutable.LinkedHashMap[String, SessionInfo]
  private[this] val executionList = new mutable.LinkedHashMap[String, ExecutionInfo]
  private[this] val retainedStatements =
    conf.getInt(SQLConf.THRIFTSERVER_UI_STATEMENT_LIMIT.key, 200)
  private[this] val retainedSessions = conf.getInt(SQLConf.THRIFTSERVER_UI_SESSION_LIMIT.key, 200)
  private[this] var totalRunning = 0

  def getOnlineSessionNum: Int = synchronized { onlineSessionNum }

  def getTotalRunning: Int = synchronized { totalRunning }

  def getSessionList: Seq[SessionInfo] = synchronized { sessionList.values.toSeq }

  def getSession(sessionId: String): Option[SessionInfo] = synchronized {
    sessionList.get(sessionId)
  }

  def getExecutionList: Seq[ExecutionInfo] = synchronized { executionList.values.toSeq }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    for {
      props <- Option(jobStart.properties)
      groupIdKey <- props.stringPropertyNames().asScala.
        filter(_.startsWith(KyuubiSparkUtil.getJobGroupIDKey()))
      groupId <- Option(props.getProperty(groupIdKey))
      (_, info) <- executionList if info.groupId == groupId
    } {
      info.jobId += jobStart.jobId.toString
      info.groupId = groupId
    }
  }

  def onSessionCreated(ip: String, sessionId: String, userName: String = "UNKNOWN"): Unit = {
    synchronized {
      val info = new SessionInfo(sessionId, System.currentTimeMillis, ip, userName)
      sessionList.put(sessionId, info)
      onlineSessionNum += 1
      trimSessionIfNecessary()
    }
  }

  def onSessionClosed(sessionId: String): Unit = synchronized {
    sessionList(sessionId).finishTimestamp = System.currentTimeMillis
    onlineSessionNum -= 1
    trimSessionIfNecessary()
  }

  def onStatementStart(
      id: String,
      sessionId: String,
      statement: String,
      groupId: String,
      userName: String = "UNKNOWN"): Unit = synchronized {
    val info = new ExecutionInfo(statement, sessionId, System.currentTimeMillis, userName)
    info.state = ExecutionState.STARTED
    executionList.put(id, info)
    trimExecutionIfNecessary()
    sessionList(sessionId).totalExecution += 1
    executionList(id).groupId = groupId
    totalRunning += 1
  }

  def onStatementParsed(id: String, executionPlan: String): Unit = synchronized {
    executionList(id).executePlan = executionPlan
    executionList(id).state = ExecutionState.COMPILED
  }

  def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit = {
    synchronized {
      executionList(id).finishTimestamp = System.currentTimeMillis
      executionList(id).detail = errorMessage
      executionList(id).state = ExecutionState.FAILED
      totalRunning -= 1
      trimExecutionIfNecessary()
    }
  }

  def onStatementFinish(id: String): Unit = synchronized {
    executionList(id).finishTimestamp = System.currentTimeMillis
    executionList(id).state = ExecutionState.FINISHED
    totalRunning -= 1
    trimExecutionIfNecessary()
  }

  private def trimExecutionIfNecessary() = {
    if (executionList.size > retainedStatements) {
      val toRemove = math.max(retainedStatements / 10, 1)
      executionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
        executionList.remove(s._1)
      }
    }
  }

  private def trimSessionIfNecessary() = {
    if (sessionList.size > retainedSessions) {
      val toRemove = math.max(retainedSessions / 10, 1)
      sessionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
        sessionList.remove(s._1)
      }
    }
  }
}
