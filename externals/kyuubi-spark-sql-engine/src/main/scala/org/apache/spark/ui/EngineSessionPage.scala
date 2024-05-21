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

import java.util.Date

import scala.collection.mutable
import scala.xml.Node

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SECRET_REDACTION_PATTERN
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Utils

import org.apache.kyuubi.engine.spark.events.SparkOperationEvent

/** Page for Spark Web UI that shows statistics of jobs running in the engine server */
abstract class EngineSessionPage(parent: EngineTab)
  extends WebUIPage("session") with Logging {
  val store = parent.store

  private def propertyHeader = Seq("Name", "Value")
  private def headerClasses = Seq("sorttable_alpha", "sorttable_alpha")
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>

  def dispatchRender(req: AnyRef): Seq[Node] = req match {
    case reqLike: HttpServletRequestLike =>
      this.render0(reqLike)
    case javaxReq: javax.servlet.http.HttpServletRequest =>
      this.render0(HttpServletRequestLike.fromJavax(javaxReq))
    case jakartaReq: jakarta.servlet.http.HttpServletRequest =>
      this.render0(HttpServletRequestLike.fromJakarta(jakartaReq))
    case unsupported =>
      throw new IllegalArgumentException(s"Unsupported class ${unsupported.getClass.getName}")
  }

  /** Render the page */
  def render0(request: HttpServletRequestLike): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val content = store.synchronized { // make sure all parts in this page are consistent
      val sessionStat = store.getSession(parameterId).orNull
      require(sessionStat != null, "Invalid sessionID[" + parameterId + "]")

      val redactionPattern = parent.sparkUI match {
        case Some(ui) => Some(ui.conf.get(SECRET_REDACTION_PATTERN))
        case None => SECRET_REDACTION_PATTERN.defaultValue
      }

      val sessionPropertiesTable =
        if (sessionStat.conf != null && sessionStat.conf.nonEmpty) {
          val table = UIUtils.listingTable(
            propertyHeader,
            propertyRow,
            Utils.redact(redactionPattern, sessionStat.conf.toSeq.sorted),
            fixedWidth = true,
            headerClasses = headerClasses)
          <span class="collapse-aggregated-kyuubiSessioinProperties collapse-table"
                onClick="collapseTable('collapse-aggregated-kyuubiSessioinProperties',
            'aggregated-kyuubiSessioinProperties')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Session Properties</a>
            </h4>
          </span>
          <div class="aggregated-kyuubiSessioinProperties collapsible-table">
            {table}
          </div>
        } else {
          Seq()
        }

      generateBasicStats() ++
        <br/> ++
        <h4>
          User {sessionStat.username},
          IP {sessionStat.ip},
          Server {sessionStat.serverIp}
        </h4> ++
        <h4>
          Session created at {formatDate(sessionStat.startTime)},
          {
          if (sessionStat.endTime > 0) {
            s"""
               | ended at ${formatDate(sessionStat.endTime)},
               | after ${formatDuration(sessionStat.duration)}.
               |""".stripMargin
          }
        }
          Total run {sessionStat.totalOperations} SQL
        </h4> ++
        sessionPropertiesTable ++
        generateSQLStatsTable(request, sessionStat.sessionId)
    }
    SparkUIUtils.headerSparkPage(request, parent.name + " Session", content, parent)
  }

  /** Generate basic stats of the engine server */
  private def generateBasicStats(): Seq[Node] =
    if (parent.engine.isDefined) {
      val timeSinceStart = parent.endTime() - parent.startTime
      <ul class ="list-unstyled">
        <li>
          <strong>Started at: </strong>
          {new Date(parent.startTime)}
        </li>
      {
        parent.engine.map { engine =>
          <li>
              <strong>Latest Logout at: </strong>
              {new Date(engine.backendService.sessionManager.latestLogoutTime)}
            </li>
        }.getOrElse(Seq.empty)
      }
        <li>
          <strong>Time since start: </strong>
          {formatDurationVerbose(timeSinceStart)}
        </li>
      </ul>
    } else {
      Seq.empty
    }

  /** Generate stats of batch statements of the engine server */
  private def generateSQLStatsTable(
      request: HttpServletRequestLike,
      sessionID: String): Seq[Node] = {
    val running = new mutable.ArrayBuffer[SparkOperationEvent]()
    val completed = new mutable.ArrayBuffer[SparkOperationEvent]()
    val failed = new mutable.ArrayBuffer[SparkOperationEvent]()

    store.getStatementList
      .filter(_.sessionId == sessionID)
      .foreach { op =>
        if (op.completeTime <= 0L) {
          running += op
        } else if (op.exception.isDefined) {
          failed += op
        } else {
          completed += op
        }
      }
    val content = mutable.ListBuffer[Node]()
    if (running.nonEmpty) {
      val sqlTableTag = "running-sqlstat"
      val table = statementStatsTable(request, sqlTableTag, parent, running.toSeq)
      content ++=
        <span id="running-sqlstat" class="collapse-aggregated-runningSqlstat collapse-table"
              onClick="collapseTable('collapse-aggregated-runningSqlstat',
              'aggregated-runningSqlstat')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Running Statement Statistics</a>
          </h4>
        </span> ++
          <div class="aggregated-runningSqlstat collapsible-table">
            {table}
          </div>
    }

    if (completed.nonEmpty) {
      val table = {
        val sqlTableTag = "completed-sqlstat"
        statementStatsTable(
          request,
          sqlTableTag,
          parent,
          completed.toSeq)
      }

      content ++=
        <span id="completed-sqlstat" class="collapse-aggregated-completedSqlstat collapse-table"
              onClick="collapseTable('collapse-aggregated-completedSqlstat',
              'aggregated-completedSqlstat')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Completed Statement Statistics (
              {completed.size}
              )</a>
          </h4>
        </span> ++
          <div class="aggregated-completedSqlstat collapsible-table">
            {table}
          </div>
    }

    if (failed.nonEmpty) {
      val table = {
        val sqlTableTag = "failed-sqlstat"
        statementStatsTable(
          request,
          sqlTableTag,
          parent,
          failed.toSeq)
      }

      content ++=
        <span id="failed-sqlstat" class="collapse-aggregated-failedSqlstat collapse-table"
              onClick="collapseTable('collapse-aggregated-failedSqlstat',
              'aggregated-failedSqlstat')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Failed Statement Statistics (
              {failed.size}
              )</a>
          </h4>
        </span> ++
          <div class="aggregated-failedSqlstat collapsible-table">
            {table}
          </div>
    }

    content
  }

  private def statementStatsTable(
      request: HttpServletRequestLike,
      sqlTableTag: String,
      parent: EngineTab,
      data: Seq[SparkOperationEvent]): Seq[Node] = {
    val sqlTablePage =
      Option(request.getParameter(s"$sqlTableTag.page")).map(_.toInt).getOrElse(1)

    try {
      new StatementStatsPagedTable(
        request,
        parent,
        data,
        "kyuubi/session",
        SparkUIUtils.prependBaseUri(request, parent.basePath),
        s"${sqlTableTag}").table(sqlTablePage)
    } catch {
      case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
        <div class="alert alert-error">
          <p>Error while rendering job table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }
}
