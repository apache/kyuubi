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
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SECRET_REDACTION_PATTERN
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Utils

/** Page for Spark Web UI that shows statistics of jobs running in the engine server */
case class EngineSessionPage(parent: EngineTab)
  extends WebUIPage("session") with Logging {
  val store = parent.store

  private def propertyHeader = Seq("Name", "Value")
  private def headerClasses = Seq("sorttable_alpha", "sorttable_alpha")
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
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
    UIUtils.headerSparkPage(request, parent.name + " Session", content, parent)
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
  private def generateSQLStatsTable(request: HttpServletRequest, sessionID: String): Seq[Node] = {
    val executionList = store.getStatementList
      .filter(_.sessionId == sessionID)
    val numStatement = executionList.size
    val table =
      if (numStatement > 0) {

        val sqlTableTag = "sqlsessionstat"

        val sqlTablePage =
          Option(request.getParameter(s"$sqlTableTag.page")).map(_.toInt).getOrElse(1)

        try {
          Some(new StatementStatsPagedTable(
            request,
            parent,
            executionList,
            "kyuubi/session",
            UIUtils.prependBaseUri(request, parent.basePath),
            sqlTableTag).table(sqlTablePage))
        } catch {
          case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
            Some(<div class="alert alert-error">
            <p>Error while rendering job table:</p>
            <pre>
              {Utils.exceptionString(e)}
            </pre>
          </div>)
        }
      } else {
        None
      }
    val content =
      <span id="sqlsessionstat" class="collapse-aggregated-sqlsessionstat collapse-table"
            onClick="collapseTable('collapse-aggregated-sqlsessionstat',
                'aggregated-sqlsessionstat')">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Statement Statistics</a>
        </h4>
      </span> ++
        <div class="aggregated-sqlsessionstat collapsible-table">
          {table.getOrElse("No statistics have been generated yet.")}
        </div>

    content
  }
}
