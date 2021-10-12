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

package org.apache.spark.kyuubi.ui

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.xml.{Node, Unparsed}

import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils, WebUIPage}
import org.apache.spark.ui.UIUtils._

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.spark.events.SessionEvent

case class EnginePage(parent: EngineTab) extends WebUIPage("") {
  private val store = parent.store

  override def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      generateBasicStats() ++
      <br/> ++
      stop(request) ++
      <br/> ++
      <h4>
        {parent.engine.backendService.sessionManager.getOpenSessionCount} session(s) are online,
        running {parent.engine.backendService.sessionManager.operationManager.getOperationCount}
        operations
      </h4> ++
      generateSessionStatsTable(request)
    UIUtils.headerSparkPage(request, parent.name, content, parent)
  }

  private def generateBasicStats(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - parent.engine.getStartTime
    <ul class ="list-unstyled">
      <li>
        <strong>Started at: </strong>
        {new Date(parent.engine.getStartTime)}
      </li>
      <li>
        <strong>Latest Logout at: </strong>
        {new Date(parent.engine.backendService.sessionManager.latestLogoutTime)}
      </li>
      <li>
        <strong>Time since start: </strong>
        {formatDurationVerbose(timeSinceStart)}
      </li>
      <li>
        <strong>Background execution pool threads alive: </strong>
        {parent.engine.backendService.sessionManager.getExecPoolSize}
      </li>
      <li>
        <strong>Background execution pool threads active: </strong>
        {parent.engine.backendService.sessionManager.getActiveCount}
      </li>
    </ul>
  }

  private def stop(request: HttpServletRequest): Seq[Node] = {
    val basePath = UIUtils.prependBaseUri(request, parent.basePath)
    if (parent.killEnabled) {
      val confirm =
        s"if (window.confirm('Are you sure you want to kill kyuubi engine ?')) " +
          "{ this.parentNode.submit(); return true; } else { return false; }"
      val stopLinkUri = s"$basePath/kyuubi/stop"
      <ul class ="list-unstyled">
        <li>
          <strong>Stop kyuubi engine:  </strong>
          <a href={stopLinkUri} onclick={confirm} class="stop-link">(kill)</a>
        </li>
      </ul>
    } else {
      Seq.empty
    }
  }

  /** Generate stats of sessions for the engine */
  private def generateSessionStatsTable(request: HttpServletRequest): Seq[Node] = {
    val numSessions = store.getSessionList.size
    val table = if (numSessions > 0) {

      val sessionTableTag = "sessionstat"

      val sessionTablePage =
        Option(request.getParameter(s"$sessionTableTag.page")).map(_.toInt).getOrElse(1)

      try {
        Some(new SessionStatsPagedTable(
          request,
          parent,
          store.getSessionList,
          "kyuubi",
          UIUtils.prependBaseUri(request, parent.basePath),
          sessionTableTag
        ).table(sessionTablePage))
      } catch {
        case e@(_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
          Some(<div class="alert alert-error">
            <p>Error while rendering job table:</p>
            <pre>
              {Utils.stringifyException(e)}
            </pre>
          </div>)
      }
    } else {
      None
    }

    val content =
      <span id="sessionstat" class="collapse-aggregated-sessionstat collapse-table"
            onClick="collapseTable('collapse-aggregated-sessionstat',
                'aggregated-sessionstat')">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Session Statistics ({numSessions})</a>
        </h4>
      </span> ++
        <div class="aggregated-sessionstat collapsible-table">
          {table.getOrElse("No statistics have been generated yet.")}
        </div>

    content
  }

  private class SessionStatsPagedTable(
      request: HttpServletRequest,
      parent: EngineTab,
      data: Seq[SessionEvent],
      subPath: String,
      basePath: String,
      sessionStatsTableTag: String) extends PagedTable[SessionEvent] {

    private val (sortColumn, desc, pageSize) =
      getRequestTableParameters(request, sessionStatsTableTag, "Start Time")

    private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

    private val parameterPath =
      s"$basePath/$subPath/?${getRequestParameterOtherTable(request, sessionStatsTableTag)}"

    override val dataSource = new SessionStatsTableDataSource(data, pageSize, sortColumn, desc)

    override def tableId: String = sessionStatsTableTag

    override def tableCssClass: String =
      "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

    override def pageLink(page: Int): String = {
      parameterPath +
        s"&$pageNumberFormField=$page" +
        s"&$sessionStatsTableTag.sort=$encodedSortColumn" +
        s"&$sessionStatsTableTag.desc=$desc" +
        s"&$pageSizeFormField=$pageSize" +
        s"#$sessionStatsTableTag"
    }

    override def pageSizeFormField: String = s"$sessionStatsTableTag.pageSize"

    override def pageNumberFormField: String = s"$sessionStatsTableTag.page"

    override def goButtonFormPath: String =
      s"$parameterPath&$sessionStatsTableTag.sort=$encodedSortColumn" +
        s"&$sessionStatsTableTag.desc=$desc#$sessionStatsTableTag"

    override def headers: Seq[Node] = {
      val sessionTableHeadersAndTooltips: Seq[(String, Boolean, Option[String])] =
        Seq(
          ("User", true, None),
          ("IP", true, None),
          ("Session ID", true, None),
          ("Start Time", true, None),
          ("Finish Time", true, None),
          ("Duration", true, None),
          ("Total Statements", true, None))

      headerStatRow(sessionTableHeadersAndTooltips, desc, pageSize, sortColumn,
        parameterPath, sessionStatsTableTag, sessionStatsTableTag)
    }

    override def row(session: SessionEvent): Seq[Node] = {
      val sessionLink = "%s/%s/session/?id=%s".format(
        UIUtils.prependBaseUri(request, parent.basePath), parent.prefix, session.sessionId)
      <tr>
        <td> {session.username} </td>
        <td> {session.ip} </td>
        <td> <a href={sessionLink}> {session.sessionId} </a> </td>
        <td> {formatDate(session.startTime)} </td>
        <td> {if (session.endTime > 0) formatDate(session.endTime)} </td>
        <td> {formatDurationVerbose(session.totalTime)} </td>
        <td> {session.totalOperations} </td>
      </tr>
    }
  }

  /**
   * Returns parameter of this table.
   */
  def getRequestTableParameters(
      request: HttpServletRequest,
      tableTag: String,
      defaultSortColumn: String): (String, Boolean, Int) = {
    val parameterSortColumn = request.getParameter(s"$tableTag.sort")
    val parameterSortDesc = request.getParameter(s"$tableTag.desc")
    val parameterPageSize = request.getParameter(s"$tableTag.pageSize")
    val sortColumn = Option(parameterSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse(defaultSortColumn)
    val desc = Option(parameterSortDesc).map(_.toBoolean).getOrElse(
      sortColumn == defaultSortColumn
    )
    val pageSize = Option(parameterPageSize).map(_.toInt).getOrElse(100)

    (sortColumn, desc, pageSize)
  }

  /**
   * Returns parameters of other tables in the page.
   */
  def getRequestParameterOtherTable(request: HttpServletRequest, tableTag: String): String = {
    request.getParameterMap.asScala
      .filterNot(_._1.startsWith(tableTag))
      .map(parameter => parameter._1 + "=" + parameter._2(0))
      .mkString("&")
  }

  def headerStatRow(
      headerInfo: Seq[(String, Boolean, Option[String])],
      desc: Boolean,
      pageSize: Int,
      sortColumn: String,
      parameterPath: String,
      tableTag: String,
      headerId: String): Seq[Node] = {
    val row: Seq[Node] = {
      headerInfo.map { case (header, sortable, tooltip) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$tableTag.desc=${!desc}" +
              s"&$tableTag.pageSize=$pageSize" +
              s"#$headerId")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          <th>
            <a href={headerLink}>
              <span data-toggle="tooltip" data-placement="top" title={tooltip.getOrElse("")}>
                {header}&nbsp;{Unparsed(arrow)}
              </span>
            </a>
          </th>
        } else {
          if (sortable) {
            val headerLink = Unparsed(
              parameterPath +
                s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
                s"&$tableTag.pageSize=$pageSize" +
                s"#$headerId")

            <th>
              <a href={headerLink}>
                <span data-toggle="tooltip" data-placement="top" title={tooltip.getOrElse("")}>
                  {header}
                </span>
              </a>
            </th>
          } else {
            <th>
              <span data-toggle="tooltip" data-placement="top" title={tooltip.getOrElse("")}>
                {header}
              </span>
            </th>
          }
        }
      }
    }
    <thead>
      <tr>{row}</tr>
    </thead>
  }
}

private class SessionStatsTableDataSource(
    info: Seq[SessionEvent],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[SessionEvent](pageSize) {

  // Sorting SessionEvent data
  private val data = info.sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[SessionEvent] = data.slice(from, to)

  /**
   * Return Ordering according to sortColumn and desc.
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[SessionEvent] = {
    val ordering: Ordering[SessionEvent] = sortColumn match {
      case "User" => Ordering.by(_.username)
      case "IP" => Ordering.by(_.ip)
      case "Session ID" => Ordering.by(_.sessionId)
      case "Start Time" => Ordering by (_.startTime)
      case "Finish Time" => Ordering.by(_.endTime)
      case "Duration" => Ordering.by(_.duration)
      case "Total Statements" => Ordering.by(_.totalOperations)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
