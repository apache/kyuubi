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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.xml.{Node, Unparsed}

import org.apache.commons.text.StringEscapeUtils
import org.apache.spark.ui.TableSourceUtil._
import org.apache.spark.ui.UIUtils._

import org.apache.kyuubi.{KYUUBI_VERSION, Utils}
import org.apache.kyuubi.engine.spark.events.{SessionEvent, SparkOperationEvent}

case class EnginePage(parent: EngineTab) extends WebUIPage("") {
  private val store = parent.store

  override def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      generateBasicStats() ++
        <br/> ++
        stop(request) ++
        <br/> ++
        <h4>
        {store.getSessionCount} session(s) are online,
        running {store.getStatementCount}
        operations
      </h4> ++
        generateSessionStatsTable(request) ++
        generateStatementStatsTable(request)
    UIUtils.headerSparkPage(request, parent.name, content, parent)
  }

  private def generateBasicStats(): Seq[Node] = {
    val timeSinceStart = parent.endTime() - parent.startTime
    <ul class ="list-unstyled">
      <li>
        <strong>Kyuubi Version: </strong>
        {KYUUBI_VERSION}
      </li>
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
    {
      parent.engine.map { engine =>
        <li>
          <strong>Background execution pool threads alive: </strong>
          {engine.backendService.sessionManager.getExecPoolSize}
        </li>
        <li>
          <strong>Background execution pool threads active: </strong>
          {engine.backendService.sessionManager.getActiveCount}
        </li>
      }.getOrElse(Seq.empty)
    }
    </ul>
  }

  private def stop(request: HttpServletRequest): Seq[Node] = {
    val basePath = UIUtils.prependBaseUri(request, parent.basePath)
    if (parent.killEnabled) {
      val confirmForceStop =
        s"if (window.confirm('Are you sure you want to stop kyuubi engine immediately ?')) " +
          "{ this.parentNode.submit(); return true; } else { return false; }"
      val forceStopLinkUri = s"$basePath/kyuubi/stop"

      val confirmGracefulStop =
        s"if (window.confirm('Are you sure you want to stop kyuubi engine gracefully ?')) " +
          "{ this.parentNode.submit(); return true; } else { return false; }"
      val gracefulStopLinkUri = s"$basePath/kyuubi/gracefulstop"

      <ul class="list-unstyled">
        <li>
          <strong>Stop kyuubi engine:</strong>
          <a href={forceStopLinkUri} onclick={confirmForceStop} class="stop-link">
            (Stop Immediately)</a>
          <a href={gracefulStopLinkUri} onclick={confirmGracefulStop} class="stop-link">
            (Stop Gracefully)</a>
        </li>
      </ul>
    } else {
      Seq.empty
    }
  }

  /** Generate stats of statements for the engine */
  private def generateStatementStatsTable(request: HttpServletRequest): Seq[Node] = {

    val numStatement = store.getStatementList.size

    val table =
      if (numStatement > 0) {

        val sqlTableTag = "sqlstat"

        val sqlTablePage =
          Option(request.getParameter(s"$sqlTableTag.page")).map(_.toInt).getOrElse(1)

        try {
          Some(new StatementStatsPagedTable(
            request,
            parent,
            store.getStatementList,
            "kyuubi",
            UIUtils.prependBaseUri(request, parent.basePath),
            sqlTableTag).table(sqlTablePage))
        } catch {
          case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
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
      <span id="sqlstat" class="collapse-aggregated-sqlstat collapse-table"
            onClick="collapseTable('collapse-aggregated-sqlstat',
                'aggregated-sqlstat')">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>SQL Statistics ({numStatement})</a>
        </h4>
      </span> ++
        <div class="aggregated-sqlstat collapsible-table">
          {table.getOrElse("No statistics have been generated yet.")}
        </div>
    content
  }

  /** Generate stats of sessions for the engine */
  private def generateSessionStatsTable(request: HttpServletRequest): Seq[Node] = {
    val numSessions = store.getSessionList.size
    val table =
      if (numSessions > 0) {

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
            sessionTableTag).table(sessionTablePage))
        } catch {
          case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
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
          ("Client IP", true, None),
          ("Server IP", true, None),
          ("Session ID", true, None),
          ("Session Name", true, None),
          ("Start Time", true, None),
          ("Finish Time", true, None),
          ("Duration", true, None),
          ("Total Statements", true, None))

      headerStatRow(
        sessionTableHeadersAndTooltips,
        desc,
        pageSize,
        sortColumn,
        parameterPath,
        sessionStatsTableTag,
        sessionStatsTableTag)
    }

    override def row(session: SessionEvent): Seq[Node] = {
      val sessionLink = "%s/%s/session/?id=%s".format(
        UIUtils.prependBaseUri(request, parent.basePath),
        parent.prefix,
        session.sessionId)
      <tr>
        <td> {session.username} </td>
        <td> {session.ip} </td>
        <td> {session.serverIp} </td>
        <td> <a href={sessionLink}> {session.sessionId} </a> </td>
        <td> {session.name} </td>
        <td> {formatDate(session.startTime)} </td>
        <td> {if (session.endTime > 0) formatDate(session.endTime)} </td>
        <td> {formatDurationVerbose(session.duration)} </td>
        <td> {session.totalOperations} </td>
      </tr>
    }
  }

}

private class StatementStatsPagedTable(
    request: HttpServletRequest,
    parent: EngineTab,
    data: Seq[SparkOperationEvent],
    subPath: String,
    basePath: String,
    sqlStatsTableTag: String) extends PagedTable[SparkOperationEvent] {

  private val (sortColumn, desc, pageSize) =
    getRequestTableParameters(request, sqlStatsTableTag, "Create Time")

  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  private val parameterPath =
    s"$basePath/$subPath/?${getRequestParameterOtherTable(request, sqlStatsTableTag)}"

  override val dataSource = new StatementStatsTableDataSource(data, pageSize, sortColumn, desc)

  override def tableId: String = sqlStatsTableTag

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$sqlStatsTableTag.sort=$encodedSortColumn" +
      s"&$sqlStatsTableTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$sqlStatsTableTag"
  }

  override def pageSizeFormField: String = s"$sqlStatsTableTag.pageSize"

  override def pageNumberFormField: String = s"$sqlStatsTableTag.page"

  override def goButtonFormPath: String =
    s"$parameterPath&$sqlStatsTableTag.sort=$encodedSortColumn" +
      s"&$sqlStatsTableTag.desc=$desc#$sqlStatsTableTag"

  override def headers: Seq[Node] = {
    val sqlTableHeadersAndTooltips: Seq[(String, Boolean, Option[String])] =
      Seq(
        ("User", true, None),
        ("Statement ID", true, None),
        ("Create Time", true, None),
        ("Finish Time", true, None),
        ("Duration", true, None),
        ("Statement", true, None),
        ("State", true, None),
        ("Query Details", true, None),
        ("Failure Reason", true, None))

    headerStatRow(
      sqlTableHeadersAndTooltips,
      desc,
      pageSize,
      sortColumn,
      parameterPath,
      sqlStatsTableTag,
      sqlStatsTableTag)
  }

  override def row(event: SparkOperationEvent): Seq[Node] = {
    <tr>
      <td>
        {event.sessionUser}
      </td>
      <td>
        {event.statementId}
      </td>
      <td >
        {formatDate(event.createTime)}
      </td>
      <td>
        {if (event.completeTime > 0) formatDate(event.completeTime)}
      </td>
      <td >
        {formatDurationVerbose(event.duration)}
      </td>
      <td>
        <span class="description-input">
          {event.statement}
        </span>
      </td>
      <td>
        {event.state}
      </td>
      <td>
        {
      if (event.executionId.isDefined) {
        <a href={
          "%s/SQL/execution/?id=%s".format(
            UIUtils.prependBaseUri(request, parent.basePath),
            event.executionId.get)
        }>
          {event.executionId.get}
          </a>
      }
    }
      </td>
      {
      if (event.exception.isDefined) errorMessageCell(event.exception.get.getMessage) else <td></td>
    }
    </tr>
  }

  private def errorMessageCell(errorMessage: String): Seq[Node] = {
    val isMultiline = errorMessage.indexOf('\n') >= 0
    val errorSummary = StringEscapeUtils.escapeHtml4(
      if (isMultiline) {
        errorMessage.substring(0, errorMessage.indexOf('\n'))
      } else {
        errorMessage
      })
    val details = detailsUINode(isMultiline, errorMessage)
    <td>
      {errorSummary}{details}
    </td>
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
      case "Client IP" => Ordering.by(_.ip)
      case "Server IP" => Ordering.by(_.serverIp)
      case "Session ID" => Ordering.by(_.sessionId)
      case "Session Name" => Ordering.by(_.name)
      case "Start Time" => Ordering.by(_.startTime)
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

private class StatementStatsTableDataSource(
    info: Seq[SparkOperationEvent],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[SparkOperationEvent](pageSize) {

  // Sorting SessionEvent data
  private val data = info.sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[SparkOperationEvent] = data.slice(from, to)

  /**
   * Return Ordering according to sortColumn and desc.
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[SparkOperationEvent] = {
    val ordering: Ordering[SparkOperationEvent] = sortColumn match {
      case "User" => Ordering.by(_.sessionUser)
      case "Statement ID" => Ordering.by(_.statementId)
      case "Create Time" => Ordering.by(_.createTime)
      case "Finish Time" => Ordering.by(_.completeTime)
      case "Duration" => Ordering.by(_.duration)
      case "Statement" => Ordering.by(_.statement)
      case "State" => Ordering.by(_.state)
      case "Query Details" => Ordering.by(_.executionId)
      case "Failure Reason" => Ordering.by(_.exception.toString)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}

private object TableSourceUtil {

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
      sortColumn == defaultSortColumn)
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
