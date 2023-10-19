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

package org.apache.kyuubi.server.trino.api.v1

import java.util
import java.util.UUID
import javax.ws.rs._
import javax.ws.rs.core.{Context, HttpHeaders, MediaType, Response, UriInfo}
import javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE
import javax.ws.rs.core.Response.Status.{BAD_REQUEST, NOT_FOUND}

import scala.util.Try
import scala.util.control.NonFatal

import io.airlift.units.Duration
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import io.trino.client.QueryResults

import org.apache.kyuubi.Logging
import org.apache.kyuubi.jdbc.hive.Utils
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.server.trino.api.{ApiRequestContext, KyuubiTrinoOperationTranslator, Query, QueryId, Slug, TrinoContext}
import org.apache.kyuubi.server.trino.api.Slug.Context.{EXECUTING_QUERY, QUEUED_QUERY}
import org.apache.kyuubi.server.trino.api.v1.dto.Ok
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.sql.parser.trino.KyuubiTrinoFeParser
import org.apache.kyuubi.sql.plan.trino.{Deallocate, ExecuteForPreparing, Prepare}

@Tag(name = "Statement")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StatementResource extends ApiRequestContext with Logging {

  lazy val translator = new KyuubiTrinoOperationTranslator(fe.be)
  lazy val parser = new KyuubiTrinoFeParser()

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "test")
  @GET
  @Path("test")
  def test(): Ok = new Ok("trino server is running")

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[QueryResults]))),
    description =
      "Create a query")
  @POST
  @Path("/")
  @Consumes(Array(MediaType.TEXT_PLAIN))
  def query(
      statement: String,
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo): Response = {
    if (statement == null || statement.isEmpty) {
      throw badRequest(BAD_REQUEST, "SQL statement is empty")
    }

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)

    try {
      parser.parsePlan(statement) match {
        case Prepare(statementId, _) =>
          val query = Query(
            statementId,
            statement.split(s"$statementId FROM")(1),
            trinoContext,
            fe.be)
          val qr = query.getPrepareQueryResults(query.getLastToken, uriInfo)
          TrinoContext.buildTrinoResponse(qr, query.context)
        case ExecuteForPreparing(statementId, parameters) =>
          val parametersMap = new util.HashMap[Integer, String]()
          for (i <- parameters.indices) {
            parametersMap.put(i + 1, parameters(i))
          }
          trinoContext.preparedStatement.get(statementId).map { originSql =>
            val realSql = Utils.updateSql(originSql, parametersMap)
            val query = Query(realSql, trinoContext, translator, fe.be)
            val qr = query.getQueryResults(query.getLastToken, uriInfo)
            TrinoContext.buildTrinoResponse(qr, query.context)
          }.get
        case Deallocate(statementId) =>
          info(s"DEALLOCATE PREPARE ${statementId}")
          val query = Query(
            QueryId(new OperationHandle(UUID.randomUUID())),
            trinoContext,
            fe.be)
          val qr = query.getPrepareQueryResults(query.getLastToken, uriInfo)
          TrinoContext.buildTrinoResponse(qr, query.context)
        case _ =>
          val query = Query(statement, trinoContext, translator, fe.be)
          val qr = query.getQueryResults(query.getLastToken, uriInfo)
          TrinoContext.buildTrinoResponse(qr, query.context)
      }
    } catch {
      case e: Exception =>
        val errorMsg =
          s"Error submitting sql"
        error(errorMsg, e)
        throw badRequest(BAD_REQUEST, errorMsg + "\n" + e.getMessage)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Get queued statement status")
  @GET
  @Path("/queued/{queryId}/{slug}/{token}")
  def getQueuedStatementStatus(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long,
      @QueryParam("maxWait") maxWait: Duration,
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo): Response = {

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)
    val waitTime = if (maxWait == null) 0 else maxWait.toMillis
    getQuery(fe.be, trinoContext, QueryId(queryId), slug, token, QUEUED_QUERY)
      .flatMap(query =>
        Try(TrinoContext.buildTrinoResponse(
          query.getQueryResults(
            token,
            uriInfo,
            waitTime),
          query.context)))
      .recover {
        case NonFatal(e) =>
          val errorMsg =
            s"Error executing for query id $queryId"
          error(errorMsg, e)
          throw badRequest(NOT_FOUND, "Query not found")
      }.get
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Get executing statement status")
  @GET
  @Path("/executing/{queryId}/{slug}/{token}")
  def getExecutingStatementStatus(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long,
      @QueryParam("maxWait") maxWait: Duration,
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo): Response = {

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)
    val waitTime = if (maxWait == null) 0 else maxWait.toMillis
    getQuery(fe.be, trinoContext, QueryId(queryId), slug, token, EXECUTING_QUERY)
      .flatMap(query =>
        Try(TrinoContext.buildTrinoResponse(
          query.getQueryResults(token, uriInfo, waitTime),
          query.context)))
      .recover {
        case NonFatal(e) =>
          val errorMsg =
            s"Error executing for query id $queryId"
          error(errorMsg, e)
          throw badRequest(NOT_FOUND, "Query not found")
      }.get
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Cancel queued statement")
  @DELETE
  @Path("/queued/{queryId}/{slug}/{token}")
  def cancelQueuedStatement(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long,
      @Context headers: HttpHeaders): Response = {

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)
    getQuery(fe.be, trinoContext, QueryId(queryId), slug, token, QUEUED_QUERY)
      .flatMap(query => Try(query.cancel))
      .recover {
        case NonFatal(e) =>
          val errorMsg =
            s"Error executing for query id $queryId"
          error(errorMsg, e)
          throw badRequest(NOT_FOUND, "Query not found")
      }.get
    Response.noContent.build
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Cancel executing statement")
  @DELETE
  @Path("/executing/{queryId}/{slug}/{token}")
  def cancelExecutingStatementStatus(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long,
      @Context headers: HttpHeaders): Response = {

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)
    getQuery(fe.be, trinoContext, QueryId(queryId), slug, token, EXECUTING_QUERY)
      .flatMap(query => Try(query.cancel))
      .recover {
        case NonFatal(e) =>
          val errorMsg =
            s"Error executing for query id $queryId"
          error(errorMsg, e)
          throw badRequest(NOT_FOUND, "Query not found")
      }.get

    Response.noContent.build
  }

  private def getQuery(
      be: BackendService,
      context: TrinoContext,
      queryId: QueryId,
      slug: String,
      token: Long,
      slugContext: Slug.Context.Context): Try[Query] = {
    Try(be.sessionManager.operationManager.getOperation(queryId.operationHandle)).map { op =>
      val sessionWithId = context.session ++
        Map(Query.KYUUBI_SESSION_ID -> op.getSession.handle.identifier.toString)
      Query(queryId, context.copy(session = sessionWithId), be)
    }.filter(_.getSlug.isValid(slugContext, slug, token))
  }

  private def badRequest(status: Response.Status, message: String) =
    new WebApplicationException(
      Response.status(status)
        .`type`(TEXT_PLAIN_TYPE)
        .entity(message)
        .build)

}
