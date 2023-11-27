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

package org.apache.kyuubi.server.trino.api

import java.net.URI
import java.security.SecureRandom
import java.util.Objects.requireNonNull
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.{Response, UriInfo}

import scala.collection.mutable

import Slug.Context.{EXECUTING_QUERY, QUEUED_QUERY}
import com.google.common.hash.Hashing
import io.trino.client.QueryResults

import org.apache.kyuubi.operation.{FetchOrientation, OperationHandle, OperationState, OperationStatus}
import org.apache.kyuubi.operation.OperationState.{FINISHED, INITIALIZED, OperationState, PENDING}
import org.apache.kyuubi.server.trino.api.Query.KYUUBI_SESSION_ID
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.service.TFrontendService.OK_STATUS
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TBoolValue, TColumnDesc, TColumnValue, TGetResultSetMetadataResp, TPrimitiveTypeEntry, TProtocolVersion, TRow, TRowSet, TTableSchema, TTypeDesc, TTypeEntry, TTypeId}

case class Query(
    queryId: QueryId,
    context: TrinoContext,
    be: BackendService) {

  private val QUEUED_QUERY_PATH = "/v1/statement/queued/"
  private val EXECUTING_QUERY_PATH = "/v1/statement/executing"

  private val slug: Slug = Slug.createNewWithUUID(queryId.getQueryId)
  private val lastToken = new AtomicLong

  private val defaultMaxRows = 1000
  private val defaultFetchOrientation = FetchOrientation.withName("FETCH_NEXT")

  def getQueryResults(token: Long, uriInfo: UriInfo, maxWait: Long = 0): QueryResults = {
    val status =
      be.getOperationStatus(queryId.operationHandle, Some(maxWait))
    val nextUri = if (status.exception.isEmpty) {
      getNextUri(token + 1, uriInfo, toSlugContext(status.state))
    } else null
    val queryHtmlUri = uriInfo.getRequestUriBuilder
      .replacePath("ui/query.html").replaceQuery(queryId.getQueryId).build()

    status.state match {
      case FINISHED =>
        val metaData = be.getResultSetMetadata(queryId.operationHandle)
        val resultSet = be.fetchResults(
          queryId.operationHandle,
          defaultFetchOrientation,
          defaultMaxRows,
          false).getResults
        TrinoContext.createQueryResults(
          queryId.getQueryId,
          nextUri,
          queryHtmlUri,
          status,
          Option(metaData),
          Option(resultSet))
      case _ =>
        TrinoContext.createQueryResults(
          queryId.getQueryId,
          nextUri,
          queryHtmlUri,
          status)
    }
  }

  def getPrepareQueryResults(
      token: Long,
      uriInfo: UriInfo,
      maxWait: Long = 0): QueryResults = {
    val status = OperationStatus(OperationState.FINISHED, 0, 0, 0, 0, false)
    val nextUri = null
    val queryHtmlUri = uriInfo.getRequestUriBuilder
      .replacePath("ui/query.html").replaceQuery(queryId.getQueryId).build()

    val columns = new TGetResultSetMetadataResp()
    columns.setStatus(OK_STATUS)
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName("result")
    val desc = new TTypeDesc
    desc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.BOOLEAN_TYPE)))
    tColumnDesc.setTypeDesc(desc)
    tColumnDesc.setPosition(0)
    val schema = new TTableSchema()
    schema.addToColumns(tColumnDesc)
    columns.setSchema(schema)

    val rows = new java.util.ArrayList[TRow]
    val trow = new TRow()
    val value = new TBoolValue()
    value.setValue(true)
    trow.addToColVals(TColumnValue.boolVal(value))
    rows.add(trow)
    val rowSet = new TRowSet(0, rows)

    TrinoContext.createQueryResults(
      queryId.getQueryId,
      nextUri,
      queryHtmlUri,
      status,
      Option(columns),
      Option(rowSet),
      updateType = "PREPARE")
  }

  def getLastToken: Long = this.lastToken.get()

  def getSlug: Slug = this.slug

  def cancel: Unit = clear

  private def clear = {
    be.closeOperation(queryId.operationHandle)
    context.session.get(KYUUBI_SESSION_ID).foreach { id =>
      be.closeSession(SessionHandle.fromUUID(id))
    }
  }

  private def setToken(token: Long): Unit = {
    val lastToken = this.lastToken.get
    if (token != lastToken && token != lastToken + 1) {
      throw new WebApplicationException(Response.Status.GONE)
    }
    this.lastToken.compareAndSet(lastToken, token)
  }

  private def getNextUri(token: Long, uriInfo: UriInfo, slugContext: Slug.Context.Context): URI = {
    val path = slugContext match {
      case QUEUED_QUERY => QUEUED_QUERY_PATH
      case EXECUTING_QUERY => EXECUTING_QUERY_PATH
    }

    uriInfo.getBaseUriBuilder.replacePath(path)
      .path(queryId.getQueryId)
      .path(slug.makeSlug(slugContext, token))
      .path(String.valueOf(token))
      .replaceQuery("")
      .build()
  }

  private def toSlugContext(state: OperationState): Slug.Context.Context = {
    state match {
      case INITIALIZED | PENDING => Slug.Context.QUEUED_QUERY
      case _ => Slug.Context.EXECUTING_QUERY
    }
  }

}

object Query {

  val KYUUBI_SESSION_ID = "kyuubi.session.id"

  def apply(
      statement: String,
      context: TrinoContext,
      translator: KyuubiTrinoOperationTranslator,
      backendService: BackendService,
      queryTimeout: Long = 0): Query = {
    val sessionHandle = getOrCreateSession(context, backendService)
    val operationHandle = translator.transform(
      statement,
      sessionHandle,
      context.session,
      true,
      queryTimeout)
    val sessionWithId =
      context.session + (KYUUBI_SESSION_ID -> sessionHandle.identifier.toString)
    val updatedContext = context.copy(session = sessionWithId)
    Query(QueryId(operationHandle), updatedContext, backendService)
  }

  def apply(
      statementId: String,
      statement: String,
      context: TrinoContext,
      backendService: BackendService): Query = {
    val sessionHandle = getOrCreateSession(context, backendService)
    val sessionWithId =
      context.session + (KYUUBI_SESSION_ID -> sessionHandle.identifier.toString)
    Query(
      queryId = QueryId(new OperationHandle(UUID.randomUUID())),
      context.copy(preparedStatement = Map(statementId -> statement), session = sessionWithId),
      backendService)
  }

  def apply(id: String, context: TrinoContext, backendService: BackendService): Query = {
    Query(QueryId(id), context, backendService)
  }

  private def getOrCreateSession(
      context: TrinoContext,
      backendService: BackendService): SessionHandle = {
    context.session.get(KYUUBI_SESSION_ID).map(SessionHandle.fromUUID).getOrElse {
      // transform Trino information to session and engine as far as possible.
      val trinoInfo = new mutable.HashMap[String, String]()
      context.clientInfo.foreach { info =>
        trinoInfo.put("trino.client.info", info)
      }
      context.source.foreach { source =>
        trinoInfo.put("trino.request.source", source)
      }
      context.traceToken.foreach { traceToken =>
        trinoInfo.put("trino.trace.token", traceToken)
      }
      context.timeZone.foreach { timeZone =>
        trinoInfo.put("trino.time.zone", timeZone)
      }
      context.language.foreach { language =>
        trinoInfo.put("trino.language", language)
      }
      if (context.clientTags.nonEmpty) {
        trinoInfo.put("trino.client.info", context.clientTags.mkString(","))
      }

      val newSessionConfigs = context.session ++ trinoInfo
      backendService.openSession(
        TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
        context.user,
        "",
        context.remoteUserAddress.getOrElse(""),
        newSessionConfigs)
    }
  }

}

case class QueryId(operationHandle: OperationHandle) {
  def getQueryId: String = operationHandle.identifier.toString
}

object QueryId {
  def apply(id: String): QueryId = QueryId(OperationHandle(id))
}

object Slug {

  object Context extends Enumeration {
    type Context = Value
    val QUEUED_QUERY, EXECUTING_QUERY = Value
  }

  private val RANDOM = new SecureRandom

  def createNew: Slug = {
    val randomBytes = new Array[Byte](16)
    RANDOM.nextBytes(randomBytes)
    new Slug(randomBytes)
  }

  def createNewWithUUID(uuid: String): Slug = {
    val uuidBytes = UUID.fromString(uuid).toString.getBytes("UTF-8")
    new Slug(uuidBytes)
  }
}

case class Slug(slugKey: Array[Byte]) {
  val hmac = Hashing.hmacSha1(requireNonNull(slugKey, "slugKey is null"))

  def makeSlug(context: Slug.Context.Context, token: Long): String = {
    "y" + hmac.newHasher.putInt(context.id).putLong(token).hash.toString
  }

  def isValid(context: Slug.Context.Context, slug: String, token: Long): Boolean =
    makeSlug(context, token) == slug
}
