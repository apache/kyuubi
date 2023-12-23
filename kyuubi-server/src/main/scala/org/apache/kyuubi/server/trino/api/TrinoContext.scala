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

import java.io.UnsupportedEncodingException
import java.net.{URI, URLDecoder, URLEncoder}
import java.util
import java.util.Optional
import javax.ws.rs.core.{HttpHeaders, Response}

import scala.collection.JavaConverters._

import com.google.common.collect.ImmutableList
import io.trino.client.{ClientStandardTypes, ClientTypeSignature, ClientTypeSignatureParameter, Column, NamedClientTypeSignature, QueryError, QueryResults, RowFieldName, StatementStats, Warning}
import io.trino.client.ProtocolHeaders.TRINO_HEADERS

import org.apache.kyuubi.operation.OperationState.FINISHED
import org.apache.kyuubi.operation.OperationStatus
import org.apache.kyuubi.server.trino.api.Query.KYUUBI_SESSION_ID
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TCLIServiceConstants, TGetResultSetMetadataResp, TRowSet, TTypeEntry, TTypeId}

// TODO: Support replace `preparedStatement` for Trino-jdbc
/**
 * The description and functionality of trino request
 * and response's context
 *
 * @param user               Specifies the session user, must be supplied with every query
 * @param timeZone           The timezone for query processing
 * @param clientCapabilities Exclusive for trino server
 * @param source             This supplies the name of the software that submitted the query,
 *                           e.g. `trino-jdbc` or `trino-cli` by default
 * @param catalog            The catalog context for query processing, will be set response
 * @param schema             The schema context for query processing
 * @param language           The language to use when processing the query and formatting results,
 *                           formatted as a Java Locale string, e.g., en-US for US English
 * @param traceToken         Trace token for correlating requests across systems
 * @param clientInfo         Extra information about the client
 * @param clientTags         Client tags for selecting resource groups. Example: abc,xyz
 * @param preparedStatement  `preparedStatement` are kv pairs, where the names
 *                           are names of previously prepared SQL statements,
 *                           and the values are keys that identify the
 *                           executable form of the named prepared statements
 */
case class TrinoContext(
    user: String,
    timeZone: Option[String] = None,
    clientCapabilities: Option[String] = None,
    source: Option[String] = None,
    catalog: Option[String] = None,
    schema: Option[String] = None,
    remoteUserAddress: Option[String] = None,
    language: Option[String] = None,
    traceToken: Option[String] = None,
    clientInfo: Option[String] = None,
    clientTags: Set[String] = Set.empty,
    session: Map[String, String] = Map.empty,
    preparedStatement: Map[String, String] = Map.empty) {}

object TrinoContext {

  private val defaultWarning: util.List[Warning] = new util.ArrayList[Warning]()
  private val GENERIC_INTERNAL_ERROR_CODE = 65536
  private val GENERIC_INTERNAL_ERROR_NAME = "GENERIC_INTERNAL_ERROR_NAME"
  private val GENERIC_INTERNAL_ERROR_TYPE = "INTERNAL_ERROR"

  def apply(headers: HttpHeaders, remoteAddress: Option[String]): TrinoContext = {
    val context = apply(headers.getRequestHeaders.asScala.toMap.map {
      case (k, v) => (k, v.asScala.toList)
    })
    context.copy(remoteUserAddress = remoteAddress)
  }

  def apply(headers: Map[String, List[String]]): TrinoContext = {
    val requestCtx = TrinoContext("")
    val kvPattern = """(.+)=(.+)""".r
    headers.foldLeft(requestCtx) { case (context, (k, v)) =>
      k match {
        case k if TRINO_HEADERS.requestUser.equalsIgnoreCase(k) && v.nonEmpty =>
          context.copy(user = v.head)
        case k if TRINO_HEADERS.requestTimeZone.equalsIgnoreCase(k) =>
          context.copy(timeZone = v.headOption)
        case k if TRINO_HEADERS.requestClientCapabilities.equalsIgnoreCase(k) =>
          context.copy(clientCapabilities = v.headOption)
        case k if TRINO_HEADERS.requestSource.equalsIgnoreCase(k) =>
          context.copy(source = v.headOption)
        case k if TRINO_HEADERS.requestCatalog.equalsIgnoreCase(k) =>
          context.copy(catalog = v.headOption)
        case k if TRINO_HEADERS.requestSchema.equalsIgnoreCase(k) =>
          context.copy(schema = v.headOption)
        case k if TRINO_HEADERS.requestLanguage.equalsIgnoreCase(k) =>
          context.copy(language = v.headOption)
        case k if TRINO_HEADERS.requestTraceToken.equalsIgnoreCase(k) =>
          context.copy(traceToken = v.headOption)
        case k if TRINO_HEADERS.requestClientInfo.equalsIgnoreCase(k) =>
          context.copy(clientInfo = v.headOption)
        case k if TRINO_HEADERS.requestClientTags.equalsIgnoreCase(k) && v.nonEmpty =>
          context.copy(clientTags = v.head.split(",").toSet)
        case k if TRINO_HEADERS.requestSession.equalsIgnoreCase(k) =>
          val session = v.collect {
            case kvPattern(key, value) => (key, urlDecode(value))
          }.toMap
          context.copy(session = session)
        case k if TRINO_HEADERS.requestPreparedStatement.equalsIgnoreCase(k) =>
          val preparedStatement = v.collect {
            case kvPattern(key, value) => (key, urlDecode(value))
          }.toMap
          context.copy(preparedStatement = preparedStatement)

        case k
            if TRINO_HEADERS.requestTransactionId.equalsIgnoreCase(k)
              && v.headOption.exists(_ != "NONE") =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if TRINO_HEADERS.requestPath.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if TRINO_HEADERS.requestRole.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if TRINO_HEADERS.requestResourceEstimate.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if TRINO_HEADERS.requestExtraCredential.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if TRINO_HEADERS.requestRole.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case _ =>
          context
      }
    }
  }

  def buildTrinoResponse(qr: QueryResults, trinoContext: TrinoContext): Response = {
    val responseBuilder = Response.ok(qr)

    // Note, We have injected kyuubi session id to session context so that the next query can find
    // the previous session to restore the query context.
    // It's hard to follow the Trino style that set all context to http headers.
    // Because we do not know the context at server side. e.g. `set k=v`, `use database`.
    // We also can not inject other session context into header before we supporting to map
    // query result to session context.
    require(trinoContext.session.contains(KYUUBI_SESSION_ID), s"$KYUUBI_SESSION_ID must be set.")
    responseBuilder.header(
      TRINO_HEADERS.responseSetSession,
      s"$KYUUBI_SESSION_ID=${urlEncode(trinoContext.session(KYUUBI_SESSION_ID))}")

    trinoContext.preparedStatement.foreach {
      case (k, v) =>
        responseBuilder.header(TRINO_HEADERS.responseAddedPrepare, s"${k}=${urlEncode(v)}")
    }

    List("responseDeallocatedPrepare").foreach { v =>
      responseBuilder.header(TRINO_HEADERS.responseDeallocatedPrepare, urlEncode(v))
    }

    responseBuilder.build()
  }

  def urlEncode(value: String): String =
    try URLEncoder.encode(value, "UTF-8")
    catch {
      case e: UnsupportedEncodingException =>
        throw new AssertionError(e)
    }

  def urlDecode(value: String): String =
    try URLDecoder.decode(value, "UTF-8")
    catch {
      case e: UnsupportedEncodingException =>
        throw new AssertionError(e)
    }

  def createQueryResults(
      queryId: String,
      nextUri: URI,
      queryHtmlUri: URI,
      queryStatus: OperationStatus,
      columns: Option[TGetResultSetMetadataResp] = None,
      data: Option[TRowSet] = None,
      updateType: String = null): QueryResults = {

    val columnList = columns match {
      case Some(value) => convertTColumn(value)
      case None => null
    }
    val rowList = data match {
      case Some(value) =>
        Option(updateType) match {
          case Some("PREPARE") =>
            ImmutableList.of(ImmutableList.of(true).asInstanceOf[util.List[Object]])
          case _ => convertTRowSet(value)
        }
      case None => null
    }

    val updatedNextUri = queryStatus.state match {
      case FINISHED if rowList == null || rowList.isEmpty || rowList.get(0).isEmpty => null
      case _ => nextUri
    }

    new QueryResults(
      queryId,
      queryHtmlUri,
      nextUri,
      updatedNextUri,
      columnList,
      rowList,
      StatementStats.builder.setState(queryStatus.state.name()).setQueued(false)
        .setElapsedTimeMillis(0).setQueuedTimeMillis(0).build(),
      toQueryError(queryStatus),
      defaultWarning,
      updateType,
      0L)
  }

  private def convertTColumn(columns: TGetResultSetMetadataResp): util.List[Column] = {
    columns.getSchema.getColumns.asScala.map(c => {
      val (tp, arguments) = toClientTypeSignature(c.getTypeDesc.getTypes.get(0))
      new Column(c.getColumnName, tp, new ClientTypeSignature(tp, arguments))
    }).toList.asJava
  }

  private def toClientTypeSignature(
      entry: TTypeEntry): (String, util.List[ClientTypeSignatureParameter]) = {
    // according to `io.trino.jdbc.ColumnInfo`
    if (entry.isSetPrimitiveEntry) {
      entry.getPrimitiveEntry.getType match {
        case TTypeId.BOOLEAN_TYPE =>
          (ClientStandardTypes.BOOLEAN, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.TINYINT_TYPE =>
          (ClientStandardTypes.TINYINT, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.SMALLINT_TYPE =>
          (ClientStandardTypes.SMALLINT, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.INT_TYPE =>
          (ClientStandardTypes.INTEGER, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.BIGINT_TYPE =>
          (ClientStandardTypes.BIGINT, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.FLOAT_TYPE =>
          (ClientStandardTypes.DOUBLE, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.DOUBLE_TYPE =>
          (ClientStandardTypes.DOUBLE, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.DATE_TYPE =>
          (ClientStandardTypes.DATE, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.TIMESTAMP_TYPE =>
          (ClientStandardTypes.TIMESTAMP, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.BINARY_TYPE =>
          (ClientStandardTypes.VARBINARY, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.DECIMAL_TYPE =>
          val map = entry.getPrimitiveEntry.getTypeQualifiers.getQualifiers
          val precision = Option(map.get(TCLIServiceConstants.PRECISION)).map(_.getI32Value)
            .getOrElse(38)
          val scale = Option(map.get(TCLIServiceConstants.SCALE)).map(_.getI32Value)
            .getOrElse(18)
          (
            ClientStandardTypes.DECIMAL,
            ImmutableList.of(
              ClientTypeSignatureParameter.ofLong(precision),
              ClientTypeSignatureParameter.ofLong(scale)))
        case TTypeId.STRING_TYPE =>
          (
            ClientStandardTypes.VARCHAR,
            varcharSignatureParameter)
        case TTypeId.VARCHAR_TYPE =>
          (
            ClientStandardTypes.VARCHAR,
            varcharSignatureParameter)
        case TTypeId.CHAR_TYPE =>
          (ClientStandardTypes.CHAR, ImmutableList.of(ClientTypeSignatureParameter.ofLong(65536)))
        case TTypeId.INTERVAL_YEAR_MONTH_TYPE =>
          (
            ClientStandardTypes.INTERVAL_YEAR_TO_MONTH,
            ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.INTERVAL_DAY_TIME_TYPE =>
          (ClientStandardTypes.TIME_WITH_TIME_ZONE, ImmutableList.of[ClientTypeSignatureParameter])
        case TTypeId.TIMESTAMPLOCALTZ_TYPE =>
          (
            ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE,
            ImmutableList.of[ClientTypeSignatureParameter])
        case _ =>
          (
            ClientStandardTypes.VARCHAR,
            varcharSignatureParameter)
      }
    } else if (entry.isSetArrayEntry) {
      // thrift does not support nested types.
      // it's quite hard to follow the hive way, so always return varchar
      // TODO: make complex data type more accurate
      (
        ClientStandardTypes.ARRAY,
        ImmutableList.of(ClientTypeSignatureParameter.ofType(
          new ClientTypeSignature(ClientStandardTypes.VARCHAR, varcharSignatureParameter))))
    } else if (entry.isSetMapEntry) {
      (
        ClientStandardTypes.MAP,
        ImmutableList.of(
          ClientTypeSignatureParameter.ofType(
            new ClientTypeSignature(ClientStandardTypes.VARCHAR, varcharSignatureParameter)),
          ClientTypeSignatureParameter.ofType(
            new ClientTypeSignature(ClientStandardTypes.VARCHAR, varcharSignatureParameter))))
    } else if (entry.isSetStructEntry) {
      val parameters = entry.getStructEntry.getNameToTypePtr.asScala.map { case (k, v) =>
        ClientTypeSignatureParameter.ofNamedType(
          new NamedClientTypeSignature(
            Optional.of(new RowFieldName(k)),
            new ClientTypeSignature(ClientStandardTypes.VARCHAR, varcharSignatureParameter)))
      }
      (
        ClientStandardTypes.ROW,
        ImmutableList.copyOf(parameters.toArray))
    } else {
      throw new UnsupportedOperationException(s"Do not support type: $entry")
    }
  }

  private def varcharSignatureParameter: util.List[ClientTypeSignatureParameter] = {
    ImmutableList.of(ClientTypeSignatureParameter.ofLong(
      ClientTypeSignature.VARCHAR_UNBOUNDED_LENGTH))
  }

  def convertTRowSet(rowSet: TRowSet): util.List[util.List[Object]] = {
    val dataResult = new util.LinkedList[util.List[Object]]

    if (rowSet.getColumns == null) {
      return rowSet.getRows.asScala
        .map(t => t.getColVals.asScala.map(v => v.getFieldValue).asJava)
        .asJava
    }

    rowSet.getColumns.asScala.foreach {
      case tColumn if tColumn.isSetBoolVal =>
        val nulls = util.BitSet.valueOf(tColumn.getBoolVal.getNulls)
        if (dataResult.isEmpty) {
          (1 to tColumn.getBoolVal.getValuesSize).foreach(_ =>
            dataResult.add(new util.LinkedList[Object]()))
        }

        tColumn.getBoolVal.getValues.asScala.zipWithIndex.foreach {
          case (_, rowIdx) if nulls.get(rowIdx) =>
            dataResult.get(rowIdx).add(null)
          case (v, rowIdx) =>
            dataResult.get(rowIdx).add(v)
        }
      case tColumn if tColumn.isSetByteVal =>
        val nulls = util.BitSet.valueOf(tColumn.getByteVal.getNulls)
        if (dataResult.isEmpty) {
          (1 to tColumn.getByteVal.getValuesSize).foreach(_ =>
            dataResult.add(new util.LinkedList[Object]()))
        }

        tColumn.getByteVal.getValues.asScala.zipWithIndex.foreach {
          case (_, rowIdx) if nulls.get(rowIdx) =>
            dataResult.get(rowIdx).add(null)
          case (v, rowIdx) =>
            dataResult.get(rowIdx).add(v)
        }
      case tColumn if tColumn.isSetI16Val =>
        val nulls = util.BitSet.valueOf(tColumn.getI16Val.getNulls)
        if (dataResult.isEmpty) {
          (1 to tColumn.getI16Val.getValuesSize).foreach(_ =>
            dataResult.add(new util.LinkedList[Object]()))
        }

        tColumn.getI16Val.getValues.asScala.zipWithIndex.foreach {
          case (_, rowIdx) if nulls.get(rowIdx) =>
            dataResult.get(rowIdx).add(null)
          case (v, rowIdx) =>
            dataResult.get(rowIdx).add(v)
        }
      case tColumn if tColumn.isSetI32Val =>
        val nulls = util.BitSet.valueOf(tColumn.getI32Val.getNulls)
        if (dataResult.isEmpty) {
          (1 to tColumn.getI32Val.getValuesSize).foreach(_ =>
            dataResult.add(new util.LinkedList[Object]()))
        }

        tColumn.getI32Val.getValues.asScala.zipWithIndex.foreach {
          case (_, rowIdx) if nulls.get(rowIdx) =>
            dataResult.get(rowIdx).add(null)
          case (v, rowIdx) =>
            dataResult.get(rowIdx).add(v)
        }
      case tColumn if tColumn.isSetI64Val =>
        val nulls = util.BitSet.valueOf(tColumn.getI64Val.getNulls)
        if (dataResult.isEmpty) {
          (1 to tColumn.getI64Val.getValuesSize).foreach(_ =>
            dataResult.add(new util.LinkedList[Object]()))
        }

        tColumn.getI64Val.getValues.asScala.zipWithIndex.foreach {
          case (_, rowIdx) if nulls.get(rowIdx) =>
            dataResult.get(rowIdx).add(null)
          case (v, rowIdx) =>
            dataResult.get(rowIdx).add(v)
        }
      case tColumn if tColumn.isSetDoubleVal =>
        val nulls = util.BitSet.valueOf(tColumn.getDoubleVal.getNulls)
        if (dataResult.isEmpty) {
          (1 to tColumn.getDoubleVal.getValuesSize).foreach(_ =>
            dataResult.add(new util.LinkedList[Object]()))
        }

        tColumn.getDoubleVal.getValues.asScala.zipWithIndex.foreach {
          case (_, rowIdx) if nulls.get(rowIdx) =>
            dataResult.get(rowIdx).add(null)
          case (v, rowIdx) =>
            dataResult.get(rowIdx).add(v)
        }
      case tColumn if tColumn.isSetBinaryVal =>
        val nulls = util.BitSet.valueOf(tColumn.getBinaryVal.getNulls)
        if (dataResult.isEmpty) {
          (1 to tColumn.getBinaryVal.getValuesSize).foreach(_ =>
            dataResult.add(new util.LinkedList[Object]()))
        }

        tColumn.getBinaryVal.getValues.asScala.zipWithIndex.foreach {
          case (_, rowIdx) if nulls.get(rowIdx) =>
            dataResult.get(rowIdx).add(null)
          case (v, rowIdx) =>
            dataResult.get(rowIdx).add(v)
        }
      case tColumn =>
        val nulls = util.BitSet.valueOf(tColumn.getStringVal.getNulls)
        if (dataResult.isEmpty) {
          (1 to tColumn.getStringVal.getValuesSize).foreach(_ =>
            dataResult.add(new util.LinkedList[Object]()))
        }

        tColumn.getStringVal.getValues.asScala.zipWithIndex.foreach {
          case (_, rowIdx) if nulls.get(rowIdx) =>
            dataResult.get(rowIdx).add(null)
          case (v, rowIdx) =>
            dataResult.get(rowIdx).add(v)
        }
    }
    dataResult
  }

  def toQueryError(queryStatus: OperationStatus): QueryError = {
    val exception = queryStatus.exception
    if (exception.isEmpty) {
      null
    } else {
      new QueryError(
        exception.get.getMessage,
        queryStatus.state.name(),
        GENERIC_INTERNAL_ERROR_CODE,
        GENERIC_INTERNAL_ERROR_NAME,
        GENERIC_INTERNAL_ERROR_TYPE,
        null,
        null)
    }
  }

}
