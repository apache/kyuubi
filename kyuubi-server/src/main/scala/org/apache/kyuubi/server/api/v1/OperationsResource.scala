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

package org.apache.kyuubi.server.api.v1

import javax.ws.rs.{BadRequestException, _}
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.jdbc.hive.cli.{ColumnBasedSet, RowBasedSet, RowSet}
import org.apache.kyuubi.operation.{FetchOrientation, KyuubiOperation, OperationHandle}
import org.apache.kyuubi.server.api.{ApiRequestContext, ApiUtils}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

@Tag(name = "Operation")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
private[v1] class OperationsResource extends ApiRequestContext with Logging {
  import ApiUtils.logAndRefineErrorMsg

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[KyuubiOperationEvent]))),
    description =
      "Get an operation event")
  @GET
  @Path("{operationHandle}/event")
  def getOperationEvent(
      @PathParam("operationHandle") operationHandleStr: String): KyuubiOperationEvent = {
    try {
      val opHandle = OperationHandle(operationHandleStr)
      val operation = fe.be.sessionManager.operationManager.getOperation(opHandle)
      ApiUtils.operationEvent(operation.asInstanceOf[KyuubiOperation])
    } catch {
      case NonFatal(e) =>
        val errorMsg = "Error getting an operation event"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "apply an action for an operation")
  @PUT
  @Path("{operationHandle}")
  def applyOpAction(
      request: OpActionRequest,
      @PathParam("operationHandle") operationHandleStr: String): Response = {
    try {
      val operationHandle = OperationHandle(operationHandleStr)
      request.getAction.toLowerCase() match {
        case "cancel" => fe.be.cancelOperation(operationHandle)
        case "close" => fe.be.closeOperation(operationHandle)
        case _ => throw KyuubiSQLException(s"Invalid action ${request.getAction}")
      }
      Response.ok().build()
    } catch {
      case NonFatal(e) =>
        val errorMsg =
          s"Error applying ${request.getAction} for operation handle $operationHandleStr"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[ResultSetMetaData]))),
    description =
      "get result set metadata")
  @GET
  @Path("{operationHandle}/resultsetmetadata")
  def getResultSetMetadata(
      @PathParam("operationHandle") operationHandleStr: String): ResultSetMetaData = {
    try {
      val operationHandle = OperationHandle(operationHandleStr)
      new ResultSetMetaData(
        fe.be.getResultSetMetadata(operationHandle).getSchema.getColumns.asScala.map(c => {
          val tPrimitiveTypeEntry = c.getTypeDesc.getTypes.get(0).getPrimitiveEntry
          var precision = 0
          var scale = 0
          if (tPrimitiveTypeEntry.getTypeQualifiers != null) {
            val qualifiers = tPrimitiveTypeEntry.getTypeQualifiers.getQualifiers
            val defaultValue = TTypeQualifierValue.i32Value(0)
            precision = qualifiers.getOrDefault("precision", defaultValue).getI32Value
            scale = qualifiers.getOrDefault("scale", defaultValue).getI32Value
          }
          new ColumnDesc(
            c.getColumnName,
            tPrimitiveTypeEntry.getType.toString,
            c.getPosition,
            precision,
            scale,
            c.getComment)
        }).asJava)
    } catch {
      case NonFatal(e) =>
        val errorMsg = s"Error getting result set metadata for operation handle $operationHandleStr"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationLog]))),
    description =
      "get operation log")
  @GET
  @Path("{operationHandle}/log")
  def getOperationLog(
      @PathParam("operationHandle") operationHandleStr: String,
      @QueryParam("maxrows") @DefaultValue("100") maxRows: Int,
      @QueryParam("fetchorientation") @DefaultValue("FETCH_NEXT")
      fetchOrientation: String): OperationLog = {
    try {
      if (fetchOrientation != "FETCH_NEXT" && fetchOrientation != "FETCH_FIRST") {
        throw new BadRequestException(s"$fetchOrientation in operation log is not supported")
      }
      val fetchResultsResp = fe.be.sessionManager.operationManager.getOperationLogRowSet(
        OperationHandle(operationHandleStr),
        FetchOrientation.withName(fetchOrientation),
        maxRows)
      val rowSet = fetchResultsResp.getResults
      val logRowSet = if (rowSet.getColumns != null) {
        rowSet.getColumns.get(0).getStringVal.getValues.asScala
      } else {
        Seq.empty
      }
      new OperationLog(logRowSet.asJava, logRowSet.size)
    } catch {
      case e: BadRequestException =>
        throw e
      case NonFatal(e) =>
        val errorMsg = s"Error getting operation log for operation handle $operationHandleStr"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[ResultRowSet]))),
    description =
      "get result row set")
  @GET
  @Path("{operationHandle}/rowset")
  def getNextRowSet(
      @PathParam("operationHandle") operationHandleStr: String,
      @QueryParam("maxrows") @DefaultValue("100") maxRows: Int,
      @QueryParam("fetchorientation") @DefaultValue("FETCH_NEXT")
      fetchOrientation: String): ResultRowSet = {
    try {
      val fetchResultsResp = fe.be.fetchResults(
        OperationHandle(operationHandleStr),
        FetchOrientation.withName(fetchOrientation),
        maxRows,
        fetchLog = false)
      val results = fetchResultsResp.getResults
      if ((results.getRows != null && results.getRows.size() == 0) ||
        (results.getColumns != null && results.getColumns.size() == 0)) {
        return new ResultRowSet(List.empty.asJava, 0)
      }

      val (rowSet: RowSet, columnTypes: Array[String]) = if (results.getRows != null) {
        (
          new RowBasedSet(results),
          results.getRows.get(0).getColVals.asScala.map(c => c.getSetField.name()).toArray)
      } else {
        (
          new ColumnBasedSet(results),
          results.getColumns.asScala.map(c => { c.getSetField.name() }).toArray)
      }
      val columnSize = rowSet.numColumns
      val rows = rowSet.iterator().asScala.toSeq.map { r =>
        new Row(
          (0 until columnSize).map(i => {
            val columnValue = r(i)
            new Field(columnTypes(i), columnValue)
          }).asJava)
      }
      new ResultRowSet(rows.asJava, rows.size)
    } catch {
      case e: IllegalArgumentException =>
        error(e.getMessage, e)
        throw new BadRequestException(e.getMessage)
      case NonFatal(e) =>
        val errorMsg = s"Error getting result row set for operation handle $operationHandleStr"
        throw new NotFoundException(logAndRefineErrorMsg(errorMsg, e))
    }
  }
}
