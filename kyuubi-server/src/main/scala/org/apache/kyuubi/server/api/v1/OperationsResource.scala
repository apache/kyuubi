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

import javax.ws.rs.{GET, Path, PathParam, Produces, _}
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.hive.service.rpc.thrift.TTypeQualifierValue

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.events.KyuubiOperationEvent
import org.apache.kyuubi.operation.KyuubiOperation
import org.apache.kyuubi.operation.OperationHandle.parseOperationHandle
import org.apache.kyuubi.server.api.ApiRequestContext

@Tag(name = "Operation")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class OperationsResource extends ApiRequestContext {

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Get an operation event")
  @GET
  @Path("{operationHandle}/event")
  def getOperationEvent(
      @PathParam("operationHandle") operationHandleStr: String): KyuubiOperationEvent = {
    try {
      val opHandle = parseOperationHandle(operationHandleStr)
      val operation = backendService.sessionManager.operationManager.getOperation(opHandle)
      KyuubiOperationEvent(operation.asInstanceOf[KyuubiOperation])
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting an operation event")
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description =
      "apply an action for an operation")
  @PUT
  @Path("{operationHandle}")
  def applyOpAction(
      request: OpActionRequest,
      @PathParam("operationHandle") operationHandleStr: String): Response = {
    try {
      val operationHandle = parseOperationHandle(operationHandleStr)
      request.action.toLowerCase() match {
        case "cancel" => backendService.cancelOperation(operationHandle)
        case "close" => backendService.closeOperation(operationHandle)
        case _ => throw KyuubiSQLException(s"Invalid action ${request.action}")
      }
      Response.ok().build()
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error applying ${request.action} " +
          s"for operation handle $operationHandleStr")
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description =
      "get result set metadata")
  @GET
  @Path("{operationHandle}/resultsetmetadata")
  def getResultSetMetadata(
      @PathParam("operationHandle") operationHandleStr: String): ResultSetMetaData = {
    try {
      val operationHandle = parseOperationHandle(operationHandleStr)
      ResultSetMetaData(
        backendService.getResultSetMetadata(operationHandle).getColumns.asScala.map(c => {
          val tPrimitiveTypeEntry = c.getTypeDesc.getTypes.get(0).getPrimitiveEntry
          var precision = 0
          var scale = 0
          if (tPrimitiveTypeEntry.getTypeQualifiers != null) {
            val qualifiers = tPrimitiveTypeEntry.getTypeQualifiers.getQualifiers
            val defaultValue = TTypeQualifierValue.i32Value(0);
            precision = qualifiers.getOrDefault("precision", defaultValue).getI32Value
            scale = qualifiers.getOrDefault("scale", defaultValue).getI32Value
          }
          ColumnDesc(
            c.getColumnName,
            tPrimitiveTypeEntry.getType.toString,
            c.getPosition,
            precision,
            scale,
            c.getComment)
        }))
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(
          s"Error getting result set metadata for operation handle $operationHandleStr")
    }
  }
}
