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

import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.KyuubiSQLException
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
      "Get an operation detail with a given session identifier and operation identifier")
  @GET
  @Path("{operationHandle}")
  def getOperationDetail(
      @PathParam("operationHandle") operationHandleStr: String): OperationDetail = {
    try {
      val operation = backendService.sessionManager.operationManager
        .getOperation(parseOperationHandle(operationHandleStr))
      OperationDetail(operation.shouldRunAsync, operation.isTimedOut, operation.getStatus)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error getting an operation detail")
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
      val operationManager = backendService.sessionManager.operationManager

      request.action.toLowerCase() match {
        case "cancel" => operationManager.cancelOperation(operationHandle)
        case "close" => operationManager.closeOperation(operationHandle)
        case _ => throw KyuubiSQLException(s"Invalid request $request")
      }
      Response.ok().build()
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error applying ${request.action} for $operationHandleStr")
    }
  }
}
