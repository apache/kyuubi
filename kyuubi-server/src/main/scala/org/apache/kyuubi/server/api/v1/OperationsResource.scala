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
import javax.ws.rs.core.MediaType

import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

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
  def getOperationHandle(
      @PathParam("operationHandle") operationHandleStr: String): OperationDetail = {
    try {
      val operationManager = backendService.sessionManager.operationManager
      val operationHandle = operationManager.parseOperationHandle(operationHandleStr)
      val operation = operationManager.getOperation(operationHandle)
      OperationDetail(operation.shouldRunAsync, operation.isTimedOut, operation.getStatus)
    } catch {
      case NonFatal(_) =>
        throw new NotFoundException(s"Error closing an operation")
    }
  }
}
