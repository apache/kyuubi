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

import javax.ws.rs._
import javax.ws.rs.core.{Context, HttpHeaders, MediaType}

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import io.trino.client.QueryResults

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.trino.api.ApiRequestContext
import org.apache.kyuubi.server.trino.api.v1.dto.Ok

@Tag(name = "Statement")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StatementResource extends ApiRequestContext with Logging {

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
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
  @GET
  @Path("/")
  @Consumes(Array(MediaType.TEXT_PLAIN))
  def query(statement: String, @Context headers: HttpHeaders): QueryResults = {
    throw new UnsupportedOperationException
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Get queued statement status")
  @GET
  @Path("/queued/{queryId}/{slug}/{token}")
  def getQueuedStatementStatus(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long): QueryResults = {
    throw new UnsupportedOperationException
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Get executing statement status")
  @GET
  @Path("/executing/{queryId}/{slug}/{token}")
  def getExecutingStatementStatus(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long): QueryResults = {
    throw new UnsupportedOperationException
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Cancel queued statement")
  @DELETE
  @Path("/queued/{queryId}/{slug}/{token}")
  def cancelQueuedStatement(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long): QueryResults = {
    throw new UnsupportedOperationException
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Cancel executing statement")
  @DELETE
  @Path("/executing/{queryId}/{slug}/{token}")
  def cancelExecutingStatementStatus(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long): QueryResults = {
    throw new UnsupportedOperationException
  }

}
