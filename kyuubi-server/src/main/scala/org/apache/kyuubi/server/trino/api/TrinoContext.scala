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
import java.net.{URLDecoder, URLEncoder}
import javax.ws.rs.core.{HttpHeaders, Response}

import scala.collection.JavaConverters._

import io.trino.client.ProtocolHeaders.TRINO_HEADERS
import io.trino.client.QueryResults

/**
 * The description and functionality of trino request
 * and response's context
 *
 * @param user Specifies the session user, must be supplied with every query
 * @param timeZone  The timezone for query processing
 * @param clientCapabilities Exclusive for trino server
 * @param source This supplies the name of the software that submitted the query,
 *               e.g. `trino-jdbc` or `trino-cli` by default
 * @param catalog The catalog context for query processing, will be set response
 * @param schema The schema context for query processing
 * @param language The language to use when processing the query and formatting results,
 *               formatted as a Java Locale string, e.g., en-US for US English
 * @param traceToken Trace token for correlating requests across systems
 * @param clientInfo Extra information about the client
 * @param clientTags Client tags for selecting resource groups. Example: abc,xyz
 * @param preparedStatement `preparedStatement` are kv pairs, where the names
 *                          are names of previously prepared SQL statements,
 *                          and the values are keys that identify the
 *                          executable form of the named prepared statements
 */
case class TrinoContext(
    user: String,
    timeZone: Option[String] = None,
    clientCapabilities: Option[String] = None,
    source: Option[String] = None,
    catalog: Option[String] = None,
    schema: Option[String] = None,
    language: Option[String] = None,
    traceToken: Option[String] = None,
    clientInfo: Option[String] = None,
    clientTags: Set[String] = Set.empty,
    session: Map[String, String] = Map.empty,
    preparedStatement: Map[String, String] = Map.empty) {}

object TrinoContext {

  def apply(headers: HttpHeaders): TrinoContext = {
    apply(headers.getRequestHeaders.asScala.toMap.map {
      case (k, v) => (k, v.asScala.toList)
    })
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

  // TODO: Building response with TrinoContext and other information
  def buildTrinoResponse(qr: QueryResults, trinoContext: TrinoContext): Response = {
    val responseBuilder = Response.ok(qr)

    trinoContext.catalog.foreach(
      responseBuilder.header(TRINO_HEADERS.responseSetCatalog, _))
    trinoContext.schema.foreach(
      responseBuilder.header(TRINO_HEADERS.responseSetSchema, _))

    trinoContext.session.foreach {
      case (k, v) =>
        responseBuilder.header(TRINO_HEADERS.responseSetSession, s"${k}=${urlEncode(v)}")
    }
    trinoContext.preparedStatement.foreach {
      case (k, v) =>
        responseBuilder.header(TRINO_HEADERS.responseAddedPrepare, s"${k}=${urlEncode(v)}")
    }

    List("responseDeallocatedPrepare").foreach { v =>
      responseBuilder.header(TRINO_HEADERS.responseDeallocatedPrepare, urlEncode(v))
    }

    responseBuilder.header(TRINO_HEADERS.responseClearSession, s"responseClearSession")
    responseBuilder.header(TRINO_HEADERS.responseClearTransactionId, "false")
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

}
