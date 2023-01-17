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

import Slug.Context.{EXECUTING_QUERY, QUEUED_QUERY}
import com.google.common.hash.Hashing
import io.trino.client.QueryResults

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.operation.OperationState.{INITIALIZED, OperationState, PENDING}

case class Query(
    statement: String,
    sessionContext: TrinoContext,
    translator: KyuubiTrinoOperationTranslator,
    queryTimeout: Long = 0) {

  private val slug: Slug = Slug.createNew
  private val lastToken = new AtomicLong
  private val operationHandle = translator.transform(
    statement,
    sessionContext.user,
    sessionContext.remoteUserAddress.getOrElse(""),
    sessionContext.session,
    true,
    queryTimeout)
  private val queryId = QueryId(operationHandle.identifier)

  def getQueryResults(token: Long, urlInfo: UriInfo): QueryResults = {
    val lastToken = this.lastToken.get
    if (token != lastToken && token != lastToken + 1) {
      throw new WebApplicationException(Response.Status.GONE)
    }
    this.lastToken.compareAndSet(lastToken, token)

    val status = translator.backendService.getOperationStatus(operationHandle)

    val nextUri = status.exception match {
      case _: KyuubiSQLException =>
        Option(getNextUri(token + 1, urlInfo, toSlugContext(status.state)))
      case _ => None
    }

    TrinoContext.createQueryResults(queryId, nextUri, urlInfo, status)
  }

  def getLastToken: Long = this.lastToken.get()

  def getSlug: Slug = this.slug

  def cancel: Unit = {
    translator.backendService.cancelOperation(operationHandle)
  }

  private def getNextUri(token: Long, uriInfo: UriInfo, slugContext: Slug.Context.Context): URI = {
    val path = slugContext match {
      case QUEUED_QUERY => "/v1/statement/queued/"
      case EXECUTING_QUERY => "/v1/statement/executing"
    }

    uriInfo.getBaseUriBuilder.replacePath(path)
      .path(queryId.toString)
      .path(slug.makeSlug(slugContext, token))
      .path(String.valueOf(token))
      .replaceQuery("")
      .build()
  }

  private def toSlugContext(state: OperationState): Slug.Context.Context = {
    state match {
      case INITIALIZED || PENDING => Slug.Context.QUEUED_QUERY
      case _ => Slug.Context.EXECUTING_QUERY
    }
  }

}

case class QueryId(identifier: UUID) {
  def getQueryId: String = identifier.toString
}

object QueryId {
  def apply(id: String): QueryId = QueryId(UUID.fromString(id))
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
}

case class Slug(slugKey: Array[Byte]) {
  val hmac = Hashing.hmacSha1(requireNonNull(slugKey, "slugKey is null"))

  def makeSlug(context: Slug.Context.Context, token: Long): String = {
    "y" + hmac.newHasher.putInt(
      requireNonNull(context, "context is null").ordinal).putLong(
      token).hash.toString
  }

  def isValid(context: Slug.Context.Context, slug: String, token: Long): Boolean =
    makeSlug(context, token) == slug
}
