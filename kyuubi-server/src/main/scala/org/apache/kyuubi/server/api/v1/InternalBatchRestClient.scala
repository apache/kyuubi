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

import java.util.{Map => JMap}
import java.util.Base64
import scala.collection.JavaConverters._
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.kyuubi.client.RestClient
import org.apache.kyuubi.client.api.v1.dto.{CloseBatchResponse, OperationLog}
import org.apache.kyuubi.server.http.authentication.AuthSchemes
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

/**
 * The internal batch rest client for batch request redirection and it is shared for all the
 * requests redirected to the same kyuubi instance.
 *
 * @param kyuubiInstance the kyuubi instance host:port.
 * @param socketTimeout the socket timeout for http client.
 * @param connectTimeout the connect timeout for http client.
 */
class InternalBatchRestClient(kyuubiInstance: String, socketTimeout: Int, connectTimeout: Int) {
  require(
    InternalSecurityAccessor.get() != null,
    "Internal secure access across Kyuubi instances is not enabled")

  val baseUrl = s"http://$kyuubiInstance/api/v1/batches"
  private val restClient = initRestClient()
  private val internalSecurityAccessor = InternalSecurityAccessor.get()

  def getBatchLocalLog(user: String, batchId: String, from: Int, size: Int): OperationLog = {
    val params = Map("from" -> from, "size" -> size).asJava
    val path = s"$batchId/localLog"
    restClient.get(
      path,
      params.asInstanceOf[JMap[String, Object]],
      classOf[OperationLog],
      getAuthHeader(user))
  }

  def deleteBatch(user: String, batchId: String): CloseBatchResponse = {
    val path = s"$batchId"
    restClient.delete(path, Map.empty.asInstanceOf[JMap[String, Object]], getAuthHeader(user))
  }

  private def initRestClient(): RestClient = {
    val requestConfig = RequestConfig.custom()
      .setSocketTimeout(socketTimeout)
      .setConnectTimeout(connectTimeout)
      .build()
    val httpClient = HttpClientBuilder.create()
      .setDefaultRequestConfig(requestConfig)
      .build()
    new RestClient(baseUrl, httpClient)
  }

  private def getAuthHeader(user: String): String = {
    val encodedAuthorization = new String(
      Base64.getEncoder.encode(s"$user:${internalSecurityAccessor.issueToken()}".getBytes()),
      "UTF-8")
    s"${AuthSchemes.KYUUBI_INTERNAL.toString} $encodedAuthorization"
  }
}
