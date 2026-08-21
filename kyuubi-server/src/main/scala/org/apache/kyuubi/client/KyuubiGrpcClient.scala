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

package org.apache.kyuubi.client

import java.util.UUID

import io.grpc._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.shaded.spark.connect.proto

class KyuubiGrpcClient(
    val configuration: KyuubiGrpcClient.Configuration,
    val channel: ManagedChannel) extends KyuubiGrpcResponseValidator with Logging {

  def hostPort: (String, Int) = (configuration.host, configuration.port)

  private val userContext: proto.UserContext = configuration.userContext

  private[kyuubi] val astub = proto.SparkConnectServiceGrpc.newStub(channel)
  private[kyuubi] val bstub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)

  /**
   * Placeholder method.
   * @return
   *   User ID.
   */
  private[kyuubi] def userId: String = userContext.getUserId

  // Generate a unique session ID for this client. This UUID must be unique to allow
  // concurrent Spark sessions of the same user. If the channel is closed, creating
  // a new client will create a new session ID.
  private[kyuubi] val sessionId: String =
    configuration.sessionId.getOrElse(UUID.randomUUID.toString)
}

object KyuubiGrpcClient {

  case class Configuration(
      userId: String = null,
      userName: String = null,
      host: String = "localhost",
      port: Int = 10999,
      token: Option[String] = None,
      isSslEnabled: Option[Boolean] = None,
      metadata: Map[String, String] = Map.empty,
      userAgent: String = "kyuubi-grpc-client",
      // retryPolicies: Seq[RetryPolicy] = RetryPolicy.defaultPolicies(),
      useReattachableExecute: Boolean = true,
      interceptors: List[ClientInterceptor] = List.empty,
      sessionId: Option[String] = None,
      grpcMaxMessageSize: Int = 128 * 1024 * 1024,
      grpcMaxRecursionLimit: Int = 1024) {

    def userContext: proto.UserContext = {
      val builder = proto.UserContext.newBuilder()
      if (userId != null) {
        builder.setUserId(userId)
      }
      if (userName != null) {
        builder.setUserName(userName)
      }
      builder.build()
    }

    def credentials: ChannelCredentials = {
      InsecureChannelCredentials.create()
      // if (isSslEnabled.contains(true)) {
      //   token match {
      //     case Some(t) =>
      //       // With access token added in the http header.
      //       CompositeChannelCredentials.create(
      //         TlsChannelCredentials.create,
      //         new AccessTokenCallCredentials(t))
      //     case None =>
      //       TlsChannelCredentials.create()
      //   }
      // } else {
      //   InsecureChannelCredentials.create()
      // }
    }

    def createChannel(): ManagedChannel = {
      val channelBuilder = Grpc.newChannelBuilderForAddress(host, port, credentials)

      if (metadata.nonEmpty) {
        // channelBuilder.intercept(new MetadataHeaderClientInterceptor(metadata))
      }

      interceptors.foreach(channelBuilder.intercept(_))

      channelBuilder.maxInboundMessageSize(grpcMaxMessageSize)
      channelBuilder.build()
    }
  }

  def createClient(
      user: String,
      password: String,
      host: String,
      port: Int,
      conf: Map[String, String]): KyuubiGrpcClient = {
    val grpcConf = Configuration(
      userId = user,
      userName = user,
      host = host,
      port = port,
      sessionId = conf.get(KYUUBI_SESSION_HANDLE_KEY))
    new KyuubiGrpcClient(grpcConf, grpcConf.createChannel())
  }
}
