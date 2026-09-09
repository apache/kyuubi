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

package org.apache.kyuubi.ha.client

import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.util.control.NonFatal

case class DataAgentSessionRoute(engineSpace: String, engineRefId: String, user: String)

object DataAgentSessionRoute {
  private val ROOT_SUFFIX = "DATA_AGENT_sessions"

  private def root(serverSpace: String): String = s"${serverSpace}_$ROOT_SUFFIX"

  private def path(serverSpace: String, sessionId: String): String =
    DiscoveryPaths.makePath(root(serverSpace), sessionId)

  def register(
      discovery: DiscoveryClient,
      serverSpace: String,
      sessionId: String,
      route: DataAgentSessionRoute): Unit = {
    val routePath = path(serverSpace, sessionId)
    val entry = encode(route)
    children(discovery, routePath) match {
      case Nil =>
        val entryPath = DiscoveryPaths.makePath(routePath, entry)
        try discovery.create(entryPath, "PERSISTENT")
        catch {
          case NonFatal(_) if discovery.pathExists(entryPath) =>
        }
      case existing :: Nil if existing == entry =>
      case _ =>
        throw new IllegalStateException(s"A different route exists for session $sessionId")
    }
  }

  def find(
      discovery: DiscoveryClient,
      serverSpace: String,
      sessionId: String): Option[DataAgentSessionRoute] = {
    val routePath = path(serverSpace, sessionId)
    children(discovery, routePath) match {
      case Nil => None
      case entry :: Nil => Some(decode(entry))
      case _ => throw new IllegalStateException(s"Multiple routes found for session $sessionId")
    }
  }

  def unregister(
      discovery: DiscoveryClient,
      serverSpace: String,
      sessionId: String): Unit = {
    val routePath = path(serverSpace, sessionId)
    try {
      discovery.delete(routePath, deleteChildren = true)
    } catch {
      case NonFatal(_) if discovery.pathNonExists(routePath, isPrefix = true) =>
    }
  }

  private def encode(route: DataAgentSessionRoute): String = {
    Seq(route.engineSpace, route.engineRefId, route.user)
      .map(encodeValue)
      .mkString(".")
  }

  private def encodeValue(value: String): String =
    Base64.getUrlEncoder.withoutPadding()
      .encodeToString(value.getBytes(StandardCharsets.UTF_8))

  private def children(discovery: DiscoveryClient, routePath: String): List[String] = {
    try discovery.getChildren(routePath)
    catch {
      case NonFatal(_) if discovery.pathNonExists(routePath, isPrefix = true) => Nil
    }
  }

  private def decode(entry: String): DataAgentSessionRoute = {
    val values = entry.split("\\.", -1)
    require(values.length == 3, "Invalid Data Agent session route")
    def decodeValue(value: String): String =
      new String(Base64.getUrlDecoder.decode(value), StandardCharsets.UTF_8)
    DataAgentSessionRoute(decodeValue(values(0)), decodeValue(values(1)), decodeValue(values(2)))
  }
}
