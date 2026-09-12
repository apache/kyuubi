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

package org.apache.kyuubi.plugin.spark.authz.ranger

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.Executors

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.hadoop.conf.Configuration
import org.apache.ranger.authz.api.RangerAuthorizer
import org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer
import org.apache.ranger.authz.model._

/**
 * A test double of the Ranger PDP server, which authorizes requests with
 * [[RangerEmbeddedAuthorizer]] backed by the local policy file, serving the same
 * REST APIs as org.apache.ranger:pdp does.
 */
class MockPdpServer extends AutoCloseable {

  private val mapper = new JsonMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  private val properties: Properties = {
    // the embedded authorizer reads configurations from the given properties only,
    // so feed the same ones the client-side plugin uses
    val conf = new Configuration
    conf.addResource(classOf[MockPdpServer].getClassLoader.getResource("ranger-spark-security.xml"))
    val ret = new Properties
    conf.iterator.asScala
      .filter(entry => entry.getKey.startsWith("ranger.") || entry.getKey.startsWith("xasecure."))
      .foreach(entry => ret.put(entry.getKey, entry.getValue))
    ret.put("ranger.authz.init.services", "hive_jenkins")
    ret.put("ranger.authz.service.hive_jenkins.servicetype", "spark")
    ret.put("ranger.authz.app.type", "ranger-authz-pdp")
    ret
  }

  private val authorizer: RangerAuthorizer = new RangerEmbeddedAuthorizer(properties)

  private val authorizeHandler: HttpHandler = new HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val request = mapper.readValue(readRequestBody(exchange), classOf[RangerAuthzRequest])
      val result = authorizer.authorize(request)
      respond(exchange, mapper.writeValueAsString(result))
    }
  }

  private val authorizeMultiHandler: HttpHandler = new HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val request = mapper.readValue(readRequestBody(exchange), classOf[RangerMultiAuthzRequest])
      val result = authorizer.authorize(request)
      respond(exchange, mapper.writeValueAsString(result))
    }
  }

  private val executor = Executors.newFixedThreadPool(4)

  private val server: HttpServer = {
    authorizer.init()
    val ret = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    ret.createContext("/authz/v1/authorize", authorizeHandler)
    ret.createContext("/authz/v1/authorizeMulti", authorizeMultiHandler)
    ret.setExecutor(executor)
    ret.start()
    ret
  }

  val url: String = s"http://localhost:${server.getAddress.getPort}"

  private def readRequestBody(exchange: HttpExchange): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream
    val in = exchange.getRequestBody
    val buf = new Array[Byte](8192)
    var len = in.read(buf)
    while (len != -1) {
      out.write(buf, 0, len)
      len = in.read(buf)
    }
    out.toByteArray
  }

  private def respond(exchange: HttpExchange, body: String): Unit = {
    val bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(200, bytes.length)
    val out = exchange.getResponseBody
    try out.write(bytes)
    finally {
      out.close()
      exchange.close()
    }
  }

  override def close(): Unit = {
    server.stop(0)
    executor.shutdownNow()
    authorizer.close()
  }
}
