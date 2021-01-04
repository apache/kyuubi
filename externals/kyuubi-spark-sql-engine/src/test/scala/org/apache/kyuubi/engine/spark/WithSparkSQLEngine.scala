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

package org.apache.kyuubi.engine.spark

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.rpc.thrift.{TCLIService, TCloseSessionReq, TOpenSessionReq, TSessionHandle}
import org.apache.spark.sql.SparkSession
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import org.apache.kyuubi.Utils
import org.apache.kyuubi.operation.JDBCTests
import org.apache.kyuubi.service.authentication.PlainSASLHelper

trait WithSparkSQLEngine extends JDBCTests {

  val warehousePath = Utils.createTempDir()
  val metastorePath = Utils.createTempDir()
  warehousePath.toFile.delete()
  metastorePath.toFile.delete()
  System.setProperty("javax.jdo.option.ConnectionURL",
    s"jdbc:derby:;databaseName=$metastorePath;create=true")
  System.setProperty("spark.sql.warehouse.dir", warehousePath.toString)
  System.setProperty("spark.sql.hive.metastore.sharedPrefixes", "org.apache.hive.jdbc")

  SparkSession.clearActiveSession()
  SparkSession.clearDefaultSession()
  protected val spark: SparkSession = SparkSQLEngine.createSpark()

  protected var engine: SparkSQLEngine = _

  protected var connectionUrl: String = _

  override def beforeAll(): Unit = {
    engine = SparkSQLEngine.startEngine(spark)
    connectionUrl = engine.connectionUrl
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (engine != null) {
      engine.stop()
    }
    spark.stop()
    SessionState.detachSession()
    Hive.closeCurrent()
  }

  protected def jdbcUrl: String = s"jdbc:hive2://$connectionUrl/;"

  protected def withThriftClient(f: TCLIService.Iface => Unit): Unit = {
    val hostAndPort = connectionUrl.split(":")
    val host = hostAndPort.head
    val port = hostAndPort(1).toInt
    val socket = new TSocket(host, port)
    val transport = PlainSASLHelper.getPlainTransport(Utils.currentUser, "anonymous", socket)

    val protocol = new TBinaryProtocol(transport)
    val client = new TCLIService.Client(protocol)
    transport.open()
    try {
      f(client)
    } finally {
      socket.close()
    }
  }

  protected def withSessionHandle(f: (TCLIService.Iface, TSessionHandle) => Unit): Unit = {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      val resp = client.OpenSession(req)
      val handle = resp.getSessionHandle

      try {
        f(client, handle)
      } finally {
        val tCloseSessionReq = new TCloseSessionReq(handle)
        try {
          client.CloseSession(tCloseSessionReq)
        } catch {
          case e: Exception => error(s"Failed to close $handle", e)
        }
      }
    }
  }
}
