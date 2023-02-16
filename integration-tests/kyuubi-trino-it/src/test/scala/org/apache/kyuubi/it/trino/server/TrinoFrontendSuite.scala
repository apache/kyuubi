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

package org.apache.kyuubi.it.trino.server

import scala.util.control.NonFatal

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.SparkMetadataTests

/**
 * This test is for Trino jdbc driver with Kyuubi Server and Spark engine:
 *
 *  -------------------------------------------------------------
 *  |                JDBC                                       |
 *  |  Trino-driver  ---->  Kyuubi Server  -->  Spark Engine    |
 *  |                                                           |
 *  -------------------------------------------------------------
 */
class TrinoFrontendSuite extends WithKyuubiServer with SparkMetadataTests {
  // TODO: Add more test cases

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.FRONTEND_PROTOCOLS, Seq("TRINO"))
  }
  override protected def jdbcUrl: String = {
    s"jdbc:trino://${server.frontendServices.head.connectionUrl}/;"
  }

  // trino jdbc driver requires enable SSL if specify password
  override protected val password: String = ""

  override def beforeAll(): Unit = {
    super.beforeAll()

    // eagerly start spark engine before running test, it's a workaround for trino jdbc driver
    // since it does not support changing http connect timeout
    try {
      withJdbcStatement() { statement =>
        statement.execute("SELECT 1")
      }
    } catch {
      case NonFatal(e) =>
    }
  }
}
