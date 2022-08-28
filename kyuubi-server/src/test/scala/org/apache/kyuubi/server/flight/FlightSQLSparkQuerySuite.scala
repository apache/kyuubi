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

package org.apache.kyuubi.server.flight

import org.apache.arrow.flight.Location
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.impl.FlightSql
import org.apache.arrow.vector.util.Text

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.server.KyuubiFlightSQLFrontendService

class FlightSQLSparkQuerySuite extends WithKyuubiServer with FlightSQLTestHelper {

  override protected val conf: KyuubiConf = KyuubiConf()

  override protected val frontendProtocols: Seq[KyuubiConf.FrontendProtocols.Value] =
    FrontendProtocols.FLIGHT_SQL :: Nil

  override protected def serverLocation: Location = new Location(
    server.frontendServices.head.asInstanceOf[KyuubiFlightSQLFrontendService].connectionUrl)

  override protected def getJdbcUrl: String = "DUMMY"

  test("Arrow Flight SQL - SqlInfo") {
    withFlightSQLClient { client =>
      val flightInfo = client.getSqlInfo(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME)
      val stream = client.getStream(flightInfo.getEndpoints.get(0).getTicket)
      val schema = stream.getSchema
      assert(schema === Schemas.GET_SQL_INFO_SCHEMA)
      assert(stream.next)
      val root = stream.getRoot
      val fieldVectors = root.getFieldVectors
      assert(fieldVectors.size == 2)
      assert(fieldVectors.get(0).getObject(0) === 0)
      assert(fieldVectors.get(1).getObject(0) === new Text("Apache Kyuubi (Incubating)"))
      assert(!stream.next)
      stream.close()
    }
  }
}
