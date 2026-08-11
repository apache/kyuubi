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

import org.apache.arrow.flight.{FlightClient, Location}
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.memory.RootAllocator
import org.scalatest.tags.Slow

import org.apache.kyuubi.WithFlightSqlServer
import org.apache.kyuubi.config.KyuubiConf

@Slow
class KyuubiFlightSqlQuerySuite extends WithFlightSqlServer {

  override protected val conf: KyuubiConf = KyuubiConf()

  test("execute a SQL statement and stream an Arrow batch") {
    val endpoint = flightSqlUrl
    val separator = endpoint.lastIndexOf(':')
    val host = endpoint.substring(0, separator)
    val port = endpoint.substring(separator + 1).toInt
    val allocator = new RootAllocator()
    val flightClient = FlightClient.builder(
      allocator,
      Location.forGrpcInsecure(host, port)).build()
    val sqlClient = new FlightSqlClient(flightClient)
    try {
      val info = sqlClient.execute("SELECT 1 AS value")
      assert(info.getEndpoints.size() === 1)
      val stream = sqlClient.getStream(info.getEndpoints.get(0).getTicket)
      try {
        assert(stream.next())
        assert(stream.getRoot.getRowCount > 0)
        assert(stream.getRoot.getVector(0).getObject(0).toString === "1")
      } finally {
        stream.close()
      }
    } finally {
      sqlClient.close()
      flightClient.close()
      allocator.close()
    }
  }
}
