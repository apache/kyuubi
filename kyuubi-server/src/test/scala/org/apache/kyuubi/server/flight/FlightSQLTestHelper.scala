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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.filght.ArrowUtils

trait FlightSQLTestHelper extends KyuubiFunSuite {

  protected def serverLocation: Location

  def withFlightSQLClient[T](f: FlightSqlClient => T): T = {
    val flightClient = FlightClient.builder()
      .location(serverLocation)
      .allocator(ArrowUtils.rootAllocator.newChildAllocator("test-flight-client", 0, Long.MaxValue))
      .build()
    val flightSQLClient = new FlightSqlClient(flightClient)
    try {
      f(flightSQLClient)
    } finally {
      flightSQLClient.close()
    }
  }
}
