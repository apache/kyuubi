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
package org.apache.kyuubi.session

import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.LongAdder

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}

class SessionLimiterSuite extends KyuubiFunSuite {

  test("test increment session limit") {
    val user = "user001"
    val ipAddress = "127.0.0.1"
    val userLimit = 30
    val ipAddressLimit = 20
    val userIpAddressLimit = 10
    val userCustomLimit = 5
    val userMap: Map[String, Int] = Map({ "user001" -> 5 })
    val threadPool = Executors.newFixedThreadPool(10)
    def checkLimit(
        userIpAddress: UserIpAddress,
        expectedIndex: Int,
        expectedErrorMsg: String): Unit = {
      val limiter = SessionLimiter(userMap, userLimit, ipAddressLimit, userIpAddressLimit)
      val successAdder = new LongAdder
      val expectedErrorAdder = new LongAdder
      val count = 50
      val latch = new CountDownLatch(count)
      for (i <- 0 until count) {
        threadPool.execute(() => {
          try {
            limiter.increment(userIpAddress)
            successAdder.increment()
          } catch {
            case e: KyuubiSQLException if e.getMessage === expectedErrorMsg =>
              expectedErrorAdder.increment()
            case _: Throwable =>
          } finally {
            latch.countDown()
          }
        })
      }
      latch.await()
      assert(successAdder.intValue() == expectedIndex)
      assert(expectedErrorAdder.intValue() == count - expectedIndex)
    }

    // user limit
    checkLimit(
      UserIpAddress(user, null),
      userLimit,
      s"Connection limit per user reached (user: $user limit: $userLimit)")

    // ipAddress limit
    checkLimit(
      UserIpAddress(null, ipAddress),
      ipAddressLimit,
      s"Connection limit per ipaddress reached (ipaddress: $ipAddress limit: $ipAddressLimit)")

    // userIpAddress limit
    checkLimit(
      UserIpAddress(user, ipAddress),
      userIpAddressLimit,
      s"Connection limit per user:ipaddress reached" +
        s" (user:ipaddress: $user:$ipAddress limit: $userIpAddressLimit)")

    // user custom limit
    checkLimit(
      UserIpAddress(user, null),
      userCustomLimit,
      s"Connection limit per user custom reached (user: $user limit: $userCustomLimit)")

    threadPool.shutdown()
  }

  test("test increment and decrement session") {
    val user = "user001"
    val ipAddress = "127.0.0.1"
    val userLimit = 30
    val ipAddressLimit = 20
    val userIpAddressLimit = 10
    val userMap: Map[String, Int] = Map({ "user001" -> 5 })
    val limiter = SessionLimiter(userMap, userLimit, ipAddressLimit, userIpAddressLimit)
    for (i <- 0 until 50) {
      val userIpAddress = UserIpAddress(user, ipAddress)
      limiter.increment(userIpAddress)
      limiter.decrement(userIpAddress)
    }
    limiter.asInstanceOf[SessionLimiterImpl].counters().asScala.values
      .foreach(c => assert(c.get() == 0))
  }
}
