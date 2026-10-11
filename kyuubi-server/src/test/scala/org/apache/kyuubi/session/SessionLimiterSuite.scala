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
import scala.util.Random

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.util.ThreadUtils

class SessionLimiterSuite extends KyuubiFunSuite {

  test("test increment session limit") {
    val user = "user001"
    val ipAddress = "127.0.0.1"
    val userLimit = 30
    val ipAddressLimit = 20
    val userIpAddressLimit = 10
    val threadPool = Executors.newFixedThreadPool(10)
    def checkLimit(
        userIpAddress: UserIpAddress,
        expectedIndex: Int,
        expectedErrorMsg: String): Unit = {
      val limiter = SessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit)
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
    threadPool.shutdown()
  }

  test("test increment and decrement session") {
    val user = "user001"
    val ipAddress = "127.0.0.1"
    val userLimit = 30
    val ipAddressLimit = 20
    val userIpAddressLimit = 10
    val limiter = SessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit)
    for (i <- 0 until 50) {
      val userIpAddress = UserIpAddress(user, ipAddress)
      limiter.increment(userIpAddress)
      limiter.decrement(userIpAddress)
    }
    limiter.asInstanceOf[SessionLimiterImpl].counters().asScala.values
      .foreach(c => assert(c.get() == 0))
  }

  test("test session limiter with user unlimited list") {
    val user = "user001"
    val ipAddress = "127.0.0.1"
    val userLimit = 30
    val ipAddressLimit = 20
    val userIpAddressLimit = 10
    val limiter = SessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit, Set(user))
    for (i <- 0 until 50) {
      val userIpAddress = UserIpAddress(user, ipAddress)
      limiter.increment(userIpAddress)
    }
    limiter.asInstanceOf[SessionLimiterImpl].counters().asScala.values
      .foreach(c => assert(c.get() == 0))
    for (i <- 0 until 50) {
      val userIpAddress = UserIpAddress(user, ipAddress)
      limiter.decrement(userIpAddress)
    }
    limiter.asInstanceOf[SessionLimiterImpl].counters().asScala.values
      .foreach(c => assert(c.get() == 0))
  }

  test("test session limiter with user deny list") {
    val ipAddress = "127.0.0.1"
    val userLimit = 100
    val ipAddressLimit = 100
    val userIpAddressLimit = 100
    val denyUsers = Set("user002", "user003")
    val limiter =
      SessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit, Set.empty, denyUsers)

    for (i <- 0 until 50) {
      val userIpAddress = UserIpAddress("user001", ipAddress)
      limiter.increment(userIpAddress)
    }
    limiter.asInstanceOf[SessionLimiterImpl].counters().asScala.values
      .foreach(c => assert(c.get() == 50))

    for (i <- 0 until 50) {
      val userIpAddress = UserIpAddress("user001", ipAddress)
      limiter.decrement(userIpAddress)
    }
    limiter.asInstanceOf[SessionLimiterImpl].counters().asScala.values
      .foreach(c => assert(c.get() == 0))

    val caught = intercept[KyuubiSQLException] {
      val userIpAddress = UserIpAddress("user002", ipAddress)
      limiter.increment(userIpAddress)
    }

    assert(caught.getMessage.equals(
      "Connection denied because the user is in the deny user list. (user: user002)"))
  }

  test("test session limiter with ip deny list") {
    val ipAddress = "127.0.0.1"
    val denyIps = Set(ipAddress)
    val limiter =
      SessionLimiter(100, 100, 100, Set.empty, Set.empty, denyIps)

    val caught = intercept[KyuubiSQLException] {
      val userIpAddress = UserIpAddress("user001", ipAddress)
      limiter.increment(userIpAddress)
    }

    assert(caught.getMessage.equals(
      "Connection denied because the client ip is in the deny ip list. (ipAddress: 127.0.0.1)"))
  }

  test("test refresh unlimited users and deny users") {
    val random: Random = new Random()
    val latch = new CountDownLatch(600)
    val userLimit = 100
    val ipAddressLimit = 101
    val userIpAddressLimit = 102
    val limiter =
      SessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit, Set.empty, Set.empty)
    val threadPool = ThreadUtils.newDaemonCachedThreadPool("test-refresh-config")

    def checkUserLimit(userIpAddress: UserIpAddress): Unit = {
      for (i <- 0 until 200) {
        threadPool.execute(() => {
          try {
            Thread.sleep(random.nextInt(200))
            limiter.increment(userIpAddress)
          } catch {
            case _: Throwable =>
          } finally {
            Thread.sleep(random.nextInt(500))
            // finally call limiter#decrement method.
            limiter.decrement(userIpAddress)
            latch.countDown()
          }
        })
      }
    }

    checkUserLimit(UserIpAddress("user001", "127.0.0.1"))
    checkUserLimit(UserIpAddress("user002", "127.0.0.2"))
    checkUserLimit(UserIpAddress("user003", "127.0.0.3"))

    Thread.sleep(100)
    // set unlimited users and deny users
    SessionLimiter.resetUnlimitedUsers(limiter, Set("user001"))
    SessionLimiter.resetDenyUsers(limiter, Set("user002"))

    Thread.sleep(300)
    // unset unlimited users and deny users
    SessionLimiter.resetUnlimitedUsers(limiter, Set.empty)
    SessionLimiter.resetDenyUsers(limiter, Set.empty)

    latch.await()
    threadPool.shutdown()
    limiter.asInstanceOf[SessionLimiterImpl].counters().asScala.values
      .foreach(c => assert(c.get() == 0))
  }

  test("test session limiter with ip allowlist") {
    val allowedIp = "10.0.0.1"
    val blockedIp = "192.168.1.100"
    val ipAllowlist = Set(allowedIp)
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      Set.empty,
      Set.empty,
      ipAllowlist)

    // allowed ip should be able to connect
    limiter.increment(UserIpAddress("user001", allowedIp))

    // blocked ip should be denied
    val caught = intercept[KyuubiSQLException] {
      limiter.increment(UserIpAddress("user001", blockedIp))
    }
    assert(caught.getMessage.equals(
      s"Connection denied because the client ip is not in the ip allowlist." +
        s" (ipAddress: $blockedIp)"))
  }

  test("test session limiter ip allowlist with multiple ips") {
    val allowedIp1 = "10.0.0.1"
    val allowedIp2 = "10.0.0.2"
    val blockedIp = "192.168.1.100"
    val ipAllowlist = Set(allowedIp1, allowedIp2)
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      Set.empty,
      Set.empty,
      ipAllowlist)

    // both allowed ips should be able to connect
    limiter.increment(UserIpAddress("user001", allowedIp1))
    limiter.increment(UserIpAddress("user002", allowedIp2))

    // blocked ip should be denied
    val caught = intercept[KyuubiSQLException] {
      limiter.increment(UserIpAddress("user003", blockedIp))
    }
    assert(caught.getMessage.contains("not in the ip allowlist"))
  }

  test("test session limiter empty ip allowlist allows all ips") {
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      Set.empty,
      Set.empty,
      Set.empty)

    // when allowlist is empty, all ips should be allowed
    limiter.increment(UserIpAddress("user001", "10.0.0.1"))
    limiter.increment(UserIpAddress("user002", "192.168.1.100"))
    limiter.increment(UserIpAddress("user003", "172.16.0.1"))
  }

  test("test session limiter ip deny list has higher priority than ip allowlist") {
    val ip = "10.0.0.1"
    val denyIps = Set(ip)
    val ipAllowlist = Set(ip)
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      Set.empty,
      denyIps,
      ipAllowlist)

    // deny ip list check happens before allowlist check
    val caught = intercept[KyuubiSQLException] {
      limiter.increment(UserIpAddress("user001", ip))
    }
    assert(caught.getMessage.equals(
      s"Connection denied because the client ip is in the deny ip list. (ipAddress: $ip)"))
  }

  test("test refresh ip allowlist") {
    val allowedIp = "10.0.0.1"
    val blockedIp = "192.168.1.100"
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      Set.empty,
      Set.empty,
      Set(allowedIp))

    // initially only allowedIp can connect
    limiter.increment(UserIpAddress("user001", allowedIp))
    intercept[KyuubiSQLException] {
      limiter.increment(UserIpAddress("user002", blockedIp))
    }

    // refresh allowlist to include blockedIp
    SessionLimiter.resetIpAllowlist(limiter, Set(allowedIp, blockedIp))
    limiter.increment(UserIpAddress("user002", blockedIp))

    // refresh allowlist to empty (allow all)
    SessionLimiter.resetIpAllowlist(limiter, Set.empty)
    limiter.increment(UserIpAddress("user003", "172.16.0.1"))
  }

  test("test session limiter with user allowlist") {
    val allowedUser = "user001"
    val blockedUser = "user002"
    val userAllowlist = Set(allowedUser)
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      Set.empty,
      Set.empty,
      Set.empty,
      userAllowlist)

    // allowed user should be able to connect
    limiter.increment(UserIpAddress(allowedUser, "10.0.0.1"))

    // blocked user should be denied
    val caught = intercept[KyuubiSQLException] {
      limiter.increment(UserIpAddress(blockedUser, "10.0.0.1"))
    }
    assert(caught.getMessage.equals(
      s"Connection denied because the user is not in the user allowlist." +
        s" (user: $blockedUser)"))
  }

  test("test session limiter user allowlist with multiple users") {
    val allowedUser1 = "user001"
    val allowedUser2 = "user002"
    val blockedUser = "user003"
    val userAllowlist = Set(allowedUser1, allowedUser2)
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      Set.empty,
      Set.empty,
      Set.empty,
      userAllowlist)

    // both allowed users should be able to connect
    limiter.increment(UserIpAddress(allowedUser1, "10.0.0.1"))
    limiter.increment(UserIpAddress(allowedUser2, "10.0.0.2"))

    // blocked user should be denied
    val caught = intercept[KyuubiSQLException] {
      limiter.increment(UserIpAddress(blockedUser, "192.168.1.100"))
    }
    assert(caught.getMessage.contains("not in the user allowlist"))
  }

  test("test session limiter empty user allowlist allows all users") {
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      Set.empty,
      Set.empty,
      Set.empty,
      Set.empty)

    // when allowlist is empty, all users should be allowed
    limiter.increment(UserIpAddress("user001", "10.0.0.1"))
    limiter.increment(UserIpAddress("user002", "192.168.1.100"))
    limiter.increment(UserIpAddress("user003", "172.16.0.1"))
  }

  test("test session limiter user deny list has higher priority than user allowlist") {
    val user = "user001"
    val denyUsers = Set(user)
    val userAllowlist = Set(user)
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      denyUsers,
      Set.empty,
      Set.empty,
      userAllowlist)

    // deny user list check happens before allowlist check
    val caught = intercept[KyuubiSQLException] {
      limiter.increment(UserIpAddress(user, "10.0.0.1"))
    }
    assert(caught.getMessage.equals(
      s"Connection denied because the user is in the deny user list. (user: $user)"))
  }

  test("test refresh user allowlist") {
    val allowedUser = "user001"
    val blockedUser = "user002"
    val limiter = SessionLimiter(
      100,
      100,
      100,
      Set.empty,
      Set.empty,
      Set.empty,
      Set.empty,
      Set(allowedUser))

    // initially only allowedUser can connect
    limiter.increment(UserIpAddress(allowedUser, "10.0.0.1"))
    intercept[KyuubiSQLException] {
      limiter.increment(UserIpAddress(blockedUser, "10.0.0.1"))
    }

    // refresh allowlist to include blockedUser
    SessionLimiter.resetUserAllowlist(limiter, Set(allowedUser, blockedUser))
    limiter.increment(UserIpAddress(blockedUser, "10.0.0.1"))

    // refresh allowlist to empty (allow all)
    SessionLimiter.resetUserAllowlist(limiter, Set.empty)
    limiter.increment(UserIpAddress("user003", "172.16.0.1"))
  }
}
