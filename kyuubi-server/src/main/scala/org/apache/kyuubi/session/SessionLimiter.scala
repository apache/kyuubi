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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiSQLException

trait SessionLimiter {
  def increment(userIpAddress: UserIpAddress): Unit
  def decrement(userIpAddress: UserIpAddress): Unit
}

case class UserIpAddress(user: String, ipAddress: String)

class SessionLimiterImpl(userLimit: Int, ipAddressLimit: Int, userIpAddressLimit: Int)
  extends SessionLimiter {

  private val _counters: java.util.Map[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]()

  private[session] def counters(): java.util.Map[String, AtomicInteger] = _counters

  override def increment(userIpAddress: UserIpAddress): Unit = {
    val user = userIpAddress.user
    val ipAddress = userIpAddress.ipAddress
    // increment userIpAddress count
    if (userIpAddressLimit > 0 && StringUtils.isNotBlank(user) &&
      StringUtils.isNotBlank(ipAddress)) {
      incrLimitCount(
        s"$user:$ipAddress",
        userIpAddressLimit,
        "Connection limit per user:ipaddress reached" +
          s" (user:ipaddress: $user:$ipAddress limit: $userIpAddressLimit)")
    }
    // increment user count
    if (userLimit > 0 && StringUtils.isNotBlank(user)) {
      incrLimitCount(
        user,
        userLimit,
        s"Connection limit per user reached (user: $user limit: $userLimit)")
    }
    // increment ipAddress count
    if (ipAddressLimit > 0 && StringUtils.isNotBlank(ipAddress)) {
      incrLimitCount(
        ipAddress,
        ipAddressLimit,
        s"Connection limit per ipaddress reached (ipaddress: $ipAddress limit: $ipAddressLimit)")
    }
  }

  override def decrement(userIpAddress: UserIpAddress): Unit = {
    val user = userIpAddress.user
    val ipAddress = userIpAddress.ipAddress
    // decrement user count
    if (userLimit > 0 && StringUtils.isNotBlank(user)) {
      decrLimitCount(user)
    }
    // decrement ipAddress count
    if (ipAddressLimit > 0 && StringUtils.isNotBlank(ipAddress)) {
      decrLimitCount(ipAddress)
    }
    // decrement userIpAddress count
    if (userIpAddressLimit > 0 && StringUtils.isNotBlank(user) &&
      StringUtils.isNotBlank(ipAddress)) {
      decrLimitCount(s"$user:$ipAddress")
    }
  }

  private def incrLimitCount(key: String, limit: Int, errorMsg: String): Unit = {
    val count = _counters.computeIfAbsent(key, _ => new AtomicInteger())
    if (count.incrementAndGet() > limit) {
      count.decrementAndGet()
      throw KyuubiSQLException(errorMsg)
    }
  }

  private def decrLimitCount(key: String): Unit = {
    _counters.get(key) match {
      case count: AtomicInteger => count.decrementAndGet()
      case _ =>
    }
  }
}

class SessionLimiterWithUnlimitedUsersImpl(
    userLimit: Int,
    ipAddressLimit: Int,
    userIpAddressLimit: Int,
    unlimitedUsers: Set[String])
  extends SessionLimiterImpl(userLimit, ipAddressLimit, userIpAddressLimit) {
  override def increment(userIpAddress: UserIpAddress): Unit = {
    if (!unlimitedUsers.contains(userIpAddress.user)) {
      super.increment(userIpAddress)
    }
  }

  override def decrement(userIpAddress: UserIpAddress): Unit = {
    if (!unlimitedUsers.contains(userIpAddress.user)) {
      super.decrement(userIpAddress)
    }
  }
}

object SessionLimiter {

  def apply(
      userLimit: Int,
      ipAddressLimit: Int,
      userIpAddressLimit: Int,
      userWhiteList: Set[String] = Set.empty): SessionLimiter = {
    new SessionLimiterWithUnlimitedUsersImpl(
      userLimit,
      ipAddressLimit,
      userIpAddressLimit,
      userWhiteList)
  }

}
