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
      case count: AtomicInteger =>
        count.accumulateAndGet(1, (l, r) => if (l > 0) l - r else l)
      case _ =>
    }
  }
}

class SessionLimiterWithAccessControlListImpl(
    userLimit: Int,
    ipAddressLimit: Int,
    userIpAddressLimit: Int,
    var unlimitedUsers: Set[String],
    var denyUsers: Set[String],
    var denyIps: Set[String])
  extends SessionLimiterImpl(userLimit, ipAddressLimit, userIpAddressLimit) {
  override def increment(userIpAddress: UserIpAddress): Unit = {
    val user = userIpAddress.user
    if (StringUtils.isNotBlank(user) && denyUsers.contains(user)) {
      val errorMsg =
        s"Connection denied because the user is in the deny user list. (user: $user)"
      throw KyuubiSQLException(errorMsg)
    }
    val ip = userIpAddress.ipAddress
    if (StringUtils.isNotBlank(ip) && denyIps.contains(ip)) {
      val errorMsg =
        s"Connection denied because the client ip is in the deny ip list. (ipAddress: $ip)"
      throw KyuubiSQLException(errorMsg)
    }

    if (!unlimitedUsers.contains(user)) {
      super.increment(userIpAddress)
    }
  }

  private[kyuubi] def setUnlimitedUsers(unlimitedUsers: Set[String]): Unit = {
    this.unlimitedUsers = unlimitedUsers
  }

  private[kyuubi] def setDenyUsers(denyUsers: Set[String]): Unit = {
    this.denyUsers = denyUsers
  }

  private[kyuubi] def setDenyIps(denyIps: Set[String]): Unit = {
    this.denyIps = denyIps
  }
}

object SessionLimiter {

  def apply(
      userLimit: Int,
      ipAddressLimit: Int,
      userIpAddressLimit: Int,
      unlimitedUsers: Set[String] = Set.empty,
      denyUsers: Set[String] = Set.empty,
      denyIps: Set[String] = Set.empty): SessionLimiter = {
    new SessionLimiterWithAccessControlListImpl(
      userLimit,
      ipAddressLimit,
      userIpAddressLimit,
      unlimitedUsers,
      denyUsers,
      denyIps)
  }

  def resetUnlimitedUsers(limiter: SessionLimiter, unlimitedUsers: Set[String]): Unit =
    limiter match {
      case l: SessionLimiterWithAccessControlListImpl => l.setUnlimitedUsers(unlimitedUsers)
      case _ =>
    }

  def getUnlimitedUsers(limiter: SessionLimiter): Set[String] = limiter match {
    case l: SessionLimiterWithAccessControlListImpl => l.unlimitedUsers
    case _ => Set.empty
  }

  def resetDenyUsers(limiter: SessionLimiter, denyUsers: Set[String]): Unit =
    limiter match {
      case l: SessionLimiterWithAccessControlListImpl => l.setDenyUsers(denyUsers)
      case _ =>
    }

  def getDenyUsers(limiter: SessionLimiter): Set[String] = limiter match {
    case l: SessionLimiterWithAccessControlListImpl => l.denyUsers
    case _ => Set.empty
  }

  def resetDenyIps(limiter: SessionLimiter, denyIps: Set[String]): Unit =
    limiter match {
      case l: SessionLimiterWithAccessControlListImpl => l.setDenyIps(denyIps)
      case _ =>
    }

  def getDenyIps(limiter: SessionLimiter): Set[String] = limiter match {
    case l: SessionLimiterWithAccessControlListImpl => l.denyIps
    case _ => Set.empty
  }
}
