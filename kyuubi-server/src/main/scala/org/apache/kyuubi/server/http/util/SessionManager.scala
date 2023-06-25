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

package org.apache.kyuubi.server.http.util

import org.apache.kyuubi.Logging

object SessionManager extends Logging {

  private val threadLocalIpAddress: ThreadLocal[String] = new ThreadLocal[String]

  def setIpAddress(ipAddress: String): Unit = {
    threadLocalIpAddress.set(ipAddress)
  }

  def clearIpAddress(): Unit = {
    threadLocalIpAddress.remove()
  }

  def getIpAddress: String = {
    threadLocalIpAddress.get
  }

  private val threadLocalProxyHttpHeaderIpAddress: ThreadLocal[String] = new ThreadLocal[String]

  def setProxyHttpHeaderIpAddress(realIpAddress: String): Unit = {
    threadLocalProxyHttpHeaderIpAddress.set(realIpAddress)
  }

  def clearProxyHttpHeaderIpAddress(): Unit = {
    threadLocalProxyHttpHeaderIpAddress.remove()
  }

  def getProxyHttpHeaderIpAddress: String = {
    threadLocalProxyHttpHeaderIpAddress.get
  }

  private val threadLocalForwardedAddresses: ThreadLocal[List[String]] =
    new ThreadLocal[List[String]]

  def setForwardedAddresses(ipAddress: List[String]): Unit = {
    threadLocalForwardedAddresses.set(ipAddress)
  }

  def clearForwardedAddresses(): Unit = {
    threadLocalForwardedAddresses.remove()
  }

  def getForwardedAddresses: List[String] = {
    threadLocalForwardedAddresses.get
  }

  private val threadLocalUserName: ThreadLocal[String] = new ThreadLocal[String]() {
    override protected def initialValue: String = {
      null
    }
  }

  def setUserName(userName: String): Unit = {
    threadLocalUserName.set(userName)
  }

  def clearUserName(): Unit = {
    threadLocalUserName.remove()
  }

  def getUserName: String = {
    threadLocalUserName.get
  }

  private val threadLocalProxyUserName: ThreadLocal[String] = new ThreadLocal[String]() {
    override protected def initialValue: String = {
      null
    }
  }

  def setProxyUserName(userName: String): Unit = {
    debug("setting proxy user name based on query param to: " + userName)
    threadLocalProxyUserName.set(userName)
  }

  def getProxyUserName: String = {
    threadLocalProxyUserName.get
  }

  def clearProxyUserName(): Unit = {
    threadLocalProxyUserName.remove()
  }
}
