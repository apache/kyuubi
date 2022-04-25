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

package org.apache.kyuubi.credentials

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.apache.hadoop.security.Credentials

import org.apache.kyuubi.credentials.CredentialsRef.UNSET_EPOCH
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

class CredentialsRef(appUser: String) {

  @volatile
  private var epoch = UNSET_EPOCH

  @volatile
  private var lastAccessTime: Long = System.currentTimeMillis()

  private var encodedCredentials: String = _

  private val credentialFuture = new AtomicReference[Future[Unit]]()

  def setFuture(future: Future[Unit]): Unit = {
    credentialFuture.set(future)
  }

  def updateLastAccessTime(): Unit = {
    lastAccessTime = System.currentTimeMillis()
  }

  def getLastAccessTime: Long = lastAccessTime

  def getNoOperationTime: Long = {
    System.currentTimeMillis() - lastAccessTime
  }

  def getEpoch: Long = epoch

  def getAppUser: String = appUser

  def getEncodedCredentials: String = {
    encodedCredentials
  }

  def updateCredentials(creds: Credentials): Unit = {
    encodedCredentials = KyuubiHadoopUtils.encodeCredentials(creds)
    epoch += 1
  }

  def waitUntilReady(timeout: Duration): Unit = {
    Option(credentialFuture.get).foreach(ThreadUtils.awaitResult(_, timeout))
  }

}

object CredentialsRef {
  val UNSET_EPOCH: Long = -1L
}
