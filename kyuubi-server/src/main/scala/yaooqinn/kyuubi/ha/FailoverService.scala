/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ha

import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.kyuubi.Logging

import yaooqinn.kyuubi.server.KyuubiServer

/**
 * A implementation of high availability service in active/standby mode a.k.a failover mode.
 * Kyuubi service instance URI is only exposed for clients in zookeeper when it gained the
 * leadership. In this mode, there are always only one Kyuubi server is reachable with zookeeper
 * namespace discovery mode.
 */
private[kyuubi] class FailoverService(name: String, server: KyuubiServer)
  extends HighAvailableService(name, server) with LeaderLatchListener with Logging {

  @volatile private var leaderLatch: LeaderLatch = _
  @volatile private var zkServiceStarted = false

  private def closeLeaderLatch(): Unit = Option(leaderLatch).foreach { latch =>
    try {
      latch.close()
      info("Close Zookeeper leader latch")
    } catch {
      case e: Exception => error("Error close leader latch", e)
    }
  }

  private def startLeaderLatch(): Unit = {
    info("Start Zookeeper leader latch")
    leaderLatch = new LeaderLatch(zkClient, serviceRootNamespace + "-latch")
    leaderLatch.addListener(this)
    leaderLatch.start()
  }

  def this(server: KyuubiServer) = {
    this(classOf[FailoverService].getSimpleName, server)
  }

  override def start(): Unit = {
    startLeaderLatch()
    super.start()
  }

  override def stop(): Unit = {
    offlineService()
    closeLeaderLatch()
    Option(zkClient).foreach(_.close())
    super.stop()
  }

  override def isLeader(): Unit = synchronized {
    if (leaderLatch.hasLeadership && !zkServiceStarted) {
      try {
        zkServiceStarted = true
        new Thread {
          override def run(): Unit = publishService()
        }.start()
        info("We have gained leadership")
      } catch {
        case e: Exception =>
          error("Failed to create service node from zookeeper, give up the leadership", e)
          reset()
      }
    }
  }

  override def notLeader(): Unit = synchronized {
    if (!leaderLatch.hasLeadership && zkServiceStarted) {
      zkServiceStarted = false
      offlineService()
      info("We have lost leadership")
    }
  }

  override private[ha] def reset(): Unit = {
    info("Reset Zookeeper leader latch")
    closeLeaderLatch()
    zkServiceStarted = false
    startLeaderLatch()
  }
}
