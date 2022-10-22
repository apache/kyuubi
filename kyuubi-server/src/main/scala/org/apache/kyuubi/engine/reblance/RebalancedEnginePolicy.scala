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
package org.apache.kyuubi.engine.reblance

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.commons.lang.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_INIT_TIMEOUT, ENGINE_POOL_BALANCE_EXPAND_THRESHOLD, ENGINE_POOL_BALANCE_POLICY}
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryPaths}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.session.SessionHandle

object RebalancedEnginePolicy extends Logging {

  private val sessionNodeSuffix = "-session"

  def deleteCloseSessionHandleNode(
      conf: KyuubiConf,
      closedHandleSpace: String,
      sessionSpace: UserSpaceInfo): Unit = {
    if (StringUtils.isEmpty(closedHandleSpace)) {
      warn(s"$closedHandleSpace not exists")
    }
    if (!conf.get(ENGINE_POOL_BALANCE_POLICY).equalsIgnoreCase("Rebalanced")) {
      return
    }
    val timeout: Long = conf.get(ENGINE_INIT_TIMEOUT)
    withDiscoveryClient(conf) { client =>
      client.tryWithLock(sessionSpace.getLockZkPath, timeout) {
        info(s"delete handle path $closedHandleSpace")
        if (client.pathExists(closedHandleSpace)) {
          client.delete(closedHandleSpace)
        } else {
          warn(s"delete handle path $closedHandleSpace not exists")
        }
      }
    }
  }

  def getAdaptiveSpaceIndex(
      conf: KyuubiConf,
      userSpaceInfo: UserSpaceInfo,
      clientPoolName: String,
      handleId: String): Int = {
    val engineExpandThreshold: Int = conf.get(ENGINE_POOL_BALANCE_EXPAND_THRESHOLD) + 1
    val timeout: Long = conf.get(ENGINE_INIT_TIMEOUT)
    val userPath = userSpaceInfo.getUserZkPath
    val lockPath = userSpaceInfo.getLockZkPath

    def groupOpenedSessionsByUserSpace(client: DiscoveryClient): Map[String, ListBuffer[String]] = {
      val allSpaceSessions = mutable.Map[String, ListBuffer[String]]()
      val allSessionNodes: List[String] =
        client.getChildren(userPath).filter(_.endsWith(sessionNodeSuffix))
      allSessionNodes.foreach { sessionNode =>
        val userSpace = SessionNodeUtil.extractSpace(sessionNode)
        val openedSessions = client.getChildren(DiscoveryPaths.makePath(userPath, sessionNode))
        debug(s"$userSpace connected session are ${openedSessions.mkString(",")}")
        allSpaceSessions.getOrElseUpdate(userSpace, new ListBuffer[String]()).appendAll(
          openedSessions)
      }
      allSpaceSessions.toMap
    }

    withDiscoveryClient(conf) { client =>
      if (client.pathNonExists(lockPath)) client.create(lockPath, "PERSISTENT")
      client.tryWithLock(lockPath, timeout) {
        val sessionsGroup = groupOpenedSessionsByUserSpace(client)
        val chooseSpace = chooseAdaptiveSpace(
          clientPoolName,
          engineExpandThreshold,
          sessionsGroup)
        // record session node in zNode for for next space choose
        recordOpenSession(handleId, userPath, client, chooseSpace)
        chooseSpace.spaceIndex
      }
    }
  }

  private def recordOpenSession(
      handleId: String,
      userPath: String,
      client: DiscoveryClient,
      chooseSpace: AdaptiveSpace): Unit = {
    val sessionNode = SessionNodeUtil
      .combine2SessionNode(chooseSpace.adaptiveSpaceStr)
    val sessionNodePath = DiscoveryPaths.makePath(userPath, sessionNode)
    addOpenSessionHandleNode(
      client,
      sessionNodePath,
      handleId,
      chooseSpace.firstInit)
  }

  def chooseAdaptiveSpace(
      subdomainPrefix: String,
      engineExpandThreshold: Int,
      sessionsGroup: Map[String, ListBuffer[String]]): AdaptiveSpace = {
    val optionalGroup = sessionsGroup.filter(oneSpaceGroup =>
      oneSpaceGroup._2.size < engineExpandThreshold)
    val adaptiveSpace =
      if (optionalGroup.isEmpty) {
        info(s"All space connections are full, start expand a new space for new engine")
        val newSpaceIndex = expandSpace(sessionsGroup.keys.toList, subdomainPrefix)
        AdaptiveSpace(s"$subdomainPrefix-$newSpaceIndex", newSpaceIndex, firstInit = true)
      } else {
        val mostOpenSessionSpace = optionalGroup.maxBy(y => y._2.size)._1
        val spaceIndex = mostOpenSessionSpace.split(s"$subdomainPrefix-").apply(1).toInt
        AdaptiveSpace(mostOpenSessionSpace, spaceIndex, firstInit = false)
      }
    adaptiveSpace
  }

  private def expandSpace(allSpaces: List[String], subdomainPrefix: String): Int = {
    val spaceIndexSeparator = "-"
    if (allSpaces.isEmpty) {
      val initSpace = s"$subdomainPrefix-1"
      info(s"init space is $initSpace")
      1
    } else {
      val lastSpace = allSpaces.max
      info(s"max index space is $lastSpace")
      val spaceSplitArray = lastSpace.split(spaceIndexSeparator)
      val engine = spaceSplitArray.apply(0)
      val pool = spaceSplitArray.apply(1)
      val maxIndex = spaceSplitArray.apply(2).toInt
      val newIndex = maxIndex + 1
      val expandSpace = s"$engine-$pool-$newIndex"
      info(s"expand space is $expandSpace")
      newIndex
    }
  }

  private def addOpenSessionHandleNode(
      client: DiscoveryClient,
      sessionNodePath: String,
      handleId: String,
      firstInit: Boolean): Unit = {
    info(s"add handleId $handleId to engine space $sessionNodePath")

    def createPath(client: DiscoveryClient, createNode: String): Unit = {
      if (client.pathNonExists(createNode)) {
        info(s"create zk path $createNode")
        client.create(createNode, "PERSISTENT")
      }
    }

    def createSeedHandlePath(): Unit = {
      if (firstInit) {
        val seedHandleId = SessionHandle().identifier.toString
        val seedHandleNodePath = s"$sessionNodePath/$seedHandleId"
        info(s"create seed handle path $seedHandleNodePath")
        createPath(client, seedHandleNodePath)
      }
    }

    createSeedHandlePath()
    createPath(client, sessionNodePath)
    createPath(client, s"$sessionNodePath/$handleId")
  }

  case class AdaptiveSpace(adaptiveSpaceStr: String, spaceIndex: Int, firstInit: Boolean)

  case class UserSpaceInfo(commonParent: String, appUser: String) {
    def getUserZkPath: String = DiscoveryPaths.makePath(commonParent, appUser)

    def getLockZkPath: String = {
      val lockPath = DiscoveryPaths.makePath(commonParent, appUser, Array("lock"))
      lockPath
    }
  }

  object SessionNodeUtil {
    val spaceSessionSuffix = "-session"

    def extractSpace(recordNode: String): String = {
      recordNode.split(spaceSessionSuffix).apply(0)
    }

    def combine2SessionNode(engineNameSpace: String): String = {
      s"$engineNameSpace$spaceSessionSuffix"
    }
  }
}
