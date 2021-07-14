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

package org.apache.kyuubi.ha.v2.engine

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths

import org.apache.kyuubi.Logging
import org.apache.kyuubi.ha.v2.{InstanceProvider, InstanceProviderBuilder, ProviderFilter}
import org.apache.kyuubi.ha.v2.engine.filters.{TagProviderFilter, VersionProviderFilter}

class EngineProvider(namespace: String, zkClient: CuratorFramework)
  extends InstanceProvider[EngineInstance] with Logging {

  val filters: ArrayBuffer[ProviderFilter[EngineInstance]] = ArrayBuffer()

  def addFilter(filter: ProviderFilter[EngineInstance]): Unit = {
    filters += filter
  }

  override def getInstances(): Seq[EngineInstance] = {
    val enginePaths = zkClient.getChildren().forPath(namespace)
    enginePaths.asScala
      .map(getEngineInstance(_))
      .filter(e => {
        filters.find(!_.filter(e)).isEmpty
      })
  }

  private def getEngineInstance(p: String): EngineInstance = {
    val enginePath = ZKPaths.makePath(namespace, p)
    val nodeData = zkClient.getData.forPath(enginePath)
    val instance = EngineInstanceSerializer.deserialize(nodeData)
    // TODO get session number
    val sessionNum = instance.sessionNum
    EngineInstance(
      instance.namespace,
      instance.nodeName,
      instance.host,
      instance.port,
      instance.version,
      sessionNum,
      instance.tags)
  }

}

class EngineProviderBuilder() extends InstanceProviderBuilder[EngineInstance] {

  var namespace: String = _
  var zkClient: CuratorFramework = _
  var version: Option[String] = None
  var tags: ArrayBuffer[String] = ArrayBuffer()

  def namespace(namespace: String): EngineProviderBuilder = {
    this.namespace = namespace
    this
  }

  def zkClient(zkClient: CuratorFramework): EngineProviderBuilder = {
    this.zkClient = zkClient
    this
  }

  def version(version: String): EngineProviderBuilder = {
    this.version = Some(version)
    this
  }

  def version(version: Option[String]): EngineProviderBuilder = {
    version.foreach(v => this.version(v))
    this
  }

  def tag(tag: String): EngineProviderBuilder = {
    this.tags += tag
    this
  }

  def tags(tags: Seq[String]): EngineProviderBuilder = {
    this.tags ++= tags
    this
  }

  override def build(): InstanceProvider[EngineInstance] = {
    assert(this.namespace != null, "namespace cannot be empty")
    assert(this.zkClient != null, "zkClient cannot be empty")
    val engineProvider = new EngineProvider(namespace, zkClient)
    version.foreach(v => engineProvider.addFilter(VersionProviderFilter(v)))
    tags.foreach(t => engineProvider.addFilter(TagProviderFilter(t)))
    engineProvider
  }

}

object EngineProvider {

  def builder(): EngineProviderBuilder = new EngineProviderBuilder()

}
