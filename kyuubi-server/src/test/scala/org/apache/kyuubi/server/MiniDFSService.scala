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

package org.apache.kyuubi.server

import java.io.{File, FileWriter}
import java.net.InetAddress

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService

class MiniDFSService(name: String, hdfsConf: Configuration)
  extends AbstractService(name)
  with Logging {
  private val hadoopConfDir: File = Utils.createTempDir().toFile
  private var hdfsCluster: MiniDFSCluster = _

  def this(hdfsConf: Configuration = new Configuration()) =
    this(classOf[MiniDFSService].getSimpleName, hdfsConf)

  override def initialize(conf: KyuubiConf): Unit = {
    // Set bind host to localhost to avoid java.net.BindException
    hdfsConf.setIfUnset("dfs.namenode.rpc-bind-host", "localhost")

    // enable proxy
    val currentUser = UserGroupInformation.getCurrentUser.getShortUserName
    hdfsConf.set(s"hadoop.proxyuser.$currentUser.groups", "*")
    hdfsConf.set(s"hadoop.proxyuser.$currentUser.hosts", "*")
    super.initialize(conf)
  }

  override def start(): Unit = {
    hdfsCluster = new MiniDFSCluster.Builder(hdfsConf)
      .checkDataNodeAddrConfig(true)
      .checkDataNodeHostConfig(true)
      .build()
    info(
      s"NameNode address in configuration is " +
        s"${hdfsConf.get(HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY)}")
    super.start()
    saveHadoopConf(hadoopConfDir)
  }

  override def stop(): Unit = {
    if (hdfsCluster != null) hdfsCluster.shutdown(true)
    super.stop()
  }

  def saveHadoopConf(hadoopConfDir: File): Unit = {
    val configToWrite = new Configuration(false)
    val hostName = InetAddress.getLocalHost.getHostName
    hdfsConf.iterator().asScala.foreach { kv =>
      val key = kv.getKey
      val value = kv.getValue.replaceAll(hostName, "localhost")
      configToWrite.set(key, value)
      getConf.set(key, value)
    }
    val writer = new FileWriter(new File(hadoopConfDir, "hdfs-site.xml"))
    configToWrite.writeXml(writer)
    writer.close()
  }

  def getHadoopConf: Configuration = hdfsConf
  def getDFSPort: Int = hdfsCluster.getNameNodePort
  def getHadoopConfDir: String = hadoopConfDir.getAbsolutePath
}
