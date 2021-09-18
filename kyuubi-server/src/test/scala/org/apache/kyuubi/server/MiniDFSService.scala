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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService

class MiniDFSService(name: String, hdfsConf: Configuration)
  extends AbstractService(name)
    with Logging {

  private var hdfsCluster: MiniDFSCluster = _

  def this(hdfsConf: Configuration = new Configuration()) =
    this(classOf[MiniDFSService].getSimpleName, hdfsConf)

  override def initialize(conf: KyuubiConf): Unit = {
    // Set bind host to localhost to avoid java.net.BindException
    hdfsConf.set("dfs.namenode.rpc-bind-host", "localhost")

    // enable proxy
    val currentUser = UserGroupInformation.getCurrentUser.getShortUserName
    hdfsConf.set(s"hadoop.proxyuser.$currentUser.groups", "*")
    hdfsConf.set(s"hadoop.proxyuser.$currentUser.hosts", "*")
    super.initialize(conf)
  }

  override def start(): Unit = {
    hdfsCluster = new MiniDFSCluster.Builder(hdfsConf)
      .checkDataNodeAddrConfig(true)
      .build()
    info(
      s"NameNode address in configuration is " +
        s"${hdfsConf.get(HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY)}")
    super.start()
  }

  override def stop(): Unit = {
    if (hdfsCluster != null) hdfsCluster.shutdown(true)
    super.stop()
  }

  def getHadoopConf: Configuration = hdfsConf
}
