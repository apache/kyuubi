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

package org.apache.kyuubi.spark.connector.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{File, FileOutputStream}

trait SparkYarnConnectorWithYarn extends WithKyuubiServerAndYarnMiniCluster {
  def writeConfigToFile(conf: Configuration, filePath: String): Unit = {
    val file = new File(filePath)
    info(s"xml path: ${file.getAbsolutePath}")
    val outputStream = new FileOutputStream(file)
    try {
      conf.writeXml(outputStream)
    } finally {
      outputStream.close()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // get all conf and set up hadoop conf dir
    if (!new File("tmp/hadoop-conf").exists()) new File("tmp/hadoop-conf").mkdirs()
    writeConfigToFile(miniHdfsService.getHadoopConf, "tmp/hadoop-conf/core-site.xml")
    writeConfigToFile(miniHdfsService.getHadoopConf, "tmp/hadoop-conf/hdfs-site.xml")
    writeConfigToFile(miniYarnService.getYarnConf, "tmp/hadoop-conf/yarn-site.xml")
    // init log dir and set permission
    val fs = FileSystem.get(hdfsConf)
    val logDir = new Path("/tmp/logs")
    fs.mkdirs(logDir)
    fs.setPermission(logDir, new org.apache.hadoop.fs.permission.FsPermission("777"))
    info(s"hdfs web address: http://${fs.getConf.get("dfs.http.address")}")
    fs.close()
    // mock app submit
    for (i <- 1 to 3) {
      submitMockTaskOnYarn()
    }
    // log all conf
    miniHdfsService.getHadoopConf.forEach(kv =>
      info(s"mini hdfs conf ${kv.getKey}: ${kv.getValue}"))
    miniYarnService.getYarnConf.forEach(kv => info(s"mini yarn conf ${kv.getKey}: ${kv.getValue}"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // delete hadoop conf dir

  }
}
