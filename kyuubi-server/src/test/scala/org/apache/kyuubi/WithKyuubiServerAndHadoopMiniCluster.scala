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
package org.apache.kyuubi

import java.io.File

import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KYUUBI_ENGINE_ENV_PREFIX
import org.apache.kyuubi.server.{MiniDFSService, MiniYarnService}
import org.apache.kyuubi.util.JavaUtils

trait WithKyuubiServerAndHadoopMiniCluster extends KyuubiFunSuite with WithKyuubiServer {

  val kyuubiHome: String = JavaUtils.getCodeSourceLocation(getClass).split("integration-tests").head

  override protected val conf: KyuubiConf = new KyuubiConf(false)

  private val hadoopConfDir: File = Utils.createTempDir().toFile

  protected var miniHdfsService: MiniDFSService = _

  protected var miniYarnService: MiniYarnService = _

  override def beforeAll(): Unit = {
    miniHdfsService = new MiniDFSService()
    miniHdfsService.initialize(conf)
    miniHdfsService.start()

    miniYarnService = new MiniYarnService()
    miniYarnService.initialize(conf)
    miniYarnService.start()

    miniHdfsService.saveHadoopConf(hadoopConfDir)
    miniYarnService.saveYarnConf(hadoopConfDir)

    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.KYUUBI_HOME", kyuubiHome)
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath)
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.YARN_CONF_DIR", hadoopConfDir.getAbsolutePath)

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (miniYarnService != null) {
      miniYarnService.stop()
      miniYarnService = null
    }
    if (miniHdfsService != null) {
      miniHdfsService.stop()
      miniHdfsService = null
    }
  }

  def getYarnMaximumAllocationMb: Int = {
    require(miniYarnService != null, "MiniYarnService is not initialized")
    miniYarnService.getYarnConf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024)
  }
}
