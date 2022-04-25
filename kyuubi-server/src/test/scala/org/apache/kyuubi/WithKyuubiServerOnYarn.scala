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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KYUUBI_ENGINE_ENV_PREFIX
import org.apache.kyuubi.engine.YarnApplicationOperation
import org.apache.kyuubi.server.MiniYarnService

/**
 * To developers:
 *   You should specify JAVA_HOME before running test with mini yarn server. Otherwise the error
 * may be thrown `/bin/bash: /bin/java: No such file or directory`.
 */
trait WithKyuubiServerOnYarn extends WithKyuubiServer {
  override protected val conf: KyuubiConf = new KyuubiConf()

  protected lazy val yarnOperation: YarnApplicationOperation = {
    val operation = new YarnApplicationOperation()
    operation.initialize(miniYarnService.getConf)
    operation
  }

  protected var miniYarnService: MiniYarnService = _

  override def beforeAll(): Unit = {
    conf.set("spark.master", "yarn")
      .set("spark.executor.instances", "1")
    miniYarnService = new MiniYarnService()
    miniYarnService.initialize(conf)
    miniYarnService.start()
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CONF_DIR", miniYarnService.getHadoopConfDir)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    // stop kyuubi server
    // stop yarn operation client
    // stop yarn cluster
    super.afterAll()
    yarnOperation.stop()
    if (miniYarnService != null) {
      miniYarnService.stop()
      miniYarnService = null
    }
  }
}
