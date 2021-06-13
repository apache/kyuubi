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
import org.apache.kyuubi.server.MiniYarnService
import org.apache.kyuubi.util.KyuubiHadoopUtils

/**
 * To developers:
 *   You should specify JAVA_HOME before running test with mini yarn server. Otherwise the error
 * may be thrown `/bin/bash: /bin/java: No such file or directory`.
 */
trait WithKyuubiServerOnYarn extends WithKyuubiServer {
  protected val kyuubiServerConf: KyuubiConf
  protected val connectionConf: Map[String, String]
  private var miniYarnService: MiniYarnService = _

  override protected lazy final val conf: KyuubiConf = {
    connectionConf.foreach { case (k, v) =>
      kyuubiServerConf.set(k, v)
    }
    kyuubiServerConf
  }

  override def beforeAll(): Unit = {
    miniYarnService = new MiniYarnService()
    miniYarnService.initialize(new KyuubiConf(false))
    miniYarnService.start()

    KyuubiHadoopUtils.toSparkPrefixedConf(miniYarnService.getHadoopConf()).foreach { case (k, v) =>
      conf.set(k, v)
    }
    conf.set(KYUUBI_ENGINE_ENV_PREFIX + "HADOOP_CONF_DIR", miniYarnService.getHadoopConfDir())
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (miniYarnService != null) {
      miniYarnService.stop()
      miniYarnService = null
    }
    super.afterAll()
  }
}
