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
package org.apache.kyuubi.engine.hive.deploy

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.deploy.yarn.EngineYarnModeSubmitter
import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.util.KyuubiHadoopUtils

object HiveYarnModeSubmitter extends EngineYarnModeSubmitter {

  def main(args: Array[String]): Unit = {
    Utils.fromCommandLineArgs(args, kyuubiConf)
    // Initialize the engine submitter.
    init()
    // Submit the engine application to YARN.
    submitApplication()
  }

  private def init(): Unit = {
    yarnConf = KyuubiHadoopUtils.newYarnConfiguration(kyuubiConf)
    hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
  }

  override var engineType: String = "hive"

  override def engineMainClass(): String = HiveSQLEngine.getClass.getName
}
