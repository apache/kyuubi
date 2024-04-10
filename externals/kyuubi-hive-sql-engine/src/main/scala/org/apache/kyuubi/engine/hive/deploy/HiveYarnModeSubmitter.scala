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

import java.io.File

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_HIVE_EXTRA_CLASSPATH
import org.apache.kyuubi.engine.deploy.yarn.EngineYarnModeSubmitter
import org.apache.kyuubi.engine.deploy.yarn.EngineYarnModeSubmitter.KYUUBI_ENGINE_DEPLOY_YARN_MODE_HIVE_CONF_KEY
import org.apache.kyuubi.engine.hive.HiveSQLEngine

object HiveYarnModeSubmitter extends EngineYarnModeSubmitter {

  def main(args: Array[String]): Unit = {
    Utils.fromCommandLineArgs(args, kyuubiConf)

    if (UserGroupInformation.isSecurityEnabled) {
      require(
        kyuubiConf.get(KyuubiConf.ENGINE_PRINCIPAL).isDefined
          && kyuubiConf.get(KyuubiConf.ENGINE_KEYTAB).isDefined,
        s"${KyuubiConf.ENGINE_PRINCIPAL.key} and " +
          s"${KyuubiConf.ENGINE_KEYTAB.key} must be set when submit " +
          s"${HiveSQLEngine.getClass.getSimpleName.stripSuffix("$")} to YARN")
    }
    run()
  }

  override var engineType: String = "hive"

  override def engineMainClass(): String = HiveSQLEngine.getClass.getName

  /**
   * Jar list for the Hive engine.
   */
  override def engineExtraJars(): Seq[File] = {
    val hadoopCp = sys.env.get("HIVE_HADOOP_CLASSPATH")
    val extraCp = kyuubiConf.get(ENGINE_HIVE_EXTRA_CLASSPATH)
    val jars = new ListBuffer[File]
    hadoopCp.foreach(cp => parseClasspath(cp, jars))
    extraCp.foreach(cp => parseClasspath(cp, jars))
    jars.toSeq
  }

  override def listConfFiles(): Seq[File] = {
    // respect the following priority loading configuration, and distinct files
    // hive configuration -> hadoop configuration -> yarn configuration
    val hiveConf = kyuubiConf.getOption(KYUUBI_ENGINE_DEPLOY_YARN_MODE_HIVE_CONF_KEY)
    listDistinctFiles(hiveConf.get) ++ super.listConfFiles()
  }
}
