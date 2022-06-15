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
package org.apache.kyuubi.ctl.cmd

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.HashMap

import org.yaml.snakeyaml.Yaml

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.CliConfig
import org.apache.kyuubi.ctl.ControlCli.printMessage
import org.apache.kyuubi.ha.HighAvailabilityConf._

abstract class Command(cliConfig: CliConfig) extends Logging {

  protected val DEFAULT_LOG_QUERY_INTERVAL: Int = 1000

  val conf = KyuubiConf().loadFileDefaults()

  val verbose = cliConfig.commonOpts.verbose

  val normalizedCliConfig: CliConfig = useDefaultPropertyValueIfMissing()

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  def validate(): Unit

  def run(): Unit

  def fail(msg: String): Unit = throw new KyuubiException(msg)

  protected def mergeArgsIntoKyuubiConf(): Unit = {
    conf.set(HA_ADDRESSES.key, normalizedCliConfig.commonOpts.zkQuorum)
    conf.set(HA_NAMESPACE.key, normalizedCliConfig.commonOpts.namespace)
  }

  private def useDefaultPropertyValueIfMissing(): CliConfig = {
    var arguments: CliConfig = cliConfig.copy()
    if (cliConfig.commonOpts.zkQuorum == null) {
      conf.getOption(HA_ADDRESSES.key).foreach { v =>
        if (verbose) {
          super.info(s"Zookeeper quorum is not specified, use value from default conf:$v")
        }
        arguments = arguments.copy(commonOpts = arguments.commonOpts.copy(zkQuorum = v))
      }
    }

    if (arguments.commonOpts.namespace == null) {
      arguments = arguments.copy(commonOpts =
        arguments.commonOpts.copy(namespace = conf.get(HA_NAMESPACE)))
      if (verbose) {
        super.info(s"Zookeeper namespace is not specified, use value from default conf:" +
          s"${arguments.commonOpts.namespace}")
      }
    }

    if (arguments.commonOpts.version == null) {
      if (verbose) {
        super.info(s"version is not specified, use built-in KYUUBI_VERSION:$KYUUBI_VERSION")
      }
      arguments = arguments.copy(commonOpts = arguments.commonOpts.copy(version = KYUUBI_VERSION))
    }
    arguments
  }

  private[ctl] def readConfig(): HashMap[String, Object] = {
    var filename = normalizedCliConfig.createOpts.filename

    var map: HashMap[String, Object] = null
    var br: BufferedReader = null
    try {
      val yaml = new Yaml()
      val input = new FileInputStream(new File(filename))
      br = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))
      map = yaml.load(br).asInstanceOf[HashMap[String, Object]]
    } catch {
      case e: Exception => fail(s"Failed to read yaml file[$filename]: $e")
    } finally {
      if (br != null) {
        br.close()
      }
    }
    map
  }

  override def info(msg: => Any): Unit = printMessage(msg)

  override def warn(msg: => Any): Unit = printMessage(s"Warning: $msg")

  override def error(msg: => Any): Unit = printMessage(s"Error: $msg")

}
