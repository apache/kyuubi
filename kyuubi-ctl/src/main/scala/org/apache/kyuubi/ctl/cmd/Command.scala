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

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.cli.ControlCli
import org.apache.kyuubi.ctl.opt.CliConfig
import org.apache.kyuubi.ha.HighAvailabilityConf._

abstract class Command[T](cliConfig: CliConfig) extends Logging {

  val conf = KyuubiConf().loadFileDefaults()

  cliConfig.conf.foreach { case (key, value) =>
    conf.set(key, value)
  }

  val verbose = cliConfig.commonOpts.verbose

  val normalizedCliConfig: CliConfig = useDefaultPropertyValueIfMissing()

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  def validate(): Unit

  /** Run the command and return the internal result. */
  def doRun(): T

  /** Render the internal result. */
  def render(obj: T): Unit

  final def run(): Unit = {
    Option(doRun()).foreach(render)
  }

  def fail(msg: String): Unit = throw new KyuubiException(msg)

  protected def mergeArgsIntoKyuubiConf(): Unit = {
    conf.set(HA_ADDRESSES.key, normalizedCliConfig.zkOpts.zkQuorum)
    conf.set(HA_NAMESPACE.key, normalizedCliConfig.zkOpts.namespace)
  }

  private def useDefaultPropertyValueIfMissing(): CliConfig = {
    var arguments: CliConfig = cliConfig.copy()
    if (cliConfig.zkOpts.zkQuorum == null) {
      val quorum = conf.get(HA_ADDRESSES)
      arguments = arguments.copy(zkOpts = arguments.zkOpts.copy(zkQuorum = quorum))
      if (verbose) {
        super.info(s"Zookeeper quorum is not specified, use value from default conf: $quorum")
      }
    }

    if (arguments.zkOpts.namespace == null) {
      arguments = arguments.copy(zkOpts =
        arguments.zkOpts.copy(namespace = conf.get(HA_NAMESPACE)))
      if (verbose) {
        super.info(s"Zookeeper namespace is not specified, use value from default conf:" +
          s"${arguments.zkOpts.namespace}")
      }
    }

    if (arguments.zkOpts.version == null) {
      if (verbose) {
        super.info(s"version is not specified, use built-in KYUUBI_VERSION:$KYUUBI_VERSION")
      }
      arguments = arguments.copy(zkOpts = arguments.zkOpts.copy(version = KYUUBI_VERSION))
    }
    arguments
  }

  override def info(msg: => Any): Unit = ControlCli.printMessage(msg)
  override def warn(msg: => Any): Unit = ControlCli.printMessage(s"Warning: $msg")
  override def error(msg: => Any): Unit = ControlCli.printMessage(s"Error: $msg")
}
