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

package org.apache.kyuubi.ctl

import java.util.{List => JList}

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, Logging, Utils}
import org.apache.kyuubi.ctl.KyuubiCtlAction._
import org.apache.kyuubi.ctl.KyuubiCtlActionRole._
import org.apache.kyuubi.ha.HighAvailabilityConf._

class KyuubiCtlArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends KyuubiCtlArgumentsParser with Logging {
  var action: KyuubiCtlAction = null
  var role: KyuubiCtlActionRole = null
  var zkAddress: String = null
  var nameSpace: String = null
  var user: String = null
  var host: String = null
  var port: String = null
  var version: String = null
  var verbose: Boolean = false

  /** Default properties present in the currently defined default file. */
  lazy val defaultKyuubiProperties: Map[String, String] = {
    val maybeConfigFile = Utils.getDefaultPropertiesFile(env)
    if (verbose) {
      info(s"Using properties file: $maybeConfigFile")
    }
    val defaultProperties = Utils.getPropertiesFromFile(maybeConfigFile)
    // Property files may contain sensitive information, so redact before printing
    if (verbose) {
      defaultProperties.foreach { case (k, v) =>
        info(s"Adding default property: $k=$v")
      }
    }
    defaultProperties
  }

  // Set parameters from command line arguments
  parse(args.asJava)

  // Use default property value if not set
  useDefaultPropertyValueIfMissing()

  validateArguments()

  private def useDefaultPropertyValueIfMissing(): Unit = {
    if (zkAddress == null) {
      zkAddress = defaultKyuubiProperties.getOrElse(HA_ZK_QUORUM.key, null)
    }
    if (nameSpace == null) {
      nameSpace = defaultKyuubiProperties.getOrElse(HA_ZK_NAMESPACE.key, null)
    }
    if (version == null) {
      version = KYUUBI_VERSION
    }
  }
  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def validateArguments(): Unit = {
    action match {
      case KyuubiCtlAction.CREATE => validateActionArguments()
      case KyuubiCtlAction.GET => validateActionArguments()
      case KyuubiCtlAction.DELETE => validateActionArguments()
      case KyuubiCtlAction.LIST => validateActionArguments()
    }
  }

  private def validateActionArguments(): Unit = {
    if (zkAddress == null) {
      fail("Zookeeper address is not specified and no default value to load")
    }
    if (nameSpace == null) {
      fail("Zookeeper namespace is not specified and no default value to load")
    }
    if (version == null) {
      fail("Kyuubi version is not specified and could not found KYUUBI_VERSION in " +
        "kyuubi-build-info")
    }
    if (host == null) {
      fail("Must specify host for service")
    }
    if (port == null) {
      fail("Must specify port for service")
    }
    if (role == KyuubiCtlActionRole.ENGINE && user == null) {
      fail("Must specify user name for engine")
    }
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    if (unknownParam != null) {
      info("Unknown/unsupported param " + unknownParam)
    }
    val command = sys.env.getOrElse("_KYUUBI_CMD_USAGE",
      """Usage: kyuubi-service <create|get|delete|list>  <server|engine> --zkAddress ...
        |--namespace ... --user ... --host ... --port ... --version""".stripMargin)
    info(command)

    info(
      s"""
         |Operations:
         |  - create                    expose a service to a namespace, this case is rare but
         |                              sometimes we may want one server to be reached in 2 or more
         |                              namespaces by different user groups
         |  - get                       get the service node info
         |  - delete                    delete the specified serviceNode
         |  - list                      list all the service nodes for a particular domain
         |
         |Role:
         |  - server                    default
         |  - engine
         |
         |Args:
         |  --zkAddress                 one of the zk ensemble address, using kyuubi-defaults/conf if absent
         |  --namespace                 the namespace, using kyuubi-defaults/conf if absent
         |  --user
         |  --host
         |  --port
         |  --version
      """.stripMargin
    )

    throw new KyuubiCtlException(exitCode)
  }

  override protected def parseActionAndRole(args: JList[String]): Int = {
    if (args.isEmpty) {
      printUsageAndExit(-1)
    }

    args.get(0) match {
      case CREATE =>
        action = KyuubiCtlAction.CREATE
      case GET =>
        action = KyuubiCtlAction.GET
      case DELETE =>
        action = KyuubiCtlAction.DELETE
      case LIST =>
        action = KyuubiCtlAction.LIST
      case HELP =>
        printUsageAndExit(0)
      case _ =>
        printUsageAndExit(-1, args.get(0))
    }

    if (args.size() == 1) {
      1
    } else {
      args.get(1) match {
        case SERVER =>
          role = KyuubiCtlActionRole.SERVER
          2
        case ENGINE =>
          role = KyuubiCtlActionRole.ENGINE
          2
        case _ =>
          role = KyuubiCtlActionRole.SERVER
          1
      }
    }
  }

  override def toString: String = {
    s"""Parsed arguments:
       |  action                  $action
       |  role                    $role
       |  zkAddress               $zkAddress
       |  namespace               $nameSpace
       |  user                    $user
       |  host                    $host
       |  port                    $port
       |  version                 $version
       |  verbose                 $verbose
    """.stripMargin
  }

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case ZK_ADDRESS =>
        zkAddress = value

      case NAMESPACE =>
        nameSpace = value

      case USER =>
        user = value

      case HOST =>
        host = value

      case PORT =>
        port = value

      case VERSION =>
        version = value

      case HELP =>
        printUsageAndExit(0)

      case VERBOSE =>
        verbose = true

      case _ =>
        fail(s"Unexpected argument '$opt'.")
    }
    true
  }

  override protected def handleUnknown(opt: String): Boolean = {
    if (opt.startsWith("-")) {
      fail(s"Unrecognized option '$opt'.")
    }
    false
  }

  private def fail(msg: String): Unit = throw new KyuubiException(msg)
}
