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
import org.apache.kyuubi.ctl.KyuubiCtlActionService._
import org.apache.kyuubi.ha.HighAvailabilityConf._

class KyuubiCtlArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends KyuubiCtlArgumentsParser with Logging {
  var action: KyuubiCtlAction = null
  var service: KyuubiCtlActionService = null
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

  if (verbose) {
    info(toString)
  }

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
    service = Option(service).getOrElse(KyuubiCtlActionService.SERVER)
    action match {
      case KyuubiCtlAction.CREATE => validateCreateGetDeleteArguments()
      case KyuubiCtlAction.GET => validateCreateGetDeleteArguments()
      case KyuubiCtlAction.DELETE => validateCreateGetDeleteArguments()
      case KyuubiCtlAction.LIST => validateListArguments()
      case KyuubiCtlAction.HELP =>
      case _ => printUsageAndExit(-1)
    }
  }

  private def validateCreateGetDeleteArguments(): Unit = {
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
    if (service == KyuubiCtlActionService.ENGINE && user == null) {
      fail("Must specify user name for engine")
    }
  }

  private def validateListArguments(): Unit = {
    if (zkAddress == null) {
      fail("Zookeeper address is not specified and no default value to load")
    }
    if (nameSpace == null) {
      fail("Zookeeper namespace is not specified and no default value to load")
    }
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    if (unknownParam != null) {
      info("Unknown/unsupported param " + unknownParam)
    }
    val command = sys.env.getOrElse("_KYUUBI_CMD_USAGE",
      s"""
        |Kyuubi Ver $KYUUBI_VERSION.
        |Usage: kyuubi-ctl <create|get|delete|list>  <server|engine> --zk-quorum ...
        |--namespace ... --user ... --host ... --port ... --version""".stripMargin)
    info(command)

    info(
      s"""
         |Command:
         |  - create                    expose a service to a namespace on the zookeeper cluster of
         |                              zkAddress manually
         |  - get                       get the service node info
         |  - delete                    delete the specified serviceNode
         |  - list                      list all the service nodes for a particular domain
         |
         |Service:
         |  - server                    default
         |  - engine
         |
         |Arguments:
         |  --zk-quorum                 one of the zk ensemble address, using kyuubi-defaults/conf
         |                              if absent
         |  --namespace                 the namespace, using kyuubi-defaults/conf if absent
         |  --host                      hostname or IP address of a service
         |  --port                      listening port of a service
         |  --version                   using the compiled KYUUBI_VERSION default, change it if the
         |                              active service is running in another
         |  --user                      for engine service only, the user name this engine belong to
         |  --help                      Show this help message and exit.
         |  --verbose                   Print additional debug output.
      """.stripMargin
    )

    throw new KyuubiCtlException(exitCode)
  }

  override protected def parseActionAndService(args: JList[String]): Int = {
    if (args.isEmpty) {
      printUsageAndExit(-1)
    }

    var actionParsed = false
    var serviceParsed = false
    var offset = 0

    while(offset < args.size() && needContinueHandle() && !(actionParsed && serviceParsed)) {
     val arg = args.get(offset)
      if (!actionParsed) {
        arg match {
          case CREATE =>
            action = KyuubiCtlAction.CREATE
            actionParsed = true
          case GET =>
            action = KyuubiCtlAction.GET
            actionParsed = true
          case DELETE =>
            action = KyuubiCtlAction.DELETE
            actionParsed = true
          case LIST =>
            action = KyuubiCtlAction.LIST
            actionParsed = true
          case _ => findSwitches(arg) match {
            case HELP =>
              action = KyuubiCtlAction.HELP
              actionParsed = true
            case VERBOSE =>
              verbose = true
            case _ =>
              printUsageAndExit(-1, arg)
          }
        }
        offset += 1
      } else if (needContinueHandle() && !serviceParsed) {
        arg match {
          case SERVER =>
            service = KyuubiCtlActionService.SERVER
            serviceParsed = true
            offset += 1
          case ENGINE =>
            service = KyuubiCtlActionService.ENGINE
            serviceParsed = true
            offset += 1
          case _ => findSwitches(arg) match {
            case HELP =>
              action = KyuubiCtlAction.HELP
              offset += 1
            case VERBOSE =>
              verbose = true
              offset += 1
            case _ =>
              service = KyuubiCtlActionService.SERVER
              serviceParsed = true
          }
        }
      }
    }
    offset
  }

  override def toString: String = {
    s"""Parsed arguments:
       |  action                  $action
       |  service                 $service
       |  zkAddress               $zkAddress
       |  namespace               $nameSpace
       |  user                    $user
       |  host                    $host
       |  port                    $port
       |  version                 $version
       |  verbose                 $verbose
    """.stripMargin
  }

  private def needContinueHandle(): Boolean = {
    action != KyuubiCtlAction.HELP
  }

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    if (!needContinueHandle()) {
      return false
    }
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
        action = KyuubiCtlAction.HELP

      case VERBOSE =>
        verbose = true

      case _ =>
        fail(s"Unexpected argument '$opt'.")
    }
    needContinueHandle()
  }

  override protected def handleUnknown(opt: String): Boolean = {
    if (!needContinueHandle()) {
      false
    } else {
      printUsageAndExit(-1, opt)
      false
    }
  }

  private def fail(msg: String): Unit = throw new KyuubiException(msg)
}
