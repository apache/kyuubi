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

import java.net.InetAddress
import java.util.{List => JList}

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.ServiceControlAction._
import org.apache.kyuubi.ctl.ServiceControlObject._
import org.apache.kyuubi.ha.HighAvailabilityConf._

class ServiceControlCliArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends ServiceControlCliArgumentsParser with Logging {
  var action: ServiceControlAction = null
  var service: ServiceControlObject = null
  var zkQuorum: String = null
  var namespace: String = null
  var user: String = null
  var host: String = null
  var port: String = null
  var version: String = null
  var verbose: Boolean = false

  val conf = KyuubiConf().loadFileDefaults()

  // Set parameters from command line arguments
  parse(args.asJava)

  // Use default property value if not set
  useDefaultPropertyValueIfMissing()

  validateArguments()

  private def useDefaultPropertyValueIfMissing(): Unit = {
    if (zkQuorum == null) {
      conf.getOption(HA_ZK_QUORUM.key).foreach { v =>
        if (verbose) {
          info(s"Zookeeper quorum is not specified, use value from default conf:$v")
        }
        zkQuorum = v
      }
    }

    // for create action, it only expose Kyuubi service instance to another domain,
    // so we do not use namespace from default conf
    if (action != ServiceControlAction.CREATE && namespace == null) {
      namespace = conf.getOption(HA_ZK_NAMESPACE.key).getOrElse{
        val defaultNamespace = conf.get(HA_ZK_NAMESPACE)
        if (verbose) {
          info(s"Zookeeper namespace is not specified," +
            s" use value from default conf:$defaultNamespace")
        }
        defaultNamespace
      }
    }

    if (version == null) {
      if (verbose) {
        info(s"version is not specified, use built-in KYUUBI_VERSION:$KYUUBI_VERSION")
      }
      version = KYUUBI_VERSION
    }
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def validateArguments(): Unit = {
    service = Option(service).getOrElse(ServiceControlObject.SERVER)
    action match {
      case ServiceControlAction.CREATE => validateCreateActionArguments()
      case ServiceControlAction.GET => validateGetDeleteActionArguments()
      case ServiceControlAction.DELETE => validateGetDeleteActionArguments()
      case ServiceControlAction.LIST => validateListActionArguments()
      case ServiceControlAction.HELP =>
      case _ => printUsageAndExit(-1)
    }
  }

  private def validateCreateActionArguments(): Unit = {
    if (service != ServiceControlObject.SERVER) {
      fail("Only support expose Kyuubi server instance to another domain")
    }
    validateZkArguments()

    val defaultNamespace = conf.getOption(HA_ZK_NAMESPACE.key)
    if (defaultNamespace.isEmpty || defaultNamespace.get.equals(namespace)) {
      fail(
        s"""
          |Only support expose Kyuubi server instance to another domain, but the default
          |namespace is [$defaultNamespace] and specified namespace is [$namespace]
        """.stripMargin)
    }
  }

  private def validateGetDeleteActionArguments(): Unit = {
    validateZkArguments()
    validateHostAndPort()
    validateUser()
    mergeArgsIntoKyuubiConf()
  }

  private def validateListActionArguments(): Unit = {
    validateZkArguments()
    mergeArgsIntoKyuubiConf()
  }

  private def mergeArgsIntoKyuubiConf(): Unit = {
    conf.set(HA_ZK_QUORUM.key, zkQuorum)
    conf.set(HA_ZK_NAMESPACE.key, namespace)
  }

  private def validateZkArguments(): Unit = {
    if (zkQuorum == null) {
      fail("Zookeeper quorum is not specified and no default value to load")
    }
    if (namespace == null) {
      if (action == ServiceControlAction.CREATE) {
        fail("Zookeeper namespace is not specified")
      } else {
        fail("Zookeeper namespace is not specified and no default value to load")
      }
    }
  }

  private def validateHostAndPort(): Unit = {
    if (host == null) {
      fail("Must specify host for service")
    }
    if (port == null) {
      fail("Must specify port for service")
    }

    try {
      host = InetAddress.getByName(host).getCanonicalHostName
    } catch {
      case _: Exception =>
        fail(s"Unknown host: $host")
    }

    try {
      if (port.toInt <= 0 ) {
        fail(s"Specified port should be a positive number")
      }
    } catch {
      case _: NumberFormatException =>
        fail(s"Specified port is not a valid integer number: $port")
    }
  }

  private def validateUser(): Unit = {
    if (service == ServiceControlObject.ENGINE && user == null) {
      fail("Must specify user name for engine")
    }
  }

  private[ctl] def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    if (unknownParam != null) {
      info("Unknown/unsupported param " + unknownParam)
    }
    val command =
      s"""
        |Kyuubi Ver $KYUUBI_VERSION.
        |Usage: kyuubi-ctl <create|get|delete|list>  <server|engine> --zk-quorum ...
        |--namespace ... --user ... --host ... --port ... --version""".stripMargin
    info(command)

    info(
      s"""
         |Command:
         |  - create                    expose a service to a namespace on the zookeeper cluster of
         |                              zk quorum manually
         |  - get                       get the service node info
         |  - delete                    delete the specified serviceNode
         |  - list                      list all the service nodes for a particular domain
         |
         |Service:
         |  - server                    default
         |  - engine
         |
         |Arguments:
         |  --zk-quorum                 The connection string for the zookeeper ensemble, using
         |                              kyuubi-defaults/conf if absent
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

    throw new ServiceControlCliException(exitCode)
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
            action = ServiceControlAction.CREATE
            actionParsed = true
          case GET =>
            action = ServiceControlAction.GET
            actionParsed = true
          case DELETE =>
            action = ServiceControlAction.DELETE
            actionParsed = true
          case LIST =>
            action = ServiceControlAction.LIST
            actionParsed = true
          case _ => findSwitches(arg) match {
            case HELP =>
              action = ServiceControlAction.HELP
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
            service = ServiceControlObject.SERVER
            serviceParsed = true
            offset += 1
          case ENGINE =>
            service = ServiceControlObject.ENGINE
            serviceParsed = true
            offset += 1
          case _ => findSwitches(arg) match {
            case HELP =>
              action = ServiceControlAction.HELP
              offset += 1
            case VERBOSE =>
              verbose = true
              offset += 1
            case _ =>
              service = ServiceControlObject.SERVER
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
       |  zkQuorum                $zkQuorum
       |  namespace               $namespace
       |  user                    $user
       |  host                    $host
       |  port                    $port
       |  version                 $version
       |  verbose                 $verbose
    """.stripMargin
  }

  private def needContinueHandle(): Boolean = {
    action != ServiceControlAction.HELP
  }

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    if (!needContinueHandle()) {
      return false
    }
    opt match {
      case ZK_QUORUM =>
        zkQuorum = value

      case NAMESPACE =>
        namespace = value

      case USER =>
        user = value

      case HOST =>
        host = value

      case PORT =>
        port = value

      case VERSION =>
        version = value

      case HELP =>
        action = ServiceControlAction.HELP

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
