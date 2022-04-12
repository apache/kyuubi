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
import java.net.{URI, URL}
import java.security.PrivilegedExceptionAction

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.hive.deploy.HiveSubmit.{LOCAL, YARN}
import org.apache.kyuubi.util.KyuubiHadoopUtils

class HiveSubmit extends Logging {

  private def doSubmit(args: Array[String]): Unit = {
    val submitArguments = parseArguments(args)
    if (submitArguments.verbose) {
      info(submitArguments.toString)
    }
    submit(submitArguments)
  }

  def parseArguments(args: Array[String]): HiveSubmitArguments = {
    new HiveSubmitArguments(args)
  }

  /**
   * Submit hive using the provided parameters.
   */
  def submit(args: HiveSubmitArguments): Unit = {

    def doRunMain(): Unit = {
      val proxyUserOpt = args.proxyUserOpt
      if (proxyUserOpt.isDefined) {
        val proxyUser = UserGroupInformation.createProxyUser(
          proxyUserOpt.get,
          UserGroupInformation.getCurrentUser)
        try {
          proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              runMain(args)
            }
          })
        } catch {
          case e: Exception =>
            error(s"Error: ${e.getMessage}", e)
            throw e
        }
      } else {
        runMain(args)
      }
    }

    doRunMain()
  }

  private[deploy] def prepareSubmitEnvironment(args: HiveSubmitArguments)
      : (Seq[String], Seq[String], KyuubiConf, String) = {
    // return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    val kyuubiConf = new KyuubiConf()
    args.hiveProperties.foreach { property =>
      kyuubiConf.set(property._1, property._2)
    }

    val clusterManager: Int = args.master match {
      case "local" => LOCAL
      case "yarn" => YARN
      case _ =>
        error("Master must either be yarn or local.")
        -1
    }

    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    args.classpath = Option(args.classpath)
      .map(resolveGlobPaths(_, hadoopConf)).orNull
    if (args.classpath != null) {
      childClasspath ++= args.classpath.split(",")
    }

    val childMainClass = clusterManager match {
      case LOCAL =>
        "org.apache.kyuubi.engine.hive.deploy.LocalHiveApplication"
      case YARN =>
        // TODO add yarn cluster
        throw new IllegalArgumentException(s"Unsupported clusterManager YARN.")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported clusterManager: $clusterManager.")
    }
    (childArgs, childClasspath, kyuubiConf, childMainClass)
  }

  private def runMain(args: HiveSubmitArguments): Unit = {
    val (childArgs, childClasspath, kyuubiConf, childMainClass) = prepareSubmitEnvironment(args)
    if (args.verbose) {
      info(s"Main class:\n$childMainClass")
      info(s"Arguments:\n${childArgs.mkString("\n")}")
      info(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      info(s"\n")
    }
    val loader = getSubmitClassLoader()
    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    val mainClass: Class[_] = classForName(childMainClass)
    val application: HiveApplication =
      if (classOf[HiveApplication].isAssignableFrom(mainClass)) {
        mainClass.getConstructor().newInstance().asInstanceOf[HiveApplication]
      } else {
        throw new IllegalStateException(s"The main class could be subclass of HiveApplication.")
      }

    application.start(childArgs.toArray, kyuubiConf)
  }

  def classForName[C](
      className: String,
      initialize: Boolean = true): Class[C] = {
    Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).asInstanceOf[
      Class[C]]
  }

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getCurrentClassLoader)

  def getCurrentClassLoader: ClassLoader = getClass.getClassLoader

  private def addJarToClasspath(localJar: String, loader: MutableURLClassLoader): Unit = {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          info(s"Loading jar $file to classpath.")
          loader.addURL(file.toURI.toURL)
        } else {
          warn(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        warn(s"Skip remote jar $uri")
    }
  }

  def resolveGlobPaths(paths: String, hadoopConf: Configuration): String = {
    require(paths != null, "paths cannot be null.")
    stringToSeq(paths).flatMap { path =>
      val (base, fragment) = splitOnFragment(path)
      (resolveGlobPath(base, hadoopConf), fragment) match {
        case (resolved, Some(_)) if resolved.length > 1 =>
          throw new KyuubiException(
            s"${base.toString} resolves ambiguously to multiple files: ${resolved.mkString(",")}")
        case (resolved, Some(namedAs)) => resolved.map(_ + "#" + namedAs)
        case (resolved, _) => resolved
      }
    }.mkString(",")
  }

  private def splitOnFragment(path: String): (URI, Option[String]) = {
    val uri = Utils.resolveURI(path)
    val withoutFragment = new URI(uri.getScheme, uri.getSchemeSpecificPart, null)
    (withoutFragment, Option(uri.getFragment))
  }

  private def resolveGlobPath(uri: URI, hadoopConf: Configuration): Array[String] = {
    uri.getScheme match {
      case "local" | "http" | "https" | "ftp" => Array(uri.toString)
      case _ =>
        val fs = FileSystem.get(uri, hadoopConf)
        Option(fs.globStatus(new Path(uri))).map { status =>
          status.filter(_.isFile).map(_.getPath.toUri.toString)
        }.getOrElse(Array(uri.toString))
    }
  }

  def stringToSeq(str: String): Seq[String] = {
    str.split(",").map(_.trim()).filter(_.nonEmpty)
  }

  private def getSubmitClassLoader(): MutableURLClassLoader = {
    val contextClassLoader = Thread.currentThread().getContextClassLoader
    val classLoader = new MutableURLClassLoader(new Array[URL](0), contextClassLoader)
    Thread.currentThread().setContextClassLoader(classLoader)
    classLoader
  }
}

object HiveSubmit extends Logging {
  private val LOCAL = 1
  private val YARN = 2

  def main(args: Array[String]): Unit = {
    val submit = new HiveSubmit()
    submit.doSubmit(args)
  }
}
