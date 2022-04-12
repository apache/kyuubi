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

import java.util

import scala.collection.mutable

import org.apache.kyuubi.Logging

class HiveSubmitArguments(args: Array[String]) extends HiveSubmitOptionParser with Logging {

  var master: String = null

  var deployMode: String = null

  var classpath: String = null

  var proxyUserOpt: Option[String] = Option.empty

  var verbose: Boolean = false

  val hiveProperties: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()

  parse(scala.collection.JavaConverters.seqAsJavaList(args))

  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case MASTER =>
        master = value
      case DEPLOY_MODE =>
        deployMode = value
      case PROXY_USER =>
        proxyUserOpt = Option.apply(value)
      case VERBOSE =>
        verbose = true
      case CLASSPATH =>
        classpath = value
      case CONF =>
        val (confKey, confValue) = parseConfProperty(value)
        hiveProperties(confKey) = confValue
      case _ =>
        error(s"Unsupported argument '$opt'")
        return false
    }
    true
  }

  private def parseConfProperty(pair: String): (String, String) = {
    pair.split("=", 2).toSeq match {
      case Seq(k, v) => (k, v)
      case _ =>
        error(s"Hive config without '=': $pair")
        throw new IllegalArgumentException(s"Hive config without '=': $pair")
    }
  }

  override protected def handleUnknown(opt: String): Boolean = {
    false
  }

  override protected def handleExtraArgs(extra: util.List[String]): Unit = {}
}
