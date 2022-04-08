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

package org.apache.kyuubi.plugin.tag

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.plugin.SessionConfAdvisor

abstract class TagConfAdvisor extends SessionConfAdvisor {

  protected val confProvider: TagConfProvider

  override def getConfOverlay(
      user: String,
      sessionConf: util.Map[String, String]): util.Map[String, String] = {

    val tagConf: mutable.Map[String, String] = mutable.Map[String, String]()

    // system conf
    confProvider.get(SystemTag()).foreach(tagConf ++= _)

    // server conf
    Option(sessionConf.get(KyuubiConf.SERVER_NAME.key)).foreach { serverName =>
      confProvider.get(ServerTag(serverName)).foreach(tagConf ++= _)
    }

    // user conf
    confProvider.get(UserTag(user)).foreach(tagConf ++= _)

    // tag conf
    Option.apply(sessionConf.get(KyuubiConf.SESSION_TAGS.key)).toSeq
      .flatMap(_.split(","))
      .foreach(tag => confProvider.get(OriginTag(tag)).foreach(tagConf ++= _))

    tagConf.asJava
  }
}
