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

package org.apache.kyuubi.plugin.tag.file

import java.io.File
import java.util.{Timer, TimerTask}
import java.util.concurrent.ConcurrentHashMap

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf.{KYUUBI_CONF_DIR, KYUUBI_HOME}
import org.apache.kyuubi.plugin.tag.TagConfProvider
import org.apache.kyuubi.plugin.tag.file.FileTagConfProvider._

private[tag] class FileTagConfProvider extends TagConfProvider {

  override protected def get(key: String): Option[Map[String, String]] = {
    Option(CONF_MAP.get(key))
  }

}

private[tag] object FileTagConfProvider {

  def apply(): FileTagConfProvider = {
    load()
    new FileTagConfProvider()
  }

  val TAG_CONF_FILE_NAME: String = "kyuubi-tags.conf"

  val CONF_MAP = new ConcurrentHashMap[String, Map[String, String]]()

  private val timer = new Timer("FileTagConfRefreshTimer", true)
  private val period = 5 * 60 * 1000
  timer.scheduleAtFixedRate(
    new TimerTask {
      override def run(): Unit = load()
    },
    period,
    period)

  private val tagConfKeyRegex = "(__.*?__)\\.(.*)".r

  def load(): Unit = {
    val maybeConfigFile = getPropertiesFile()
    Utils.getPropertiesFromFile(maybeConfigFile).toSeq
      .map(c => (tagConfKeyRegex.findFirstMatchIn(c._1), c._2))
      .filter(i => i._1.isDefined)
      .map { item =>
        val tagKey = item._1.get.group(1)
        val confKey = item._1.get.group(2)
        (tagKey, (confKey, item._2))
      }.groupBy(_._1)
      .map { item =>
        (item._1, item._2.map(_._2).toMap)
      }
      .foreach {
        case (k, v) => CONF_MAP.put(k, v)
        case _ =>
      }
  }

  private def getPropertiesFile(env: Map[String, String] = sys.env): Option[File] = {
    env.get(KYUUBI_CONF_DIR)
      .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "conf"))
      .map(d => new File(d + File.separator + TAG_CONF_FILE_NAME))
      .filter(_.exists())
      .orElse {
        Option(getClass.getClassLoader.getResource(TAG_CONF_FILE_NAME)).map { url =>
          new File(url.getFile)
        }.filter(_.exists())
      }
  }

}
