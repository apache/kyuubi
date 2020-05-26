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

package org.apache.kyuubi

import java.io.{File, InputStreamReader, IOException}
import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.collection.JavaConverters._

private[kyuubi] object Utils extends Logging {

  import org.apache.kyuubi.config.KyuubiConf._

  def strToSeq(s: String): Seq[String] = {
    require(s != null)
    s.split(",").map(_.trim).filter(_.nonEmpty)
  }

  def getSystemProperties: Map[String, String] = {
    sys.props.toMap
  }

  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): Option[File] = {
    env.get(KYUUBI_CONF_DIR)
      .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "/conf"))
      .map( d => new File(d + File.separator + KYUUBI_CONF_FILE_NAME))
      .filter(f => f.exists() && f.isFile)
  }

  def getPropertiesFromFile(file: Option[File]): Map[String, String] = {
    file.map { f =>
      val reader = new InputStreamReader(f.toURI.toURL.openStream(), StandardCharsets.UTF_8)
      try {
        val properties = new Properties()
        properties.load(reader)
        properties.stringPropertyNames().asScala.map { k =>
          (k, properties.getProperty(k).trim)
        }.toMap
      } catch {
        case e: IOException =>
          throw new KyuubiException(
            s"Failed when loading Kyuubi properties from ${f.getAbsolutePath}", e)
      } finally {
        reader.close()
      }
    }.getOrElse(Map.empty)
  }
}
