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

package org.apache.kyuubi.plugin.lineage.dispatcher.atlas

import java.util

import scala.collection.JavaConverters._

import org.apache.atlas.ApplicationProperties
import org.apache.commons.configuration.Configuration
import org.apache.spark.kyuubi.lineage.SparkContextHelper

class AtlasClientConf(configuration: Configuration) {

  def get(entry: ConfigEntry): String = {
    configuration.getProperty(entry.key) match {
      case s: String => s
      case jl: util.List[_] => jl.asScala.mkString(",")
      case o if o != null => o.toString
      case _ => entry.defaultValue
    }
  }

}

object AtlasClientConf {

  private lazy val clientConf: AtlasClientConf = {
    val conf = ApplicationProperties.get()
    SparkContextHelper.globalSparkContext.getConf.getAllWithPrefix("spark.atlas.")
      .foreach { case (k, v) => conf.setProperty(s"atlas.$k", v) }
    new AtlasClientConf(conf)
  }

  def getConf(): AtlasClientConf = clientConf

  val ATLAS_REST_ENDPOINT = ConfigEntry("atlas.rest.address", "http://localhost:21000")

  val CLIENT_TYPE = ConfigEntry("atlas.client.type", "rest")
  val CLIENT_USERNAME = ConfigEntry("atlas.client.username", null)
  val CLIENT_PASSWORD = ConfigEntry("atlas.client.password", null)

  val CLUSTER_NAME = ConfigEntry("atlas.cluster.name", "primary")

  val COLUMN_LINEAGE_ENABLED = ConfigEntry("atlas.hook.spark.column.lineage.enabled", "true")
}
