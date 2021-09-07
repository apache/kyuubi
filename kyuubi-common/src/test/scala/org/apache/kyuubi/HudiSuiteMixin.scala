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

import java.nio.file.Path

trait HudiSuiteMixin extends DataLakeSuiteMixin {

  override protected def format: String = "hudi"

  override protected def catalog: String = "spark_catalog"

  override protected def warehouse: Path = Utils.createTempDir()

  override protected def extraJars: String = {
    var extraJars = ""
    System.getProperty("java.class.path")
      .split(":")
      .filter(_.contains("jar"))
      .foreach(i => extraJars += i + ",")
    extraJars.substring(0, extraJars.length - 1)
  }

  override def tableOptions: Map[String, String] = Map(
    "hoodie.bootstrap.index.class" -> "org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex")

  def getTableOptions(opts: (String, String)*): String = {
    var options = ""
    if (tableOptions.nonEmpty || opts.nonEmpty) {
      for ((k, v) <- tableOptions ++ opts) {
        options += s"$k='$v',"
      }
      options = options.substring(0, options.length() - 1)
      options = s"options ($options)"
    }
    options
  }

  override protected def extraConfigs = Map(
    "spark.sql.catalogImplementation" -> "in-memory",
    "spark.sql.defaultCatalog" -> catalog,
    "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.jars" -> extraJars)
}
