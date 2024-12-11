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

trait GlutenSuiteMixin {
  protected def extraJars: String = {
    System.getProperty("java.class.path")
      .split(":")
      .filter(_.contains("gluten-velox-bundle-spark")).head
  }

  protected def extraConfigs: Map[String, String] = Map(
    "spark.plugins" -> "org.apache.gluten.GlutenPlugin",
    "spark.memory.offHeap.size" -> "4g",
    "spark.memory.offHeap.enabled" -> "true",
    "spark.shuffle.manager" -> "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
    "spark.gluten.ui.enabled" -> "false",
    "spark.jars" -> extraJars)
}
