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

import java.nio.file.Paths

import scala.collection.JavaConverters._

import org.apache.kyuubi.client.api.v1.dto.BatchRequest
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.SparkProcessBuilder

trait BatchTestHelper {
  val sparkBatchTestBatchType = "SPARK"

  val sparkBatchTestMainClass = "org.apache.spark.examples.SparkPi"

  val sparkBatchTestAppName = "Spark Pi" // the app name is hard coded in spark example code

  val sparkBatchTestResource: Option[String] = {
    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", true, KyuubiConf())
    Paths.get(sparkProcessBuilder.sparkHome, "examples", "jars").toFile.listFiles().find(
      _.getName.startsWith("spark-examples")) map (_.getCanonicalPath)
  }

  def newBatchRequest(
      batchType: String,
      resource: String,
      className: String,
      name: String,
      conf: Map[String, String] = Map.empty,
      args: Seq[String] = Seq.empty): BatchRequest = {
    new BatchRequest(
      batchType,
      resource,
      className,
      name,
      conf.asJava,
      args.asJava)
  }

  def newSparkBatchRequest(
      conf: Map[String, String] = Map.empty,
      args: Seq[String] = Seq.empty): BatchRequest = {
    newBatchRequest(
      sparkBatchTestBatchType,
      sparkBatchTestResource.get,
      sparkBatchTestMainClass,
      sparkBatchTestAppName,
      conf,
      args)
  }
}
