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

package org.apache.kyuubi.engine.spark

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class SparkBatchProcessBuilderSuite extends KyuubiFunSuite {
  test("spark batch conf should be converted with `spark.` prefix") {
    val builder = new SparkBatchProcessBuilder(
      "kyuubi",
      KyuubiConf(false),
      UUID.randomUUID().toString,
      "test",
      Some("test"),
      "test",
      Map("kyuubi.key" -> "value"),
      Seq.empty,
      None)
    assert(builder.commands.toSeq.contains("spark.kyuubi.key=value"))
  }

  test("spark.kubernetes.file.upload.path supports placeholder") {
    val conf1 = KyuubiConf(false)
    conf1.set("spark.master", "k8s://test:12345")
    conf1.set("spark.submit.deployMode", "cluster")
    conf1.set("spark.kubernetes.file.upload.path", "hdfs:///spark-upload-{{YEAR}}{{MONTH}}{{DAY}}")
    val builder1 = new SparkBatchProcessBuilder(
      "",
      conf1,
      UUID.randomUUID().toString,
      "test",
      Some("test"),
      "test",
      Map("kyuubi.key" -> "value"),
      Seq.empty,
      None)
    val commands1 = builder1.toString.split(' ')
    val toady = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now())
    assert(commands1.contains(s"spark.kubernetes.file.upload.path=hdfs:///spark-upload-$toady"))
  }
}
