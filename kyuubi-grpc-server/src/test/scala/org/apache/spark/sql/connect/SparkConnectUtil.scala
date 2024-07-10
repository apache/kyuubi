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

package org.apache.spark.sql.connect

import java.net.InetSocketAddress
import java.time.Instant

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service._

object SparkConnectUtil {
  def startSparkConnectServer(): (String, Int) = {
    val _sparkConf = new SparkConf()
    _sparkConf.setIfMissing("spark.sql.binaryOutputStyle", "UTF8")
    _sparkConf.setIfMissing("spark.master", "local")
    val appName = s"kyuubi_test_spark_${Instant.now}"
    _sparkConf.setIfMissing("spark.app.name", appName)
    val session = SparkSession.builder().config(_sparkConf).getOrCreate()
    var isa: InetSocketAddress = null
    SparkConnectService.start(session.sparkContext)
    ("localhost", 15002)
  }
}
