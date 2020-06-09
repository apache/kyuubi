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

package org.apache.kyuubi.spark

import scala.collection.JavaConverters._

import org.apache.hive.service.cli.CLIService
import org.apache.hive.service.cli.thrift.ThriftCLIService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

object SparkSQLEngineApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(loadDefaults = true)
    val session = SparkSession.builder()
      .config(conf)
      .appName("Kyuubi Spark SQL Engine App")
      .getOrCreate()

    val server = HiveThriftServer2.startWithContext(session.sqlContext)

    var thriftCLIService: ThriftCLIService = null
    var cliService: CLIService = null
    server.getServices.asScala.foreach {
      case t: ThriftCLIService if t.getPortNumber > 0 =>
        thriftCLIService = t
      case c: CLIService => cliService = c
      case _ =>
    }

    if (thriftCLIService.getPortNumber <= 0) {
      thriftCLIService.stop()
      try {
        thriftCLIService = new KyuubiThriftBinaryCliService(cliService)
        thriftCLIService.init(server.getHiveConf)
        thriftCLIService.start()
      }
    }
    val port = thriftCLIService.getPortNumber
    val hostName = thriftCLIService.getServerIPAddress.getHostName

  }

}
