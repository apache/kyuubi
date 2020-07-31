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

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.CompositeService
import org.apache.kyuubi.util.SignalRegister

class SparkSQLRunner(name: String) extends CompositeService(name) {
  def this() = this(classOf[SparkSQLRunner].getSimpleName)

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
  }
}


object SparkSQLRunner extends Logging {

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)
    val kyuubiConf = KyuubiConf()
    val sparkConf = new SparkConf()
    val prefix = "spark.kyuubi."
    sparkConf.getAllWithPrefix(prefix).foreach { case (k, v) =>
      kyuubiConf.set(k, v)
    }

    sparkConf.get("spark.app.name", Seq(Utils.currentUser,
      classOf[SparkSQLRunner].getSimpleName,
      UUID.randomUUID()).mkString("_"))

    val appName = Utils.currentUser + ""
    SparkSession
      .builder()
      .enableHiveSupport()
      .appName(s"")
  }
}
