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

package org.apache.kyuubi.server.mysql

import org.apache.kyuubi.Utils
import org.apache.kyuubi.operation.JDBCTestHelper

trait MySQLJDBCTestHelper extends JDBCTestHelper {

  override def jdbcDriverClass: String = "com.mysql.jdbc.Driver"

  protected lazy val user: String = Utils.currentUser

  protected val password: String = "kyuubi"

  private val _jdbcConfigs: Map[String, String] = Map(
    "useSSL" -> "false"
  )

  protected override def sessionConfigs: Map[String, String] = Map.empty

  protected override def jdbcConfigs: Map[String, String] = _jdbcConfigs

  protected override def jdbcVars: Map[String, String] = Map.empty

  protected def jdbcUrlWithConf(jdbcUrl: String): String = {
    val jdbcConfStr = if (jdbcConfigs.isEmpty) {
      ""
    } else {
      "?" + jdbcConfigs.map(kv => kv._1 + "=" + kv._2).mkString(";")
    }
    jdbcUrl + jdbcConfStr
  }
}
