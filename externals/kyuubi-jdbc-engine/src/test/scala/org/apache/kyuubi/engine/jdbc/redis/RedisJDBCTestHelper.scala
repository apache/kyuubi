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
package org.apache.kyuubi.engine.jdbc.redis

import org.apache.kyuubi.operation.JDBCTestHelper

trait RedisJDBCTestHelper extends JDBCTestHelper{
  override def jdbcDriverClass: String = "com.itmuch.redis.jdbc.redis.RedisDriver"

  protected val URL_PREFIX = "jdbc:redis://"

  protected def matchAllPatterns = Seq("", "*", "%", null, ".*", "_*", "_%", ".%")

  override protected lazy val user: String = null
  override protected val password = null
  private var _sessionConfigs: Map[String, String] = Map.empty
  private var _jdbcConfigs: Map[String, String] = Map.empty
  private var _jdbcVars: Map[String, String] = Map.empty

  override protected def sessionConfigs: Map[String, String] = _sessionConfigs

  override protected def jdbcConfigs: Map[String, String] = _jdbcConfigs

  override protected def jdbcVars: Map[String, String] = _jdbcVars

  override protected def jdbcUrlWithConf(jdbcUrl: String): String = {
    jdbcUrl
  }

}
