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
package org.apache.kyuubi.engine.jdbc.dialect
import java.util

import org.apache.kyuubi.engine.jdbc.redis.{RedisSchemaHelper, RedisTRowSetGenerator}
import org.apache.kyuubi.engine.jdbc.schema.{JdbcTRowSetGenerator, SchemaHelper}
import org.apache.kyuubi.session.Session

class RedisDialect extends JdbcDialect {

  override def getTablesQuery(
      catalog: String,
      schema: String,
      tableName: String,
      tableTypes: util.List[String]): String = {
    ""
  }

  override def getColumnsQuery(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): String = {
    ""
  }

  override def getTRowSetGenerator(): JdbcTRowSetGenerator = {
    new RedisTRowSetGenerator
  }

  override def getSchemaHelper(): SchemaHelper = {
    new RedisSchemaHelper
  }

  override def name(): String = {
    "redis"
  }
}
