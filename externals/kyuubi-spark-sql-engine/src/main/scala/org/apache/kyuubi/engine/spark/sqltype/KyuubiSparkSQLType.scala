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

package org.apache.kyuubi.engine.spark.sqltype

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.sql.SparkBaseSQLType

/**
 * This object is used to get this SQL's type.
 * There have 4 different types: DDL, DML, DQL and others.
 * By these SQL's type, we can do someting below:
 *    1. Reduce some log print.
 *       For Auxiliary Statements(the sql type is others), i don't need to print operation log.
 *    2. Optimizing some configuration item.
 *       For example, in final stage, the conf is different between DML and DQL.
 *       Through this configuration item, we can use different conf for them.
 *
 * The SQL_TYPE will store in the spark's conf.
 * The configuration item's name is ${sql.type}.
 */
object KyuubiSparkSQLType extends Logging{

  import org.apache.kyuubi.config.KyuubiConf._
  import org.apache.kyuubi.KyuubiSparkUtils._

  private val kyuubiConf: KyuubiConf = KyuubiConf()
  private val SQLTypeEnable: Boolean = kyuubiConf.get(ENGINE_SQL_TYPE_ENABLED)

  def getAndSetSQLType(spark: SparkSession, statement: String): Unit = {
    if (SQLTypeEnable) {
      // Generate the logicPlan and get it's simpleName
      val simpleName = spark.sessionState.sqlParser.parsePlan(statement).getClass.getSimpleName
      val sqlType = SparkBaseSQLType.getSQLType(simpleName)
      // Store it in spark.conf
      spark.conf.set(SQL_TYPE, sqlType)
    }
  }
}
