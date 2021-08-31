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

package org.apache.kyuubi.sql.sqlclassification

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

case class KyuubiSqlClassification(session: SparkSession, delegate: ParserInterface)
  extends ParserInterface {

  import KyuubiSqlConf._

  override def parsePlan(sqlText: String): LogicalPlan = {
    val logicalPlan = delegate.parsePlan(sqlText)
    if (CLASSIFICATION_ENABLED) {
      val simpleName = logicalPlan.getClass.getSimpleName
      val sqlClassification = KyuubiGetSqlClassification.getSqlClassification(simpleName)
      session.conf.set(SPARK_SQL_CLASSIFICATION, sqlClassification)
    }
    logicalPlan
  }

  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }
}

object KyuubiSqlConf {

  import org.apache.kyuubi.sql.KyuubiSQLConf._

  val CLASSIFICATION_ENABLED = SQLConf.get.getConf(SQL_CLASSIFICATION_ENABLED)
  val SPARK_SQL_CLASSIFICATION = "spark.sql.classification"
}
