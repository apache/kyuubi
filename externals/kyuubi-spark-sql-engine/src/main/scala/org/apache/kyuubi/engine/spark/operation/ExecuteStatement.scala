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

package org.apache.kyuubi.engine.spark.operation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil
import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.session.Session

class ExecuteStatement(
    spark: SparkSession,
    session: Session,
    protected override val statement: String)
  extends SparkOperation(spark, OperationType.EXECUTE_STATEMENT, session) with Logging {

  private var result: DataFrame = _

  override protected def resultSchema: StructType = {
    if (result == null || result.schema.isEmpty) {
      new StructType().add("Result", "string")
    } else {
      result.schema
    }
  }

  override protected def runInternal(): Unit = {
    try {
      info(KyuubiSparkUtil.diagnostics(spark))
      spark.sparkContext.setJobGroup(statementId, statement)
      result = spark.sql(statement)
      iter = result.collect().toList.iterator
    } catch {
      onError(cancel = true)
    } finally {
      spark.sparkContext.clearJobGroup()
    }
  }
}
