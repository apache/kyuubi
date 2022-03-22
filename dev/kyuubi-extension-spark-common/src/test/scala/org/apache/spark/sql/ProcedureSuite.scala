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
package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper
import org.apache.spark.sql.internal.StaticSQLConf

class ProcedureSuite extends KyuubiSparkSQLExtensionTest with ExpressionEvalHelper {
  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.kyuubi.sql.KyuubiSparkSQLCommonExtension")
  }

  test("parse call statements") {
    val p1 = sql(
      "call spark.kyuubi.create_tpcds(db=>'tpcds_sf10', sf=>10, format=>'parquet')"
    ).queryExecution.analyzed
    assert(p1 != null)
  }

}
