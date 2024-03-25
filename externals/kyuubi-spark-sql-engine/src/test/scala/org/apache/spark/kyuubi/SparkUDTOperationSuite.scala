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

package org.apache.spark.kyuubi

import org.apache.spark.sql.types.UDTRegistration

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class SparkUDTOperationSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {
  override def withKyuubiConf: Map[String, String] = Map(
    KyuubiConf.ENGINE_SINGLE_SPARK_SESSION.key -> "true")

  override protected def jdbcUrl: String = getJdbcUrl

  test("retrieve UserDefinedType result") {
    UDTRegistration.register(classOf[ExampleValue].getName, classOf[ExampleValueUDT].getName)
    spark.udf.register(
      "exampleValueUdf",
      (param: Double) =>
        {
          ExampleValue(param)
        }: ExampleValue)

    withJdbcStatement() { stmt =>
      val result = stmt.executeQuery("SELECT exampleValueUdf(1.0)")
      assert(result.next())
      assert(result.getString(1) == ExampleValue(1.0).toString)
    }
  }
}
