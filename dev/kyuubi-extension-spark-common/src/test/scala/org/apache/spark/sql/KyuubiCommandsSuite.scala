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
import org.apache.spark.sql.internal.StaticSQLConf
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import org.apache.kyuubi.sql.KyuubiSparkSQLCommonExtension

class KyuubiCommandsSuite extends KyuubiSparkSQLExtensionTest with Eventually{
  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[KyuubiSparkSQLCommonExtension].getCanonicalName)
  }

  test("kyuubi defined procedure - show_procedures") {
    val procedures = sql("EXEC show_procedures()")
    assert(!procedures.isEmpty)
  }

  test("test invalid procedure parameter") {
    intercept[AnalysisException](sql("EXEC desc_procedure(invalid => 'non_exist_procedure')"))
  }

  test("kyuubi defined procedure - desc_procedure") {
    assert(!sql("EXEC desc_procedure('stop_engine')").isEmpty)
    assert(!sql("EXEC desc_procedure(name => 'stop_engine')").isEmpty)
    intercept[AnalysisException](sql("EXEC desc_procedure('non_exist_procedure')"))
  }

  test("kyuubi defined procedure - stop_engine") {
    sql("EXEC stop_engine()")
    eventually (timeout(Span(10, Seconds)), interval(Span(1, Seconds))) {
      assert(spark.sparkContext.isStopped)
    }
  }
}
