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

package org.apache.kyuubi.sql

import org.apache.spark.sql.KyuubiSparkSQLExtensionTest

class GlutenPlanManagerSuite extends KyuubiSparkSQLExtensionTest {

  test("Kyuubi Extension fast fail with Plan if over un-supported operator threshold") {
    withSQLConf(
      KyuubiSQLConf.GLUTEN_FALLBACK_OPERATOR_THRESHOLD.key -> "1") {
      withTable("gluten_tmp_1") {
        sql("CREATE TABLE gluten_tmp_1 (c1 int) USING JSON PARTITIONED BY (c2 string)")
        assertThrows[TooMuchGlutenUnsupportedOperationException] {
          sql("SELECT * FROM gluten_tmp_1").collect()
        }
      }
      withTable("gluten_tmp_2") {
        sql("CREATE TABLE gluten_tmp_2 (c1 int) USING PARQUET PARTITIONED BY (c2 string)")
        sql("SELECT * FROM gluten_tmp_2").collect()
      }
    }
  }

  test("Kyuubi Extension fast fail with Expression if over un-supported operator threshold") {
    withSQLConf(KyuubiSQLConf.GLUTEN_FALLBACK_OPERATOR_THRESHOLD.key -> "1") {
      assertThrows[TooMuchGlutenUnsupportedOperationException] {
        sql("SELECT rand(100)").collect()
      }

      spark.udf.register("str_len", (s: String) => s.length)
      assertThrows[TooMuchGlutenUnsupportedOperationException] {
        sql("SELECT str_len('123')").collect()
      }
    }
  }
}
