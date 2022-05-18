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

package org.apache.kyuubi.plugin.spark.authz.ranger

// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.ObjectType._

class AccessResourceSuite extends AnyFunSuite {
// scalastyle:on
  test("generate spark ranger resources") {
    val resource = AccessResource(DATABASE, "my_db_name")
    assert(resource.getDatabase === "my_db_name")
    assert(resource.getTable === null)
    assert(resource.getColumn === null)
    assert(resource.getColumns.isEmpty)

    val resource1 = AccessResource(DATABASE, null, "my_table_name", "my_col_1,my_col_2")
    assert(resource1.getDatabase === null)
    assert(resource1.getTable === null)
    assert(resource1.getColumn === null)
    assert(resource1.getColumns.isEmpty)

    val resource2 = AccessResource(FUNCTION, "my_db_name", "my_func_name", null)
    assert(resource2.getDatabase === "my_db_name")
    assert(resource2.getTable === null)
    assert(resource2.getValue("udf") === "my_func_name")
    assert(resource1.getColumn === null)
    assert(resource1.getColumns.isEmpty)

    val resource3 = AccessResource(TABLE, "my_db_name", "my_table_name", "my_col_1,my_col_2")
    assert(resource3.getDatabase === "my_db_name")
    assert(resource3.getTable === "my_table_name")
    assert(resource3.getColumn === null)
    assert(resource3.getColumns.isEmpty)

    val resource4 = AccessResource(COLUMN, "my_db_name", "my_table_name", "my_col_1,my_col_2")
    assert(resource4.getDatabase === "my_db_name")
    assert(resource4.getTable === "my_table_name")
    assert(resource4.getColumn === "my_col_1,my_col_2")
    assert(resource4.getColumns === Seq("my_col_1", "my_col_2"))
  }
}
