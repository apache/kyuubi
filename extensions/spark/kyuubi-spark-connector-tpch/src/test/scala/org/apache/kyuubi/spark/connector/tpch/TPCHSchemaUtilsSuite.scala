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

package org.apache.kyuubi.spark.connector.tpch

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.tpch.TPCHSchemaUtils.normalize

class TPCHSchemaUtilsSuite extends KyuubiFunSuite {

  test("normalize scale") {
    assert(normalize(1) === "1")
    assert(normalize(0.010000000000000000001) === "0.01")
    assert(normalize(1.000000000000000000001) === "1")
    assert(normalize(0.999999999999999999999) === "1")
    assert(normalize(9.999999999999999999999) === "10")
  }
}
