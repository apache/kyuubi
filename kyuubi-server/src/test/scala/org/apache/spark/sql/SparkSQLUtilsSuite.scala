/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.SparkFunSuite

class SparkSQLUtilsSuite extends SparkFunSuite {
  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession
      .builder()
      .appName(classOf[SparkSQLUtilsSuite].getSimpleName)
      .master("local")
      .getOrCreate()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  test("initialize metastore client ahead") {
    val dbs = SparkSQLUtils.initializeMetaStoreClient(sparkSession)
    assert(dbs.contains("default"))
  }

  test("get user jar class loader") {
    sparkSession.sql("add jar udf-test.jar")
    val loader = SparkSQLUtils.getUserJarClassLoader(sparkSession)
    assert(loader.getResource("udf-test.jar") !== null)
  }
}
