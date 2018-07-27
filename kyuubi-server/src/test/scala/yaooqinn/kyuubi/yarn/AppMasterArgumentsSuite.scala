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

package yaooqinn.kyuubi.yarn

import org.apache.spark.SparkFunSuite

class AppMasterArgumentsSuite extends SparkFunSuite {

  test("kyuubi application master arguments test") {
    val arg1 = null
    val e = intercept[IllegalArgumentException](AppMasterArguments(arg1))
    assert(e.getMessage.contains("Arguments for Kyuubi Application Master can not be null"))
    val arg2 = Array.empty[String]
    assert(AppMasterArguments(arg2).propertiesFile === null)

    val arg3 = Array("--properties-file")
    intercept[IllegalArgumentException](AppMasterArguments(arg3))

    val arg4 = Array("--properties-file", "kyuubi.test.properties")
    assert(AppMasterArguments(arg4).propertiesFile === Some("kyuubi.test.properties"))

    val arg5 = Array("--properties-file", "kyuubi.test.properties", "--arg", "arg1")
    val e2 = intercept[IllegalArgumentException](AppMasterArguments(arg5))
    assert(e2.getMessage.contains(arg5.toList.takeRight(2).toString()))
  }
}
