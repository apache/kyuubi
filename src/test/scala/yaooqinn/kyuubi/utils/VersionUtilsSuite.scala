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

package yaooqinn.kyuubi.utils

import org.apache.spark.SparkFunSuite

class VersionUtilsSuite extends SparkFunSuite {

  test("Get Supported Spark Vesion") {
    val v160 = VersionUtils.getSupportedSpark("1.6.0")
    assert(v160.nonEmpty, "1.6.0 is supported")
    val v161 = VersionUtils.getSupportedSpark("1.6.1")
    assert(v161.nonEmpty, "1.6.1 is supported")
    val v162 = VersionUtils.getSupportedSpark("1.6.1")
    assert(v162.nonEmpty, "1.6.2 is supported")
    val v200 = VersionUtils.getSupportedSpark("2.0.0")
    assert(v200.nonEmpty, "2.0.0 is supported")
    val v210 = VersionUtils.getSupportedSpark("2.1.0")
    assert(v210.nonEmpty, "2.1.0 is supported")
    val v220 = VersionUtils.getSupportedSpark("2.2.0")
    assert(v220.nonEmpty, "2.2.0 is supported")
    val v230 = VersionUtils.getSupportedSpark("2.3.0")
    assert(v230.nonEmpty, "2.3.0 is supported")
    val v150 = VersionUtils.getSupportedSpark("1.5.0")
    assert(v150.isEmpty, "1.5.0 is not supported")
  }

}
