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

package yaooqinn.kyuubi

import java.util.Properties

import org.apache.spark.SparkFunSuite

class packageSuite extends SparkFunSuite {
  test("build info") {
    val buildFile = "kyuubi-version-info.properties"
    val str = Thread.currentThread().getContextClassLoader.getResourceAsStream(buildFile)
    val props = new Properties()
    assert(str !== null)
    props.load(str)
    str.close()
    assert(props.getProperty("kyuubi_version") === KYUUBI_VERSION)
    assert(props.getProperty("kyuubi_spark_version") === SPARK_COMPILE_VERSION)
    assert(props.getProperty("branch") === BRANCH)
    assert(props.getProperty("jar") === KYUUBI_JAR_NAME)
    assert(props.getProperty("revision") === REVISION)
    assert(props.getProperty("user") === BUILD_USER)
    assert(props.getProperty("url") === REPO_URL)
  }

}
