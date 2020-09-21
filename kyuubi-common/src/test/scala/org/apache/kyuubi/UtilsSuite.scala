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

package org.apache.kyuubi

import java.io.File
import java.util.Properties

class UtilsSuite extends KyuubiFunSuite {

  test("build information check") {
    val buildFile = "kyuubi-version-info.properties"
    val str = Thread.currentThread().getContextClassLoader.getResourceAsStream(buildFile)
    val props = new Properties()
    assert(str !== null)
    props.load(str)
    str.close()
    assert(props.getProperty("kyuubi_version") === KYUUBI_VERSION)
    assert(props.getProperty("kyuubi_java_version") === JAVA_COMPILE_VERSION)
    assert(props.getProperty("kyuubi_scala_version") === SCALA_COMPILE_VERSION)
    assert(props.getProperty("kyuubi_spark_version") === SPARK_COMPILE_VERSION)
    assert(props.getProperty("kyuubi_hive_version") === HIVE_COMPILE_VERSION)
    assert(props.getProperty("kyuubi_hadoop_version") === HADOOP_COMPILE_VERSION)
    assert(props.getProperty("branch") === BRANCH)
    assert(props.getProperty("revision") === REVISION)
    assert(props.getProperty("user") === BUILD_USER)
    assert(props.getProperty("url") === REPO_URL)
    assert(props.getProperty("date") === BUILD_DATE)
  }

  test("string to seq") {
    intercept[IllegalArgumentException](Utils.strToSeq(null))
    assert(Utils.strToSeq("") === Nil)
    assert(Utils.strToSeq(",") === Nil)
    assert(Utils.strToSeq("1") === Seq("1"))
    assert(Utils.strToSeq("1,") === Seq("1"))
    assert(Utils.strToSeq("1, 2") === Seq("1", "2"))
  }

  test("get system properties") {
    val key = "kyuubi.test"
    System.setProperty(key, "true")
    val p1 = Utils.getSystemProperties
    try {
      assert(p1.get(key).exists(_.toBoolean))
    } finally {
      sys.props.remove(key)
      val p2 = Utils.getSystemProperties
      assert(!p2.get(key).exists(_.toBoolean))
    }
  }

  test("get properties from file") {
    val propsFile = new File("src/test/resources/kyuubi-defaults.conf")
    val props = Utils.getPropertiesFromFile(Option(propsFile))
    assert(props("kyuubi.yes") === "yes")
    assert(!props.contains("kyuubi.no"))
  }

  test("resolveURI") {
    def assertResolves(before: String, after: String): Unit = {
      // This should test only single paths
      assert(before.split(",").length === 1)
      def resolve(uri: String): String = Utils.resolveURI(uri).toString
      assert(resolve(before) === after)
      assert(resolve(after) === after)
      // Repeated invocations of resolveURI should yield the same result
      assert(resolve(resolve(after)) === after)
      assert(resolve(resolve(resolve(after))) === after)
    }
    assertResolves("hdfs:/root/spark.jar", "hdfs:/root/spark.jar")
    assertResolves("hdfs:///root/spark.jar#app.jar", "hdfs:///root/spark.jar#app.jar")
    assertResolves("file:/C:/path/to/file.txt", "file:/C:/path/to/file.txt")
    assertResolves("file:///C:/path/to/file.txt", "file:///C:/path/to/file.txt")
    assertResolves("file:/C:/file.txt#alias.txt", "file:/C:/file.txt#alias.txt")
    assertResolves("file:foo", "file:foo")
    assertResolves("file:foo:baby", "file:foo:baby")
  }

}
