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

import java.io.{File, IOException}
import java.net.InetAddress
import java.nio.file.Files
import java.security.PrivilegedExceptionAction
import java.util.Properties

import org.apache.hadoop.security.UserGroupInformation

class UtilsSuite extends KyuubiFunSuite {

  test("build information check") {
    val buildFile = "kyuubi-version-info.properties"
    val str = this.getClass.getClassLoader.getResourceAsStream(buildFile)
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

    val e = intercept[KyuubiException] {
      Utils.getPropertiesFromFile(Some(new File("invalid-file")))
    }
    assert(e.getMessage contains "Failed when loading Kyuubi properties from")
  }

  test("create directory") {
    val path = Utils.createDirectory(System.getProperty("java.io.tmpdir"))
    assert(Files.exists(path))
    assert(path.getFileName.toString.startsWith("kyuubi-"))
    path.toFile.deleteOnExit()
    val e = intercept[IOException](Utils.createDirectory("/"))
    assert(e.getMessage === "Failed to create a temp directory (under /) after 10 attempts!")
    val path1 = Utils.createDirectory(System.getProperty("java.io.tmpdir"), "kentyao")
    assert(Files.exists(path1))
    assert(path1.getFileName.toString.startsWith("kentyao-"))
    path1.toFile.deleteOnExit()
  }

  test("create tmp dir") {
    val path = Utils.createTempDir()
    assert(Files.exists(path))
    assert(path.getFileName.toString.startsWith("kyuubi-"))
  }

  test("current user") {
    UserGroupInformation.createRemoteUser("kentyao").doAs(
      new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          assert(Utils.currentUser === "kentyao")
        }
      }
    )
  }

  test("version test") {
    assert(Utils.majorVersion(KYUUBI_VERSION) ===
      Utils.majorMinorVersion(KYUUBI_VERSION)._1)
    assert(Utils.majorVersion(SPARK_COMPILE_VERSION) ===
      Utils.majorMinorVersion(SPARK_COMPILE_VERSION)._1)
    assert(Utils.majorVersion(HADOOP_COMPILE_VERSION) ===
      Utils.majorMinorVersion(HADOOP_COMPILE_VERSION)._1)
    assert(Utils.minorVersion(KYUUBI_VERSION) ===
      Utils.majorMinorVersion(KYUUBI_VERSION)._2)
    assert(Utils.shortVersion(KYUUBI_VERSION) ===
      KYUUBI_VERSION.stripSuffix("-SNAPSHOT"))
    intercept[IllegalArgumentException](Utils.shortVersion("-" + KYUUBI_VERSION))
    intercept[IllegalArgumentException](Utils.majorMinorVersion("-" + KYUUBI_VERSION))
  }

  test("findLocalInetAddress") {
    val address = InetAddress.getLocalHost
    if (!address.isLoopbackAddress) {
      assert(Utils.findLocalInetAddress === InetAddress.getLocalHost)
    } else {
      assert(Utils.findLocalInetAddress !== InetAddress.getLocalHost)
    }
  }
}
