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
import java.nio.file.{Files, Paths}
import java.security.PrivilegedExceptionAction
import java.util.Properties

import scala.collection.mutable

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SERVER_SECRET_REDACTION_PATTERN
import org.apache.kyuubi.util.command.CommandLineUtils._

class UtilsSuite extends KyuubiFunSuite {

  test("build information check") {
    val buildFile = "kyuubi-version-info.properties"
    val str = Utils.getContextOrKyuubiClassLoader.getResourceAsStream(buildFile)
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
    assert(props.getProperty("kyuubi_flink_version") === FLINK_COMPILE_VERSION)
    assert(props.getProperty("kyuubi_trino_version") === TRINO_COMPILE_VERSION)
    assert(props.getProperty("branch") === BRANCH)
    assert(props.getProperty("revision") === REVISION)
    assert(props.getProperty("revision_time") === REVISION_TIME)
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
      })
  }

  test("findLocalInetAddress") {
    val address = InetAddress.getLocalHost
    if (!address.isLoopbackAddress) {
      assert(Utils.findLocalInetAddress === InetAddress.getLocalHost)
    } else {
      assert(Utils.findLocalInetAddress !== InetAddress.getLocalHost)
    }
  }

  test("getAbsolutePathFromWork") {
    val workDir = System.getenv("KYUUBI_WORK_DIR_ROOT")
    val path1 = "path1"
    assert(Utils.getAbsolutePathFromWork(path1).toAbsolutePath.toString ===
      Paths.get(workDir, path1).toAbsolutePath.toString)

    val path2 = "/tmp/path2"
    assert(Utils.getAbsolutePathFromWork(path2).toString === path2)
  }

  test("test args parser") {
    val args = Array[String]("--conf", "k1=v1", "--conf", " k2 = v2")
    val conf = new KyuubiConf()
    Utils.fromCommandLineArgs(args, conf)
    assert(conf.getOption("k1").get == "v1")
    assert(conf.getOption("k2").get == "v2")

    val args1 = Array[String]("--conf", "k1=v1", "--conf")
    val exception1 = intercept[IllegalArgumentException](Utils.fromCommandLineArgs(args1, conf))
    assert(exception1.getMessage.contains("Illegal size of arguments"))

    val args2 = Array[String]("--conf", "k1=v1", "--conf", "a")
    val exception2 = intercept[IllegalArgumentException](Utils.fromCommandLineArgs(args2, conf))
    assert(exception2.getMessage.contains("Illegal argument: a"))
  }

  test("redact sensitive information in command line args") {
    val conf = new KyuubiConf()
    conf.set(SERVER_SECRET_REDACTION_PATTERN, "(?i)secret|password".r)

    val buffer = new mutable.ListBuffer[String]()
    buffer += "main"
    buffer ++= confKeyValue("kyuubi.my.password", "sensitive_value")
    buffer ++= confKeyValue("kyuubi.regular.property1", "regular_value")
    buffer ++= confKeyValue("kyuubi.my.secret", "sensitive_value")
    buffer ++= confKeyValue("kyuubi.regular.property2", "regular_value")

    val commands = buffer

    // Redact sensitive information
    val redactedCmdArgs = Utils.redactCommandLineArgs(conf, commands)

    val expectBuffer = new mutable.ListBuffer[String]()
    expectBuffer += "main"
    expectBuffer += "--conf"
    expectBuffer += "kyuubi.my.password=" + REDACTION_REPLACEMENT_TEXT
    expectBuffer += "--conf"
    expectBuffer += "kyuubi.regular.property1=regular_value"
    expectBuffer += "--conf"
    expectBuffer += "kyuubi.my.secret=" + REDACTION_REPLACEMENT_TEXT
    expectBuffer += "--conf"
    expectBuffer += "kyuubi.regular.property2=regular_value"

    assert(expectBuffer === redactedCmdArgs)
  }

  test("redact sensitive information") {
    val secretKeys = Some("my.password".r)
    assert(Utils.redact(secretKeys, Seq(("kyuubi.my.password", "12345"))) ===
      Seq(("kyuubi.my.password", REDACTION_REPLACEMENT_TEXT)))
    assert(Utils.redact(secretKeys, Seq(("anything", "kyuubi.my.password=12345"))) ===
      Seq(("anything", REDACTION_REPLACEMENT_TEXT)))
    assert(Utils.redact(secretKeys, Seq((999, "kyuubi.my.password=12345"))) ===
      Seq((999, REDACTION_REPLACEMENT_TEXT)))
    // Do not redact when value type is not string
    assert(Utils.redact(secretKeys, Seq(("my.password", 12345))) ===
      Seq(("my.password", 12345)))
  }

  test("test isCommandAvailable") {
    assert(Utils.isCommandAvailable("java"))
    assertResult(false)(Utils.isCommandAvailable("un_exist_cmd"))
  }
}
