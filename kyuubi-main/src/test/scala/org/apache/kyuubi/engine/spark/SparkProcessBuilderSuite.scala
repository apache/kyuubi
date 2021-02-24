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

package org.apache.kyuubi.engine.spark

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.kyuubi.{KerberizedTestHelper, KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.ServiceUtils

class SparkProcessBuilderSuite extends KerberizedTestHelper {
  private val conf = KyuubiConf()
    .set(EMBEDDED_ZK_PORT, 5555)
    .set(EMBEDDED_ZK_TEMP_DIR, "spark_process_test")
    .set("kyuubi.on", "off")
    .toSparkPrefixedConf

  test("spark process builder") {
    val builder = new SparkProcessBuilder("kentyao", conf)
    val commands = builder.toString.split(' ')
    assert(commands(2) === "org.apache.kyuubi.engine.spark.SparkSQLEngine")
    assert(commands.contains("spark.kyuubi.on=off"))
    builder.toString.contains("\\\n\t--class")
    builder.toString.contains("\\\n\t--conf spark.kyuubi.on=off")
    val pb = new ProcessBuilder(commands.head, "--help")
    assert(pb.start().waitFor() === 0)
    assert(Files.exists(Paths.get(commands.last)))

    val process = builder.start
    assert(process.isAlive)
    process.destroyForcibly()
  }

  test("capture error from spark process builder") {
    val processBuilder = new SparkProcessBuilder("kentyao", conf ++ Map("spark.ui.port" -> "abc"))
    val proc = processBuilder.start
    proc.waitFor(10, TimeUnit.SECONDS)
    while (proc.isAlive) {
      Thread.sleep(100)
    }
    Thread.sleep(5000)
    val error = processBuilder.getError
    assert(error.getMessage.contains(
      "java.lang.IllegalArgumentException: spark.ui.port should be int, but was abc"))
    assert(error.isInstanceOf[KyuubiSQLException])

    val processBuilder1 = new SparkProcessBuilder("kentyao",
      conf ++ Map("spark.hive.metastore.uris" -> "thrift://dummy"))
    val proc1 = processBuilder1.start
    proc1.waitFor(10, TimeUnit.SECONDS)
    while (proc1.isAlive) {
      Thread.sleep(100)
    }
    Thread.sleep(5000)
    val error1 = processBuilder1.getError
    assert(
      error1.getMessage.contains("org.apache.hadoop.hive.ql.metadata.HiveException:"))
  }

  test("proxy user or keytab") {
    val b1 = new SparkProcessBuilder("kentyao", conf)
    assert(b1.toString.contains("--proxy-user kentyao"))

    val conf1 = conf ++ Map("spark.kerberos.principal" -> testPrincipal)
    val b2 = new SparkProcessBuilder("kentyao", conf1)
    assert(b2.toString.contains("--proxy-user kentyao"))

    val conf2 = conf ++ Map("spark.kerberos.keytab" -> testKeytab)
    val b3 = new SparkProcessBuilder("kentyao", conf2)
    assert(b3.toString.contains("--proxy-user kentyao"))

    tryWithSecurityEnabled {
      val conf3 = conf ++ Map("spark.kerberos.principal" -> testPrincipal,
        "spark.kerberos.keytab" -> "testKeytab")
      val b4 = new SparkProcessBuilder(Utils.currentUser, conf3)
      assert(b4.toString.contains(s"--proxy-user ${Utils.currentUser}"))

      val conf4 = conf ++ Map("spark.kerberos.principal" -> testPrincipal,
        "spark.kerberos.keytab" -> testKeytab)
      val b5 = new SparkProcessBuilder("kentyao", conf4)
      assert(b5.toString.contains("--proxy-user kentyao"))

      val b6 = new SparkProcessBuilder(ServiceUtils.getShortName(testPrincipal), conf4)
      assert(!b6.toString.contains("--proxy-user kentyao"))
    }
  }

  test("log capture should release after close") {
    val process = new FakeSparkProcessBuilder
    try {
      val subProcess = process.start
      assert(!process.logCaptureThread.isInterrupted)
      subProcess.waitFor(3, TimeUnit.SECONDS)
    } finally {
      process.close()
    }
    assert(process.logCaptureThread.isInterrupted)
  }

  test(s"sub process log should be overwritten") {
    def atomicTest(): Unit = {
      val pool = Executors.newFixedThreadPool(3)
      val fakeWorkDir = Files.createTempDirectory("fake")
      val dir = fakeWorkDir.toFile
      try {
        assert(dir.list().length == 0)

        val longTimeFile = new File(dir, "kyuubi-spark-sql-engine.log.1")
        longTimeFile.createNewFile()
        longTimeFile.setLastModified(System.currentTimeMillis() - 3600000)

        val shortTimeFile = new File(dir, "kyuubi-spark-sql-engine.log.0")
        shortTimeFile.createNewFile()

        val config = Map(s"spark.${SESSION_SUBMIT_LOG_RETAIN_MILLIS.key}" -> "20000")
        (1 to 10).foreach { _ =>
          pool.execute(new Runnable {
            override def run(): Unit = {
              val pb = new FakeSparkProcessBuilder(config) {
                override val workingDir: Path = fakeWorkDir
              }
              try {
                val p = pb.start
                p.waitFor()
              } finally {
                pb.close()
              }
            }
          })
        }
        pool.shutdown()
        while (!pool.isTerminated) {
          Thread.sleep(100)
        }
        assert(dir.listFiles().length == 10 + 1)
      } finally {
        dir.listFiles().foreach(_.delete())
        dir.delete()
      }
    }

    // this loop is to promise we have a good test
    (1 to 10).foreach { _ =>
      atomicTest()
    }
  }
}

class FakeSparkProcessBuilder(config: Map[String, String] = Map.empty)
  extends SparkProcessBuilder("fake", config) {
  override protected def commands: Array[String] = Array("ls")
}
