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

package org.apache.spark

import java.lang.reflect.{InvocationTargetException, UndeclaredThrowableException}
import java.net.URL
import java.security.PrivilegedExceptionAction

import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.util.{ChildFirstURLClassLoader, SignalUtils}

import yaooqinn.kyuubi.{Logging, SPARK_COMPILE_VERSION}
import yaooqinn.kyuubi.service.ServiceException
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiSparkUtilSuite extends SparkFunSuite with Logging {

  test("get current user name") {
    val user = KyuubiSparkUtil.getCurrentUserName
    assert(user === System.getProperty("user.name"))
  }

  test("get user with impersonation") {
    val currentUser = UserGroupInformation.getCurrentUser
    val user1 = KyuubiSparkUtil.getCurrentUserName
    assert(user1 === currentUser.getShortUserName)
    val remoteUser = UserGroupInformation.createRemoteUser("test")
    remoteUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        val user2 = KyuubiSparkUtil.getCurrentUserName
        assert(user2 === remoteUser.getShortUserName)
      }
    })
  }

  test("Spark version test") {
    assert(SPARK_VERSION === SPARK_COMPILE_VERSION)
  }

  test("testLocalHostName") {
    val hostname = KyuubiSparkUtil.localHostName()
    assert("localhost" === hostname)
  }

  test("test HIVE_VAR_PREFIX") {
    val conf = "spark.foo"
    val hiveConf = "set:hivevar:" + conf
    hiveConf match {
      case KyuubiSparkUtil.HIVE_VAR_PREFIX(c) =>
        assert(c === conf)
    }
  }

  test("major version") {
    assert(KyuubiSparkUtil.majorVersion("2.1") === 2)
    intercept[IllegalArgumentException](KyuubiSparkUtil.majorVersion(".2"))
    intercept[IllegalArgumentException](KyuubiSparkUtil.majorVersion("-2"))
    intercept[IllegalArgumentException](KyuubiSparkUtil.majorVersion("x-2"))
    assert(KyuubiSparkUtil.majorVersion("2.1.2") === 2)
    assert(KyuubiSparkUtil.majorVersion("2.1.2-SNAPSHOT") === 2)
  }

  test("testMinorVersion") {
    assert(KyuubiSparkUtil.minorVersion("2.1.2-SNAPSHOT") === 1)
    assert(KyuubiSparkUtil.minorVersion("2.3") === 3)
  }

  test("testDEPLOY_MODE_DEFAULT") {
    assert(KyuubiSparkUtil.DEPLOY_MODE=== "spark.submit.deployMode")
    assert(KyuubiSparkUtil.DEPLOY_MODE_DEFAULT === "client")
  }

  test("testPRINCIPAL") {
    assert(KyuubiSparkUtil.PRINCIPAL === "spark.yarn.principal")
  }

  test("testKEYTAB") {
    assert(KyuubiSparkUtil.KEYTAB === "spark.yarn.keytab")
  }

  test("testNewConfiguration") {
    val conf = new SparkConf(loadDefaults = true).setMaster("local").setAppName("test")
    conf.set("spark.hadoop.foo", "bar")
    assert(KyuubiSparkUtil.newConfiguration(conf).get("foo") === "bar")
  }

  test("testGetJobGroupIDKey") {
    assert(KyuubiSparkUtil.getJobGroupIDKey === "spark.jobGroup.id")
  }

  test("testMULTIPLE_CONTEXTS_DEFAULT") {
    assert(KyuubiSparkUtil.MULTIPLE_CONTEXTS === "spark.driver.allowMultipleContexts")
    assert(KyuubiSparkUtil.MULTIPLE_CONTEXTS_DEFAULT === "true")
  }

  test("testCreateTempDir") {
    val tmpDir = KyuubiSparkUtil.createTempDir(namePrefix = "test_kyuubi")
    val tmpDir2 = KyuubiSparkUtil.createTempDir()
    assert(tmpDir.exists())
    assert(tmpDir.isDirectory)
    assert(tmpDir2.exists())
    assert(tmpDir2.isDirectory)
  }

  test("testExceptionString") {
    val e1: Throwable = null
    assert(KyuubiSparkUtil.exceptionString(e1) === "")
    val msg = "test exception"
    val e2 = new ServiceException(msg, e1)
    assert(KyuubiSparkUtil.exceptionString(e2).contains(msg))
  }

  test("testMETASTORE_JARS") {
    assert(KyuubiSparkUtil.METASTORE_JARS === "spark.sql.hive.metastore.jars")
  }

  test("testDRIVER_BIND_ADDR") {
    assert(KyuubiSparkUtil.DRIVER_BIND_ADDR === "spark.driver.bindAddress")

  }

  test("testCATALOG_IMPL") {
    assert(KyuubiSparkUtil.CATALOG_IMPL === "spark.sql.catalogImplementation")
    assert(KyuubiSparkUtil.CATALOG_IMPL_DEFAULT === "hive")

  }

  test("testSPARK_HADOOP_PREFIX") {
    assert(KyuubiSparkUtil.SPARK_HADOOP_PREFIX === "spark.hadoop.")
  }

  test("testSPARK_UI_PORT") {
    assert(KyuubiSparkUtil.SPARK_UI_PORT === "spark.ui.port")
    assert(KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT === "0")

  }

  test("testSPARK_PREFIX") {
    assert(KyuubiSparkUtil.SPARK_PREFIX === "spark.")
  }

  test("testIsSparkVersionOrHigher") {
    assert(KyuubiSparkUtil.equalOrHigherThan("1.6.3"))
    assert(KyuubiSparkUtil.equalOrHigherThan("2.0.2"))
    assert(KyuubiSparkUtil.equalOrHigherThan(SPARK_COMPILE_VERSION))
    assert(!KyuubiSparkUtil.equalOrHigherThan("2.4.1"))
    assert(!KyuubiSparkUtil.equalOrHigherThan("3.0.0"))
  }

  test("testTimeStringAsMs") {
    assert(KyuubiSparkUtil.timeStringAsMs("-1") === -1)
    assert(KyuubiSparkUtil.timeStringAsMs("50s") === 50000L)
    assert(KyuubiSparkUtil.timeStringAsMs("50min") === 50 * 60 * 1000L)
    assert(KyuubiSparkUtil.timeStringAsMs("100ms") === 100L)
  }

  test("testGetContextClassLoader") {
    val origin = Thread.currentThread().getContextClassLoader
    try {
      assert(KyuubiSparkUtil.getContextOrSparkClassLoader === origin)

      val classloader = new URLClassLoader(Seq.empty[URL], origin)
      Thread.currentThread().setContextClassLoader(classloader)
      assert(KyuubiSparkUtil.getContextOrSparkClassLoader === classloader)
    } finally {
      Thread.currentThread().setContextClassLoader(origin)
    }
  }

  test("testInitDaemon") {
    KyuubiSparkUtil.initDaemon(logger)
    assert(ReflectUtils.getFieldValue(SignalUtils, "loggerRegistered") === true)
  }

  test("testAddShutdownHook") {
    val x = 1
    var y: Int = 0
    def f(): Int = {
      y = x * 2
      y
    }
    KyuubiSparkUtil.addShutdownHook(f())
    assert(y === 0)
  }

  test("testHDFS_CLIENT_CACHE") {
    assert(KyuubiSparkUtil.HDFS_CLIENT_CACHE === "spark.hadoop.fs.hdfs.impl.disable.cache")
    assert(KyuubiSparkUtil.HDFS_CLIENT_CACHE_DEFAULT.toBoolean)
    assert(KyuubiSparkUtil.FILE_CLIENT_CACHE === "spark.hadoop.fs.file.impl.disable.cache")
    assert(KyuubiSparkUtil.FILE_CLIENT_CACHE_DEFAULT.toBoolean)
  }

  test("resolve uri") {
    val path1 = "test"
    val resolvedPath1 = KyuubiSparkUtil.resolveURI(path1)
    assert(resolvedPath1.getScheme === "file")
    val path2 = "hdfs://cluster-test/user/kent/"
    val resolvedPath2 = KyuubiSparkUtil.resolveURI(path2)
    assert(resolvedPath2.getScheme === "hdfs")
  }

  test("get local dir") {
    val conf = new SparkConf()
    val dir1 = KyuubiSparkUtil.getLocalDir(conf)
    assert(dir1.contains(System.getProperty("java.io.tmpdir")))
    conf.set(KyuubiSparkUtil.SPARK_LOCAL_DIR, "test")
    val dir2 = KyuubiSparkUtil.getLocalDir(conf)
    assert(dir2 === dir1)
  }

  test("split command string") {
    val cmd = "-XX:PermSize=1024m -XX:MaxPermSize=1024m  -XX:MaxDirectMemorySize=4096m" +
      " -XX:+HeapDumpOnOutOfMemoryError -XX:OnOutOfMemoryError=\"kill -9 %p\"" +
      " -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution" +
      " -Xloggc:/home/hadoop/logs/kyuubi-server.gc -XX:+UseGCLogFileRotation" +
      " -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=256M" +
      " -Djava.security.krb5.conf=/home/hadoop/krb5/krb5.conf"
    val cmds = KyuubiSparkUtil.splitCommandString(cmd)
    assert(cmds.size === 14)
    assert(cmds.contains("-XX:OnOutOfMemoryError=kill -9 %p"))
  }

  test("substitute app id") {
    val symbol = "{{APP_ID}}"
    val cmd = "bin/java -server Main " + symbol
    val id1 = "application_id_1"
    val substituted = KyuubiSparkUtil.substituteAppId(cmd, id1)
    assert(!substituted.contains(symbol))
    assert(substituted.contains(id1))
  }

  test("escape for shell") {
    val cmd = "bash -c \"command arg1 arg2\""
    val escaped = KyuubiSparkUtil.escapeForShell(cmd)
    assert(escaped === "'bash -c \\\"command arg1 arg2\\\"'")
    val cmd2 = "bash -c \"command $1 arg1 arg2\""
    val escaped2 = KyuubiSparkUtil.escapeForShell(cmd2)
    assert(escaped2 === "'bash -c \\\"command \\$1 arg1 arg2\\\"'")
    val cmd3 = "bash -c \"command \' arg1 arg2\""
    val escaped3 = KyuubiSparkUtil.escapeForShell(cmd3)
    assert(escaped3 === "'bash -c \\\"command '\\'' arg1 arg2\\\"'")
  }

  test("get properties from file") {
    val file = "kyuubi-test.conf"
    val url = KyuubiSparkUtil.getContextOrSparkClassLoader.getResource(file)
    val path = url.getPath
    val props = KyuubiSparkUtil.getPropertiesFromFile(path)
    assert(props.get("spark.kyuubi.test") === Some("1"))
  }

  test("get and set kyuubi first classloader") {
    val loader = KyuubiSparkUtil.getAndSetKyuubiFirstClassLoader
    assert(loader === Thread.currentThread().getContextClassLoader)
    assert(loader.getParent === null)
    assert(loader.getURLs().length === 1)
    assert(loader.isInstanceOf[ChildFirstURLClassLoader])
    assert(loader.getURLs()(0) ===
      KyuubiSparkUtil.getClass.getProtectionDomain.getCodeSource.getLocation)
    assert(loader.loadClass(classOf[SparkEnv].getName).getClassLoader === loader)
    assert(loader.loadClass(classOf[SparkContext].getName).getClassLoader !== loader)
  }

  test("test setup common kyuubi config") {
    val conf = new SparkConf(true).set(KyuubiSparkUtil.METASTORE_JARS, "maven")
    KyuubiSparkUtil.setupCommonConfig(conf)
    val name = "spark.app.name"
    assert(conf.get(name) === "Kyuubi Server")
    assert(conf.get(KyuubiSparkUtil.SPARK_UI_PORT) === KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.MULTIPLE_CONTEXTS) ===
      KyuubiSparkUtil.MULTIPLE_CONTEXTS_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.CATALOG_IMPL) === KyuubiSparkUtil.CATALOG_IMPL_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.DEPLOY_MODE) === KyuubiSparkUtil.DEPLOY_MODE_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.METASTORE_JARS) === "builtin")
    assert(conf.get(KyuubiSparkUtil.SPARK_UI_PORT) === KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.SPARK_LOCAL_DIR)
      .startsWith(System.getProperty("java.io.tmpdir")))
    assert(!conf.contains(KyuubiSparkUtil.HDFS_CLIENT_CACHE))
    val foo = "spark.foo"
    val e = intercept[NoSuchElementException](conf.get(foo))
    assert(e.getMessage === foo)

    val bar = "bar"
    val conf2 = new SparkConf(loadDefaults = true)
      .set(name, "test")
      .set(foo, bar)
      .set(KyuubiSparkUtil.SPARK_UI_PORT, "1234")
    KyuubiSparkUtil.setupCommonConfig(conf2)
    assert(conf.get(name) === "Kyuubi Server") // app name will be overwritten
    assert(conf2.get(KyuubiSparkUtil.SPARK_UI_PORT) === KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    assert(conf2.get(foo) === bar)
    assert(conf2.getOption(KyuubiSparkUtil.METASTORE_JARS).isEmpty)

    val conf3 = new SparkConf(loadDefaults = true).set(KyuubiSparkUtil.METASTORE_JARS, "builtin")
    KyuubiSparkUtil.setupCommonConfig(conf3)
    assert(conf.get(KyuubiSparkUtil.METASTORE_JARS) === "builtin")
  }

  test("find cause") {
    val msg = "message"
    val e0 = new Exception(msg)
    val e1 = new UndeclaredThrowableException(null)
    val e2 = KyuubiSparkUtil.findCause(e1)
    assert(e1 === e2)

    val e3 = new UndeclaredThrowableException(e0)
    val e4 = KyuubiSparkUtil.findCause(e3)
    assert(e4 === e0)

    val e5 = new InvocationTargetException(null)
    val e6 = KyuubiSparkUtil.findCause(e5)
    assert(e5 === e6)

    val e7 = new InvocationTargetException(e0)
    val e8 = KyuubiSparkUtil.findCause(e7)
    assert(e8 === e0)

    val e9 = KyuubiSparkUtil.findCause(e0)
    assert(e9 === e0)
  }
}
