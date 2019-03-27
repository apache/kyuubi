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

import com.google.common.io.Files
import java.io.{File, FileOutputStream, IOException}
import java.net.URI
import java.util.zip.{ZipFile, ZipOutputStream}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.mockito.Mockito.{doNothing, when}
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.KYUUBI_JAR_NAME
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiYarnClientSuite extends SparkFunSuite with Matchers with MockitoSugar {

  private val yarnDefCP = YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.toSeq
  private val mrDefCP = MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH.split(",").toSeq

  test("get default yarn application classpath") {
    KyuubiYarnClient.getDefaultYarnApplicationClasspath should be(yarnDefCP)
  }

  test("get default mr application classpath") {
    KyuubiYarnClient.getDefaultMRApplicationClasspath should be(mrDefCP)
  }

  test("populate hadoop classpath with yarn cp defined") {
    val conf = Map(YarnConfiguration.YARN_APPLICATION_CLASSPATH -> "/path/to/yarn")
    withConf(conf) { c =>
      val env = mutable.HashMap[String, String]()
      KyuubiYarnClient.populateHadoopClasspath(c, env)
      classpath(env) should be(conf.values.toList ++ mrDefCP)

    }
  }

  test("populate hadoop classpath with mr cp defined") {
    val conf = Map("mapreduce.application.classpath" -> "/path/to/mr")
    withConf(conf) { c =>
      val env = mutable.HashMap[String, String]()
      KyuubiYarnClient.populateHadoopClasspath(c, env)
      classpath(env) should be(yarnDefCP ++ conf.values.toList)
    }
  }

  test("populate hadoop classpath with yarn mr cp defined") {
    val conf = Map(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH -> "/path/to/yarn",
      "mapreduce.application.classpath" -> "/path/to/mr")
    withConf(conf) { c =>
      val env = mutable.HashMap[String, String]()
      KyuubiYarnClient.populateHadoopClasspath(c, env)
      classpath(env) should be(conf.values.toList)
    }
  }

  /**
   * test with maven. not ide
   */
  test("populate classpath") {
    val conf = new Configuration()
    val env = mutable.HashMap[String, String]()
    KyuubiYarnClient.populateClasspath(conf, env)
    val cp = classpath(env)
    cp should contain("{{PWD}}")
    cp should contain(Environment.PWD.$$() + Path.SEPARATOR + SPARK_CONF_DIR)
    cp should contain(KyuubiYarnClient.buildPath(Environment.PWD.$$(), KYUUBI_JAR_NAME))
    cp should contain(KyuubiYarnClient.buildPath(Environment.PWD.$$(), SPARK_LIB_DIR, "*"))
    cp should contain(yarnDefCP.head)
    cp should contain(mrDefCP.head)
  }

  test("build path") {
    KyuubiYarnClient.buildPath("1", "2", "3") should be(
      "1" + Path.SEPARATOR + "2" + Path.SEPARATOR + "3")
  }

  test("add path to env") {
    val env = mutable.HashMap[String, String]()
    KyuubiYarnClient.addPathToEnvironment(env, Environment.CLASSPATH.name, "1")
    classpath(env) should be(List("1"))
    KyuubiYarnClient.addPathToEnvironment(env, Environment.CLASSPATH.name, "2")
    classpath(env) should be(List("1", "2"))
    KyuubiYarnClient.addPathToEnvironment(env, "JAVA_TEST_HOME", "/path/to/java")
    env("JAVA_TEST_HOME") should be("/path/to/java")
  }

  test("get qualified local path") {
    val conf = new Configuration()
    val uri1 = KyuubiSparkUtil.resolveURI("1")
    val path1 = KyuubiYarnClient.getQualifiedLocalPath(uri1, conf)
    path1.toUri should be(uri1)
    val uri2 = new URI("1")
    val path2 = KyuubiYarnClient.getQualifiedLocalPath(uri2, conf)
    path2.toUri should not be uri2
    path2.toUri should be(uri1)

    val uri3 = KyuubiSparkUtil.resolveURI("hdfs://1")
    val path3 = KyuubiYarnClient.getQualifiedLocalPath(uri3, conf)
    path3.toUri should be(uri3)
  }

  test("kyuubi yarn client init") {
    val conf = new SparkConf()
    val client = new KyuubiYarnClient(conf)
    assert(ReflectUtils.getFieldValue(client,
      "yaooqinn$kyuubi$yarn$KyuubiYarnClient$$hadoopConf").isInstanceOf[YarnConfiguration])

    assert(!ReflectUtils.getFieldValue(client, "loginFromKeytab").asInstanceOf[Boolean])

    conf.set(KyuubiSparkUtil.KEYTAB, "kyuubi.keytab").set(KyuubiSparkUtil.PRINCIPAL, "kyuubi")
    val client2 = new KyuubiYarnClient(conf)
    assert(ReflectUtils.getFieldValue(client2, "loginFromKeytab").asInstanceOf[Boolean])
  }

  test("submit with exceeded memory") {
    withAppBase { (c, kc, appRes) =>
      val resource = mock[Resource]
      when(appRes.getMaximumResourceCapability).thenReturn(resource)
      when(resource.getMemory).thenReturn(10)
      val appId = mock[ApplicationId]
      when(appRes.getApplicationId).thenReturn(appId)
      when(appId.toString).thenReturn("appId1")
      when(c.getApplicationReport(appId)).thenThrow(classOf[IOException])
      kc.submit()
      ReflectUtils.getFieldValue(kc, "yaooqinn$kyuubi$yarn$KyuubiYarnClient$$memory") should be(9)
      ReflectUtils.getFieldValue(kc,
        "yaooqinn$kyuubi$yarn$KyuubiYarnClient$$memoryOverhead") should be(1)
    }
  }

  test("submit with suitable memory") {
    withAppBase { (c, kc, appRes) =>
      val resource = mock[Resource]
      when(appRes.getMaximumResourceCapability).thenReturn(resource)
      when(resource.getMemory).thenReturn(2048 *1024)
      val appId = mock[ApplicationId]
      when(appRes.getApplicationId).thenReturn(appId)
      when(appId.toString).thenReturn("appId1")
      when(c.getApplicationReport(appId)).thenThrow(classOf[ApplicationNotFoundException])
      kc.submit()
      ReflectUtils.getFieldValue(kc,
        "yaooqinn$kyuubi$yarn$KyuubiYarnClient$$memory") should be(1024)
      ReflectUtils.getFieldValue(kc,
        "yaooqinn$kyuubi$yarn$KyuubiYarnClient$$memoryOverhead") should be(102)
    }
  }

  test("submit accepted") {
    withAppReport { (kc, report) =>
      when(report.getYarnApplicationState).thenReturn(YarnApplicationState.ACCEPTED)
      val currentTime = System.currentTimeMillis()
      when(report.getStartTime).thenReturn(currentTime).thenReturn(currentTime - 100 *1000L)
      kc.submit()
    }
  }

  test("submit running") {
    withAppReport { (kc, report) =>
      when(report.getYarnApplicationState).thenReturn(YarnApplicationState.RUNNING)
      kc.submit()
    }
  }

  test("submit new") {
    withAppReport { (kc, report) =>
      when(report.getYarnApplicationState).thenReturn(YarnApplicationState.NEW)
      val currentTime = System.currentTimeMillis()
      when(report.getStartTime).thenReturn(currentTime).thenReturn(currentTime - 100 *1000L)
      kc.submit()
    }
  }

  test("submit submitted") {
    withAppReport { (kc, report) =>
      when(report.getYarnApplicationState).thenReturn(YarnApplicationState.SUBMITTED)
      val currentTime = System.currentTimeMillis()
      when(report.getStartTime).thenReturn(currentTime).thenReturn(currentTime - 100 *1000L)
      kc.submit()
    }
  }

  test("submit killed") {
    withAppReport { (kc, report) =>
      when(report.getYarnApplicationState).thenReturn(YarnApplicationState.KILLED)
      kc.submit()
    }
  }

  test("submit finished") {
    withAppReport { (kc, report) =>
      when(report.getYarnApplicationState).thenReturn(YarnApplicationState.FINISHED)
      kc.submit()
    }
  }

  test("submit new saving") {
    withAppReport { (kc, report) =>
      when(report.getYarnApplicationState).thenReturn(YarnApplicationState.NEW_SAVING)
      val currentTime = System.currentTimeMillis()
      when(report.getStartTime).thenReturn(currentTime).thenReturn(currentTime - 100 *1000L)
      kc.submit()
    }
  }

  test("test prepare local resources") {
    withTempDir { dir =>
      withTempJarsDir(dir) { (_, jarName, dirJarName) =>
        val conf = new SparkConf()
        val kyc = new KyuubiYarnClient(conf)
        val stagingDir = new Path(dir.getAbsolutePath, "appStaing")
        ReflectUtils.setFieldValue(kyc,
          "yaooqinn$kyuubi$yarn$KyuubiYarnClient$$appStagingDir", stagingDir)
        val resources = ReflectUtils.invokeMethod(kyc, "prepareLocalResources")
          .asInstanceOf[mutable.HashMap[String, LocalResource]]
        val keys = resources.keySet.toIterator
        assert(keys.next() === "__spark_libs__")
        assert(keys.next() === "__spark_conf__")
        assert(keys.next().startsWith("kyuubi-server"))
        assert(!keys.hasNext)
        val libResource = resources.get("__spark_libs__").get
        val libEntries = new ZipFile(libResource.getResource.getFile).entries()
        assert(libEntries.nextElement().getName === jarName)
        assert(libEntries.nextElement().getName === dirJarName)
        assert(!libEntries.hasMoreElements)
      }
    }
  }

  def withTempDir(f: File => Unit): Unit = {
    val path = KyuubiSparkUtil.createTempDir().getCanonicalFile
    val sparkHome = Option(System.getenv("SPARK_HOME"))
    setEnv("SPARK_HOME", Some(path.getAbsolutePath))
    try {
      f(path)
    } finally {
      setEnv("SPARK_HOME", sparkHome)
      KyuubiSparkUtil.deleteRecursively(path)
    }
  }

  def withTempJarsDir(path: File)(f: (File, String, String) => Unit): Unit = {
    val jarsDir = path.getAbsolutePath + File.separator + "jars"
    val libDirName = "libDir"
    new File(jarsDir).mkdir()
    new File(jarsDir + File.separator + libDirName).mkdir()
    val basePath = jarsDir + File.separator
    val jarName = "KYUUBI_JAR.jar"
    val dirJarName = libDirName + File.separator + "dirJar.jar"
    Files.write("JAR".getBytes(), new File(basePath + jarName))
    Files.write("DIRJAR".getBytes(), new File(basePath + dirJarName))
    val invalidJar = new File(basePath + "invalidJar.jar")
    val invalidDirJar = new File(basePath + libDirName + File.separator + "invalidDirJar.jar")
    Files.write("INVALID".getBytes(), invalidJar)
    Files.write("INVALID".getBytes(), invalidDirJar)
    invalidJar.setReadable(false)
    invalidDirJar.setReadable(false)
    System.setProperty("KYUUBI_JAR", basePath + jarName)
    ReflectUtils.setFieldValue(KyuubiSparkUtil, "SPARK_JARS_DIR", jarsDir)
    try f(new File(jarsDir), jarName, dirJarName) finally System.clearProperty("KYUUBI_JAR")
  }

  def setEnv(key: String, value: Option[String]): Unit = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv())
      .asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    value match {
      case Some(v) =>
        map.put(key, v)
      case _ =>
        map.remove(key)
    }
  }

  def withConf(map: Map[String, String] = Map.empty)(testCode: Configuration => Any): Unit = {
    val conf = new Configuration()
    map.foreach { case (k, v) => conf.set(k, v) }
    testCode(conf)
  }

  def withClientBase(f: (YarnClient, KyuubiYarnClient) => Any): Unit = {
    val conf = new SparkConf()
    val client = new KyuubiYarnClient(conf)
    val yarnClient = mock[YarnClient]
    ReflectUtils.setFieldValue(client,
      "yaooqinn$kyuubi$yarn$KyuubiYarnClient$$yarnClient", yarnClient)
    doNothing().when(yarnClient).start()
    val kyuubiJar = this.getClass.getClassLoader.getResource(KYUUBI_JAR_NAME).getPath
    ReflectUtils.setFieldValue(KyuubiSparkUtil,
      "SPARK_JARS_DIR", kyuubiJar.stripSuffix(KYUUBI_JAR_NAME))
    f(yarnClient, client)
  }

  def withAppBase(f: (YarnClient, KyuubiYarnClient, GetNewApplicationResponse) => Any): Unit = {
    withClientBase { (c, kc) =>
      val app = mock[YarnClientApplication]
      when(c.createApplication()).thenReturn(app)
      val appRes = mock[GetNewApplicationResponse]
      when(app.getNewApplicationResponse).thenReturn(appRes)
      val appContext = mock[ApplicationSubmissionContext]
      when(app.getApplicationSubmissionContext).thenReturn(appContext)
      f(c, kc, appRes)
    }
  }

  def withAppReport(f: (KyuubiYarnClient, ApplicationReport) => Any): Unit = {
    withAppBase { (c, kc, appRes) =>
      val resource = mock[Resource]
      when(appRes.getMaximumResourceCapability).thenReturn(resource)
      when(resource.getMemory).thenReturn(2048 *1024)
      val appId = mock[ApplicationId]
      when(appRes.getApplicationId).thenReturn(appId)
      when(appId.toString).thenReturn("appId1")
      val report = mock[ApplicationReport]
      when(c.getApplicationReport(appId)).thenReturn(report)
      val token = mock[Token]
      when(report.getClientToAMToken).thenReturn(token)
      when(token.toString).thenReturn("")
      when(report.getHost).thenReturn("")
      when(report.getRpcPort).thenReturn(1)
      when(report.getQueue).thenReturn("default")
      when(report.getStartTime).thenReturn(0)
      when(report.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)
      when(report.getTrackingUrl).thenReturn("")
      when(report.getUser).thenReturn(KyuubiSparkUtil.getCurrentUserName)
      f(kc, report)
    }
  }

  def classpath(env: mutable.HashMap[String, String]): Array[String] =
    env(Environment.CLASSPATH.name).split(":|;|<CPS>")
}
