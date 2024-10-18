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

package org.apache.kyuubi.server.rest.client
import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Path, Paths}
import java.util.Base64
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.JavaConverters._

import org.apache.http.client.HttpResponseException
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{BatchTestHelper, KYUUBI_VERSION, RestClientTestHelper}
import org.apache.kyuubi.client.{BatchRestApi, KyuubiRestClient}
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.config.KyuubiReservedKeys
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.session.{KyuubiSession, SessionHandle}
import org.apache.kyuubi.util.AssertionUtils.interceptCauseContains
import org.apache.kyuubi.util.GoldenFileUtils.getCurrentModuleHome

class BatchRestApiSuite extends RestClientTestHelper with BatchTestHelper {

  override protected val otherConfigs: Map[String, String] = {
    // allow to impersonate other users with spnego authentication
    Map(
      s"hadoop.proxyuser.$clientPrincipalUser.groups" -> "*",
      s"hadoop.proxyuser.$clientPrincipalUser.hosts" -> "*")
  }

  override protected def afterEach(): Unit = {
    eventually(timeout(5.seconds), interval(200.milliseconds)) {
      assert(MetricsSystem.counterValue(
        MetricsConstants.REST_CONN_OPEN).getOrElse(0L) === 0)
    }
  }

  test("basic batch rest client") {
    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(30000)
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(basicKyuubiRestClient)

    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))

    var batch: Batch = batchRestApi.createBatch(requestObj)
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")

    // get batch by id
    batch = batchRestApi.getBatchById(batch.getId())
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")

    // get batch log
    eventually(timeout(1.minutes)) {
      val log = batchRestApi.getBatchLocalLog(batch.getId(), 0, 1)
      assert(log.getRowCount == 1)
    }

    // delete batch
    val closeResp = batchRestApi.deleteBatch(batch.getId())
    assert(closeResp.getMsg.nonEmpty)

    basicKyuubiRestClient.close()
  }

  test("basic batch rest client with uploading resource file") {
    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(30000)
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(basicKyuubiRestClient)

    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))
    val exampleJarFile = Paths.get(sparkBatchTestResource.get).toFile
    val batch: Batch = batchRestApi.createBatch(requestObj, exampleJarFile)

    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")
    basicKyuubiRestClient.close()
  }

  test("basic batch rest client with uploading resource and extra resources") {
    def preparePyModulesZip(
        srcFolderPath: Path,
        targetZipFileName: String,
        excludedFileNames: Set[String] = Set.empty[String]): String = {

      def addFolderToZip(zos: ZipOutputStream, folder: File, parentFolder: String = ""): Unit = {
        if (folder.isDirectory) {
          folder.listFiles().foreach { file =>
            val fileName = file.getName
            if (!(excludedFileNames.contains(fileName) || fileName.startsWith("."))) {
              if (file.isDirectory) {
                val folderPath =
                  if (parentFolder.isEmpty) fileName else parentFolder + "/" + fileName
                addFolderToZip(zos, file, folderPath)
              } else {
                val filePath = if (parentFolder.isEmpty) fileName else parentFolder + "/" + fileName
                zos.putNextEntry(new ZipEntry(filePath))
                zos.write(Files.readAllBytes(file.toPath))
                zos.closeEntry()
              }
            }
          }
        }
      }

      val zipFilePath = Paths.get(System.getProperty("java.io.tmpdir"), targetZipFileName).toString
      val fileOutputStream = new FileOutputStream(zipFilePath)
      val zipOutputStream = new ZipOutputStream(fileOutputStream)
      try {
        addFolderToZip(zipOutputStream, srcFolderPath.toFile)
      } finally {
        zipOutputStream.close()
        fileOutputStream.close()
      }
      zipFilePath
    }

    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(5 * 60 * 1000)
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(basicKyuubiRestClient)

    val pythonScriptsPath = s"${getCurrentModuleHome(this)}/src/test/resources/python/"
    val appScriptFileName = "app.py"
    val appScriptFile = Paths.get(pythonScriptsPath, appScriptFileName).toFile
    val modulesZipFileName = "pymodules.zip"
    val modulesZipFile = preparePyModulesZip(
      srcFolderPath = Paths.get(pythonScriptsPath),
      targetZipFileName = modulesZipFileName,
      excludedFileNames = Set(appScriptFileName))

    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))
    requestObj.setBatchType("PYSPARK")
    requestObj.setName("pyspark-test")
    requestObj.setExtraResourcesMap(Map("spark.submit.pyFiles" -> modulesZipFileName).asJava)
    val extraResources = List(modulesZipFile)
    val batch: Batch = batchRestApi.createBatch(requestObj, appScriptFile, extraResources.asJava)

    try {
      assert(batch.getKyuubiInstance === fe.connectionUrl)
      assert(batch.getBatchType === "PYSPARK")
      val batchId = batch.getId
      assert(batchId !== null)

      eventually(timeout(1.minutes), interval(1.seconds)) {
        val batch = batchRestApi.getBatchById(batchId)
        assert(batch.getState == "FINISHED")
      }

    } finally {
      Files.deleteIfExists(Paths.get(modulesZipFile))
      basicKyuubiRestClient.close()
    }
  }

  test("basic batch rest client with uploading resource and extra resources of unuploaded") {

    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(5 * 60 * 1000)
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(basicKyuubiRestClient)

    val pythonScriptsPath = s"${getCurrentModuleHome(this)}/src/test/resources/python/"
    val appScriptFileName = "app.py"
    val appScriptFile = Paths.get(pythonScriptsPath, appScriptFileName).toFile
    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))
    requestObj.setBatchType("PYSPARK")
    requestObj.setName("pyspark-test")
    requestObj.setExtraResourcesMap(Map(
      "spark.submit.pyFiles" -> "non-existed-zip.zip",
      "spark.files" -> "non-existed-jar.jar",
      "spark.some.config1" -> "",
      "spark.some.config2" -> " ").asJava)

    try {
      interceptCauseContains[KyuubiRestException, HttpResponseException] {
        batchRestApi.createBatch(requestObj, appScriptFile, List().asJava)
      }("required extra resource files [non-existed-jar.jar,non-existed-zip.zip]" +
        " are not uploaded in the multipart form data")
    } finally {
      basicKyuubiRestClient.close()
    }
  }

  test("basic batch rest client with invalid user") {
    val totalConnections =
      MetricsSystem.counterValue(MetricsConstants.REST_CONN_TOTAL).getOrElse(0L)
    val failedConnections =
      MetricsSystem.counterValue(MetricsConstants.REST_CONN_FAIL).getOrElse(0L)

    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(customUser)
        .password(customPasswd)
        .socketTimeout(30000)
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(basicKyuubiRestClient)

    val e = intercept[KyuubiRestException] {
      // get batch by id
      batchRestApi.getBatchById("1")
    }
    assert(e.getCause.toString.contains(s"Error validating LDAP user: uid=${customUser}"))

    basicKyuubiRestClient.close()

    eventually(timeout(3.seconds), interval(200.milliseconds)) {
      assert(MetricsSystem.counterValue(
        MetricsConstants.REST_CONN_TOTAL).getOrElse(0L) - totalConnections === 1)
      assert(MetricsSystem.counterValue(MetricsConstants.REST_CONN_OPEN).getOrElse(0L) === 0)
      assert(MetricsSystem.counterValue(
        MetricsConstants.REST_CONN_FAIL).getOrElse(0L) - failedConnections === 1)
    }
  }

  test("spnego batch rest client") {
    val proxyUser = "user1"
    val spnegoKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
        .spnegoHost("localhost")
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(spnegoKyuubiRestClient)
    // create batch
    val requestObj =
      newSparkBatchRequest(Map("spark.master" -> "local", "hive.server2.proxy.user" -> proxyUser))

    var batch: Batch = batchRestApi.createBatch(requestObj)
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")
    val session = server.backendService.sessionManager.getSession(
      SessionHandle.fromUUID(batch.getId)).asInstanceOf[KyuubiSession]
    assert(session.realUser === clientPrincipalUser)
    assert(session.user === proxyUser)

    // get batch by id
    batch = batchRestApi.getBatchById(batch.getId())
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")

    // get batch log
    eventually(timeout(1.minutes)) {
      val log = batchRestApi.getBatchLocalLog(batch.getId(), 0, 1)
      assert(log.getRowCount == 1)
    }

    // delete batch
    val closeResp = batchRestApi.deleteBatch(batch.getId())
    assert(closeResp.getMsg.nonEmpty)

    // list batches
    var listBatchesResp = batchRestApi.listBatches("SPARK", null, null, null, null, 0, Int.MaxValue)
    assert(listBatchesResp.getTotal > 0)

    listBatchesResp =
      batchRestApi.listBatches(
        "SPARK",
        null,
        null,
        Long.MaxValue - 1,
        Long.MaxValue,
        0,
        Int.MaxValue)
    assert(listBatchesResp.getTotal === 0)

    listBatchesResp =
      batchRestApi.listBatches("SPARK", null, null, Long.MaxValue, null, 0, Int.MaxValue)
    assert(listBatchesResp.getTotal === 0)

    listBatchesResp = batchRestApi.listBatches("SPARK", null, null, null, 1000, 0, Int.MaxValue)
    assert(listBatchesResp.getTotal === 0)

    // list batches with non-existing user
    listBatchesResp =
      batchRestApi.listBatches("SPARK", "non_existing_user", null, 0, 0, 0, Int.MaxValue)
    assert(listBatchesResp.getTotal == 0)

    // list batches with invalid batch state
    intercept[KyuubiRestException] {
      batchRestApi.listBatches("SPARK", null, "BAD_STATE", 0, 0, 0, Int.MaxValue)
    }

    spnegoKyuubiRestClient.close()
  }

  test("CUSTOM auth header generator") {
    val kyuubiRestClient = KyuubiRestClient
      .builder(baseUri.toString)
      .authHeaderGenerator(() => {
        s"BASIC ${Base64.getEncoder.encodeToString(s"$ldapUser:$ldapUserPasswd".getBytes())}"
      })
      .build()
    val batchRestApi = new BatchRestApi(kyuubiRestClient)

    batchRestApi.listBatches(null, null, null, 0, 0, 0, 1)
    batchRestApi.listBatches(null, null, null, 0, 0, 0, 1)
  }

  test("support to transfer client version when creating batch") {
    val spnegoKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
        .spnegoHost("localhost")
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(spnegoKyuubiRestClient)
    // create batch
    val requestObj =
      newSparkBatchRequest(Map("spark.master" -> "local"))

    val batch = batchRestApi.createBatch(requestObj)
    val batchSession =
      server.backendService.sessionManager.getSession(SessionHandle.fromUUID(batch.getId))
    assert(
      batchSession.conf.get(KyuubiReservedKeys.KYUUBI_CLIENT_VERSION_KEY) == Some(KYUUBI_VERSION))
  }
}
