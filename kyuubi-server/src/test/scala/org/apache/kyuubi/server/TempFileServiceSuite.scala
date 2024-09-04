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

package org.apache.kyuubi.server

import java.io.ByteArrayInputStream
import java.time.Duration

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.Utils.writeToTempFile
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{SERVER_TEMP_FILE_EXPIRE_MAX_COUNT, SERVER_TEMP_FILE_EXPIRE_TIME}
import org.apache.kyuubi.session.KyuubiSessionManager

class TempFileServiceSuite extends WithKyuubiServer {
  private val expirationInMs = 100
  private val tempFilesMaxCount = 3

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(SERVER_TEMP_FILE_EXPIRE_TIME, Duration.ofMillis(expirationInMs).toMillis)
    .set(SERVER_TEMP_FILE_EXPIRE_MAX_COUNT, tempFilesMaxCount)

  test("file cleaned up after expiration") {
    val tempFileService =
      server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager].tempFileService
    (0 until 3).map { i =>
      val dir = Utils.createTempDir()
      writeToTempFile(new ByteArrayInputStream(s"$i".getBytes()), dir, s"$i.txt")
      dir.toFile
    }.map { dirFile =>
      assert(dirFile.exists())
      tempFileService.addPathToExpiration(dirFile.toPath)
      dirFile
    }.foreach { f =>
      eventually(Timeout((expirationInMs * 2).millis)) {
        assert(!f.exists())
      }
    }
  }

  test("temp files cleaned up when exceeding max count") {
    val tempFileService =
      server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager].tempFileService
    (0 until tempFilesMaxCount * 2).map { i =>
      val dir = Utils.createTempDir()
      writeToTempFile(new ByteArrayInputStream(s"$i".getBytes()), dir, s"$i.txt")
      dir.toFile
    }.foreach { dirFile =>
      assert(dirFile.exists())
      tempFileService.addPathToExpiration(dirFile.toPath)
      dirFile
    }

    assertResult(tempFilesMaxCount)(tempFileService.remainedExpiringFilesCount())
  }
}
