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

class TempFileManagerSuite extends WithKyuubiServer {
  private val expirationInterval = 100

  override protected val conf: KyuubiConf = KyuubiConf().set(
    KyuubiConf.SERVER_STALE_FILE_EXPIRATION_INTERVAL,
    Duration.ofMillis(expirationInterval).toMillis)

  test("file cleaned up after expiration") {
    val tempFileManager = KyuubiServer.kyuubiServer.tempFileManager
    (0 until 3).map { i =>
      val dir = Utils.createTempDir()
      writeToTempFile(new ByteArrayInputStream(s"$i".getBytes()), dir, s"$i.txt")
      dir.toFile
    }.map { dirFile =>
      assert(dirFile.exists())
      tempFileManager.addDirToExpiration(dirFile.toPath)
      dirFile
    }.foreach { f =>
      eventually(Timeout((expirationInterval * 2).millis)) {
        assert(!f.exists())
      }
    }
  }
}
