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

import java.nio.file.Path
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.FileExpirationUtils

class TempFileManager(name: String) extends AbstractService(name) {
  def this() = this(classOf[TempFileManager].getSimpleName)

  private lazy val fileCounter = new AtomicLong(0)
  final private var expiringFiles: Cache[String, Path] = _
  private lazy val cleanupScheduler = Executors.newScheduledThreadPool(1)

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
    val interval = conf.get(KyuubiConf.SERVER_STALE_FILE_EXPIRATION_INTERVAL)
    expiringFiles = CacheBuilder.newBuilder()
      .expireAfterWrite(interval, TimeUnit.MILLISECONDS)
      .removalListener((notification: RemovalNotification[String, Path]) => {
        val path = notification.getValue
        FileExpirationUtils.removeFromFileList(path.toString)
        Utils.deleteDirectoryRecursively(path.toFile)
      })
      .build[String, Path]()

    cleanupScheduler.scheduleAtFixedRate(
      () => expiringFiles.cleanUp(),
      0,
      Math.max(interval / 10, 100),
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    expiringFiles.invalidateAll()
    super.stop()
  }

  /**
   * add the file path to the expiration list
   * ensuring the path will be deleted
   * either after duration
   * or on the JVM exit
   * @param path the path of file or directory
   */
  def addDirToExpiration(path: Path): Unit = {
    if (path != null) {
      expiringFiles.put(s"${fileCounter.incrementAndGet()}-${System.currentTimeMillis()}", path)
      FileExpirationUtils.deleteFileOnExit(path)
    }
  }
}
