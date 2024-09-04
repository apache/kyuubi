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

package org.apache.kyuubi.service

import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.TempFileService.tempFileCounter
import org.apache.kyuubi.util.{TempFileCleanupUtils, ThreadUtils}

class TempFileService(name: String) extends AbstractService(name) {
  def this() = this(classOf[TempFileService].getSimpleName)

  final private var expiringFiles: Cache[String, String] = _
  private lazy val cleanupScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"$name-cleanup-scheduler")

  def remainedExpiringFilesCount(): Long = expiringFiles.size()

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
    val expireTimeInMs = conf.get(KyuubiConf.SERVER_TEMP_FILE_EXPIRE_TIME)
    val maxCountOpt = conf.get(KyuubiConf.SERVER_TEMP_FILE_EXPIRE_MAX_COUNT)
    expiringFiles = {
      val builder = CacheBuilder.newBuilder()
        .expireAfterWrite(expireTimeInMs, TimeUnit.MILLISECONDS)
        .removalListener((notification: RemovalNotification[String, String]) => {
          val pathStr = notification.getValue
          debug(s"Remove expired temp file: $pathStr")
          cleanupFilePath(pathStr)
        })
      maxCountOpt.foreach(builder.maximumSize(_))
      builder.build()
    }

    cleanupScheduler.scheduleAtFixedRate(
      () => expiringFiles.cleanUp(),
      0,
      Math.max(expireTimeInMs / 10, 100),
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    expiringFiles.asMap().values().forEach(cleanupFilePath)
    super.stop()
  }

  private def cleanupFilePath(pathStr: String): Unit = {
    try {
      val path = Paths.get(pathStr)
      TempFileCleanupUtils.cancelDeleteOnExit(path)
      Utils.deleteDirectoryRecursively(path.toFile)
    } catch {
      case e: Throwable => error(s"Failed to delete file $pathStr", e)
    }
  }

  /**
   * add the file path to the expiration list
   * ensuring the path will be deleted
   * either after duration
   * or on the JVM exit
   *
   * @param path the path of file or directory
   */
  def addPathToExpiration(path: Path): Unit = {
    require(path != null)
    expiringFiles.put(
      s"${tempFileCounter.incrementAndGet()}-${System.currentTimeMillis()}",
      path.toString)
    TempFileCleanupUtils.deleteOnExit(path)
  }

}

object TempFileService {
  private lazy val tempFileCounter = new AtomicLong(0)
}
