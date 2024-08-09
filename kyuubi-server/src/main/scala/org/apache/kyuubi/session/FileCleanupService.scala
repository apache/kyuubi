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

package org.apache.kyuubi.session

import java.lang.{Long => JLong}
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.FileExpirationUtils

class FileCleanupService extends AbstractService("fileCleanup") with Logging {
  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
  }

  private lazy val expiringFileCounter = new AtomicLong(0)

  private lazy val fileCleanupCache: Cache[JLong, Path] = {
    val expirationInterval = conf.get(KyuubiConf.SERVER_FILE_PERIODIC_CLEANUP_INTERVAL)
    val cache = CacheBuilder.newBuilder()
      .expireAfterWrite(expirationInterval, TimeUnit.MILLISECONDS)
      .removalListener((notification: RemovalNotification[JLong, Path]) => {
        val path = notification.getValue
        FileExpirationUtils.removeFromFileList(path.toString)
        Utils.deleteDirectoryRecursively(path.toFile)
      }).build[JLong, Path]()
    cache
  }

  def addDirToExpiration(path: Path): Unit = {
    if (path != null) {
      fileCleanupCache.put(expiringFileCounter.incrementAndGet(), path)
      FileExpirationUtils.deleteFileOnExit(path)
    }
  }
}
