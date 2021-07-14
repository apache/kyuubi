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

package org.apache.kyuubi.tools

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.{CountDownLatch, Executors}

import org.apache.kyuubi.Logging

/*
* Spark storage shuffle data as the following structure.
*
* local-dir1/
*   blockmgr-uuid/
*     hash-sub-dir/
*       shuffle-data
*       shuffle-index
*
* local-dir2/
*   blockmgr-uuid/
*     hash-sub-dir/
*       shuffle-data
*       shuffle-index
*
* ...
*/
object KubernetesSparkBlockCleaner extends Logging {
  import KubernetesSparkBlockCleanerConstants._

  private val envMap = System.getenv()

  private val freeSpaceThreshold = envMap.getOrDefault(FREE_SPACE_THRESHOLD_KEY,
    "60").toInt
  private val fileExpiredTime = envMap.getOrDefault(FILE_EXPIRED_TIME_KEY,
    "604800").toLong * 1000
  private val scheduleInterval = envMap.getOrDefault(SCHEDULE_INTERVAL,
    "3600").toLong * 1000
  private val deepCleanFileExpiredTime = envMap.getOrDefault(DEEP_CLEAN_FILE_EXPIRED_TIME_KEY,
    "432000").toLong * 1000
  private val cacheDirs = if (envMap.containsKey(CACHE_DIRS_KEY)) {
    envMap.get(CACHE_DIRS_KEY).split(",").filter(!_.equals(""))
  } else {
    throw new IllegalArgumentException(s"the env $CACHE_DIRS_KEY must be set")
  }
  private val isTesting = envMap.getOrDefault("kyuubi.testing", "false").toBoolean
  checkConfiguration()

  /**
   * one thread clean one dir
   */
  private val threadPool = Executors.newFixedThreadPool(cacheDirs.length)

  private def checkConfiguration(): Unit = {
    require(fileExpiredTime > 0,
      s"the env $FILE_EXPIRED_TIME_KEY should be greater than 0")
    require(deepCleanFileExpiredTime > 0,
      s"the env $DEEP_CLEAN_FILE_EXPIRED_TIME_KEY should be greater than 0")
    require(scheduleInterval > 0,
      s"the env $SCHEDULE_INTERVAL should be greater than 0")
    require(freeSpaceThreshold > 0 && freeSpaceThreshold < 100,
      s"the env $FREE_SPACE_THRESHOLD_KEY should between 0 and 100")
    require(cacheDirs.nonEmpty, s"the env $CACHE_DIRS_KEY must be set")
    cacheDirs.foreach { dir =>
      val path = Paths.get(dir)
      require(Files.exists(path),
        s"the input cache dir: $dir does not exists")
      require(Files.isDirectory(path),
        s"the input cache dir: $dir should be a directory")
    }

    info(s"finish initializing configuration, " +
      s"use $CACHE_DIRS_KEY: ${cacheDirs.mkString(",")},  " +
      s"$FILE_EXPIRED_TIME_KEY: $fileExpiredTime,  " +
      s"$FREE_SPACE_THRESHOLD_KEY: $freeSpaceThreshold, " +
      s"$SCHEDULE_INTERVAL: $scheduleInterval, " +
      s"$DEEP_CLEAN_FILE_EXPIRED_TIME_KEY: $deepCleanFileExpiredTime")
  }

  private def doClean(dir: File, time: Long) {
    // clean blockManager shuffle file
    dir.listFiles.filter(_.isDirectory).filter(_.getName.startsWith("blockmgr"))
      .foreach { blockManagerDir =>
        info(s"start check blockManager dir ${blockManagerDir.getCanonicalPath}")
        // check blockManager directory
        val released = blockManagerDir.listFiles.filter(_.isDirectory).map { subDir =>
          debug(s"start check sub dir ${subDir.getCanonicalPath}")
          // check sub directory
          val subDirReleased = subDir.listFiles.map(file => checkAndDeleteFile(file, time))
          // delete empty sub directory
          checkAndDeleteFile(subDir, time, true)
          subDirReleased.sum
        }
        // delete empty blockManager directory
        checkAndDeleteFile(blockManagerDir, time, true)
        info(s"finished clean blockManager dir ${blockManagerDir.getCanonicalPath}, " +
          s"released space: ${released.sum / 1024 / 1024} MB.")
      }

    // clean spark cache file
    dir.listFiles.filter(_.isDirectory).filter(_.getName.startsWith("spark"))
      .foreach { cacheDir =>
        info(s"start check cache dir ${cacheDir.getCanonicalPath}")
        val released = cacheDir.listFiles.map(file => checkAndDeleteFile(file, time))
        // delete empty spark cache file
        checkAndDeleteFile(cacheDir, time, true)
        info(s"finished clean cache dir ${cacheDir.getCanonicalPath}, " +
          s"released space: ${released.sum / 1024 / 1024} MB.")
      }
  }

  private def checkAndDeleteFile(file: File, time: Long, isDir: Boolean = false): Long = {
    debug(s"check file ${file.getName}")
    val shouldDeleteFile = if (isDir) {
      file.listFiles.isEmpty && (System.currentTimeMillis() - file.lastModified() > time)
    } else {
      System.currentTimeMillis() - file.lastModified() > time
    }
    val length = if (isDir) 0 else file.length()
    if (shouldDeleteFile) {
      if (file.delete()) {
        debug(s"delete file ${file.getAbsolutePath} success")
        return length
      } else {
        warn(s"delete file ${file.getAbsolutePath} fail")
      }
    }
    0L
  }

  import scala.sys.process._

  private def needToDeepClean(dir: String): Boolean = {
    val used = (s"df $dir" #| s"grep $dir").!!
      .split(" ").filter(_.endsWith("%")) {
      0
    }.replace("%", "")
    info(s"$dir now used $used% space")

    used.toInt > (100 - freeSpaceThreshold)
  }

  private def doCleanJob(dir: String): Unit = {
    val startTime = System.currentTimeMillis()
    val path = Paths.get(dir)
    info(s"start clean job for $dir")
    doClean(path.toFile, fileExpiredTime)
    // re check if the disk has enough space
    if (needToDeepClean(dir)) {
      info(s"start deep clean job for $dir")
      doClean(path.toFile, deepCleanFileExpiredTime)
      if (needToDeepClean(dir)) {
        warn(s"after deep clean $dir, used space still higher than $freeSpaceThreshold")
      }
    }
    val finishedTime = System.currentTimeMillis()
    info(s"clean job $dir finished, elapsed time: ${(finishedTime - startTime) / 1000} s.")
  }

  def main(args: Array[String]): Unit = {
    do {
      info(s"start all clean job")
      val startTime = System.currentTimeMillis()
      val hasFinished = new CountDownLatch(cacheDirs.length)
      cacheDirs.foreach { dir =>
        threadPool.execute(() => {
          doCleanJob(dir)
          hasFinished.countDown()
        })
      }
      hasFinished.await()

      val usedTime = System.currentTimeMillis() - startTime
      info(s"finished to clean all dir, elapsed time $usedTime")
      if (usedTime > scheduleInterval) {
        warn(s"clean job elapsed time $usedTime which is greater than $scheduleInterval")
      } else {
        Thread.sleep(scheduleInterval - usedTime)
      }
    } while (!isTesting)
  }
}

object KubernetesSparkBlockCleanerConstants {
  val CACHE_DIRS_KEY = "CACHE_DIRS"
  val FILE_EXPIRED_TIME_KEY = "FILE_EXPIRED_TIME"
  val FREE_SPACE_THRESHOLD_KEY = "FREE_SPACE_THRESHOLD"
  val SCHEDULE_INTERVAL = "SCHEDULE_INTERVAL"
  val DEEP_CLEAN_FILE_EXPIRED_TIME_KEY = "DEEP_CLEAN_FILE_EXPIRED_TIME"
}
