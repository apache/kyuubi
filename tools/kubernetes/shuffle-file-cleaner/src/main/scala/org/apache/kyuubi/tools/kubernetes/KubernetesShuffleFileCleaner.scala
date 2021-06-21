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

package org.apache.kyuubi.tools.kubernetes

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.tools.kubernetes.Constants._

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
object KubernetesShuffleFileCleaner extends Logging {

  private val envMap = System.getenv()

  private val freeSpaceThreshold = envMap.getOrDefault(FREE_SPACE_THRESHOLD_KEY,
    FREE_SPACE_THRESHOLD_DEFAULT_VALUE).toInt
  private val cacheDirs = envMap.getOrDefault(CACHE_DIRS_KEY,
    CACHE_DIRS_DEFAULT_VALUE).split(",").filter(!_.equals(""))
  private val fileExpiredTime = envMap.getOrDefault(FILE_EXPIRED_TIME_KEY,
    FILE_EXPIRED_TIME_DEFAULT_VALUE).toLong
  private val sleepTime = envMap.getOrDefault(SLEEP_TIME_KEY,
    SLEEP_TIME_DEFAULT_VALUE).toLong
  private val deepCleanFileExpiredTime = envMap.getOrDefault(DEEP_CLEAN_FILE_EXPIRED_TIME_KEY,
    DEEP_CLEAN_FILE_EXPIRED_TIME_DEFAULT_VALUE).toLong

  def doClean(dir: File, time: Long) {
    info(s"start clean ${dir.getName} with fileExpiredTime ${time}")

    // clean blockManager shuffle file
    dir.listFiles.filter(_.isDirectory).filter(_.getName.startsWith("blockmgr"))
      .foreach(blockManagerDir => {

        info(s"start check blockManager dir ${blockManagerDir.getName}")
        // check blockManager directory
        blockManagerDir.listFiles.filter(_.isDirectory).foreach(subDir => {

          info(s"start check sub dir ${subDir.getName}")
          // check sub directory
          subDir.listFiles.foreach(file => checkAndDeleteFIle(file, time))
          // delete empty sub directory
          checkAndDeleteDir(subDir)
        })
        // delete empty blockManager directory
        checkAndDeleteDir(blockManagerDir)
      })

    // clean spark cache file
    dir.listFiles.filter(_.isDirectory).filter(_.getName.startsWith("spark"))
      .foreach(cacheDir => {
        info(s"start check cache dir ${cacheDir.getName}")
        cacheDir.listFiles.foreach(file => checkAndDeleteFIle(file, time))
      })
  }

  def checkAndDeleteFIle(file: File, time: Long): Unit = {
    info(s"check file ${file.getName}")
    if (System.currentTimeMillis() - file.lastModified() > time) {
      if (file.delete()) {
        info(s"delete file ${file.getName} success")
      } else {
        warn(s"delete file ${file.getName} fail")
      }
    }
  }

  def checkAndDeleteDir(dir: File): Unit = {
    if (dir.listFiles.isEmpty) {
      if (dir.delete()) {
        info(s"delete dir ${dir.getName} success")
      } else {
        warn(s"delete dir ${dir.getName} fail")
      }
    }
  }

  import scala.sys.process._

  def checkUsedCapacity(dir: String): Boolean = {
    val used = (s"df ${dir}" #| s"grep ${dir}").!!
      .split(" ").filter(_.endsWith("%")) {
      0
    }.replace("%", "")
    info(s"${dir} now used ${used}% space")

    used.toInt > freeSpaceThreshold
  }

  def initializeConfiguration(): Unit = {
    if (cacheDirs == null || cacheDirs.isEmpty) {
      throw new IllegalArgumentException(s"the env ${CACHE_DIRS_KEY} can not be null")
    }

    if (fileExpiredTime < 0) {
      throw new IllegalArgumentException(s"the env ${FILE_EXPIRED_TIME_KEY} " +
        s"should be greater than 0")
    }

    if (deepCleanFileExpiredTime < 0) {
      throw new IllegalArgumentException(s"the env ${DEEP_CLEAN_FILE_EXPIRED_TIME_KEY} " +
        s"should be greater than 0")
    }

    if (sleepTime < 0) {
      throw new IllegalArgumentException(s"the env ${SLEEP_TIME_KEY} " +
        s"should be greater than 0")
    }

    if (freeSpaceThreshold < 0 || freeSpaceThreshold > 100) {
      throw new IllegalArgumentException(s"the env ${FREE_SPACE_THRESHOLD_KEY} " +
        s"should between 0 and 100")
    }

    info(s"finish initializing configuration, " +
      s"use ${CACHE_DIRS_KEY}: ${cacheDirs.mkString(",")},  " +
      s"${FILE_EXPIRED_TIME_KEY}: ${fileExpiredTime},  " +
      s"${FREE_SPACE_THRESHOLD_KEY}: ${freeSpaceThreshold}, " +
      s"${SLEEP_TIME_KEY}: ${sleepTime}, " +
      s"${DEEP_CLEAN_FILE_EXPIRED_TIME_KEY}: ${deepCleanFileExpiredTime}")
  }

  def main(args: Array[String]): Unit = {
    initializeConfiguration()

    while (true) {
      info("start clean job")
      cacheDirs.foreach(pathStr => {
        val path = Paths.get(pathStr)

        if (!Files.exists(path)) {
          throw new IllegalArgumentException(s"this path ${pathStr} does not exists")
        }

        if (!Files.isDirectory(path)) {
          throw new IllegalArgumentException(s"this path ${pathStr} is not directory")
        }

        // Clean up files older than $fileExpiredTime
        doClean(path.toFile, fileExpiredTime)

        if (checkUsedCapacity(pathStr)) {
          info("start deep clean job")
          doClean(path.toFile, deepCleanFileExpiredTime)
          if (checkUsedCapacity(pathStr)) {
            warn(s"after deep clean ${pathStr} " +
              s"used space still higher than ${freeSpaceThreshold}")
          }
        }

      })
      // Once $sleepTime
      Thread.sleep(sleepTime)
    }
  }
}
