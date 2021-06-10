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

package org.apache.kyuubi

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.log4j.PropertyConfigurator
import org.slf4j.{Logger, LoggerFactory}

import org.apache.kyuubi.Constants._

object KubernetesCacheFileCleaner {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var freeSpaceThreshold = FREE_SPACE_THRESHOLD_DEFAULT_VALUE
  private var cacheDirs = CACHE_DIRS_DEFAULT_VALUE
  private var fileExpiredTime = FILE_EXPIRED_TIME_DEFAULT_VALUE
  private var cycleIntervalTime = CYCLE_INTERVAL_TIME_DEFAULT_VALUE
  private var deepCleanFileExpiredTime = DEEP_CLEAN_FILE_EXPIRED_TIME_DEFAULT_VALUE

  def doClean(dir: File, time: Long) {
    logger.info(s"start clean ${dir.getName} with fileExpiredTime ${time}")

    // clean blockManager shuffle file
    dir.listFiles.filter(_.isDirectory).filter(_.getName.startsWith(BLOCK_MANAGER_PREFIX))
      .foreach(blockManagerDir => {

        logger.info(s"start check blockManager dir ${blockManagerDir.getName}")
        // check blockManager directory
        blockManagerDir.listFiles.filter(_.isDirectory).foreach(subDir => {

          logger.info(s"start check sub dir ${subDir.getName}")
          // check sub directory
          subDir.listFiles.foreach(file => checkAndDeleteFIle(file, time))
          // delete empty sub directory
          checkAndDeleteDir(subDir)
        })
        // delete empty blockManager directory
        checkAndDeleteDir(blockManagerDir)
      })

    // clean spark cache file
    dir.listFiles.filter(_.isDirectory).filter(_.getName.startsWith(SPARK_CACHE_DIR_PREFIX))
      .foreach(cacheDir => {
        logger.info(s"start check cache dir ${cacheDir.getName}")
        cacheDir.listFiles.foreach(file => checkAndDeleteFIle(file, time))
      })
  }

  def checkAndDeleteFIle (file: File, time: Long) : Unit = {
    logger.info(s"check file ${file.getName}")
    if (System.currentTimeMillis() - file.lastModified() > time) {
      if (file.delete()) {
        logger.info(s"delete file ${file.getName} success")
      } else {
        logger.warn(s"delete file ${file.getName} fail")
      }
    }
  }

  def checkAndDeleteDir (dir: File) : Unit = {
    if (dir.listFiles.isEmpty) {
      if (dir.delete()) {
        logger.info(s"delete dir ${dir.getName} success")
      } else {
        logger.warn(s"delete dir ${dir.getName} fail")
      }
    }
  }

  import scala.sys.process._

  def checkUsedMemory(dir: String) : Boolean = {
    val used = (s"df ${dir}" #| s"grep ${dir}").!!
      .split(" ").filter(_.endsWith("%")) {0}.replace("%", "")
    logger.info(s"${dir} now used ${used}% space")

    used.toInt > freeSpaceThreshold
  }

  def initializeConfiguration(args: Array[String]) : Unit = {
    // build options
    val options = new Options()
    options.addOption(CACHE_DIRS_KEY, true, "address where the cache file is stored")
    options.addOption(FILE_EXPIRED_TIME_KEY, true, "time to determine whether to recover files")
    options.addOption(CYCLE_INTERVAL_TIME_KEY, true, "the interval of each round of cleaning")
    options.addOption(FREE_SPACE_THRESHOLD_KEY, true,
      "determine whether to enter the threshold of deep cleaning")
    options.addOption(DEEP_CLEAN_FILE_EXPIRED_TIME_KEY, true,
      "time to determine whether to recover files for deep cleaning")

    // fetch input env
    val argEnv = new DefaultParser().parse(options, args)

    if (!argEnv.hasOption(CACHE_DIRS_KEY)) {
      throw new IllegalArgumentException(s"the env ${CACHE_DIRS_KEY} can not be null")
    } else {
      cacheDirs = argEnv.getOptionValue(CACHE_DIRS_KEY).split(",")
    }

    if (argEnv.hasOption(FILE_EXPIRED_TIME_KEY)) {
      fileExpiredTime = argEnv.getOptionValue(FILE_EXPIRED_TIME_KEY).toInt

      if (fileExpiredTime < 0) {
        throw new IllegalArgumentException(s"the env ${FILE_EXPIRED_TIME_KEY} " +
          s"should be greater than 0")
      }
    }

    if (argEnv.hasOption(DEEP_CLEAN_FILE_EXPIRED_TIME_KEY)) {
      deepCleanFileExpiredTime = argEnv.getOptionValue(DEEP_CLEAN_FILE_EXPIRED_TIME_KEY).toInt

      if (deepCleanFileExpiredTime < 0) {
        throw new IllegalArgumentException(s"the env ${DEEP_CLEAN_FILE_EXPIRED_TIME_KEY} " +
          s"should be greater than 0")
      }
    }

    if (argEnv.hasOption(CYCLE_INTERVAL_TIME_KEY)) {
      cycleIntervalTime = argEnv.getOptionValue(CYCLE_INTERVAL_TIME_KEY).toInt

      if (cycleIntervalTime < 0) {
        throw new IllegalArgumentException(s"the env ${CYCLE_INTERVAL_TIME_KEY} " +
          s"should be greater than 0")
      }
    }

    if (argEnv.hasOption(FREE_SPACE_THRESHOLD_KEY)) {
      freeSpaceThreshold = argEnv.getOptionValue(FREE_SPACE_THRESHOLD_KEY).toInt

      if (freeSpaceThreshold < 0 || freeSpaceThreshold > 100) {
        throw new IllegalArgumentException(s"the env ${FREE_SPACE_THRESHOLD_KEY} " +
          s"should between 0 and 100")
      }
    }

    logger.info(s"finish initializing configuration, " +
      s"use ${CACHE_DIRS_KEY}: ${cacheDirs.mkString(",")},  " +
      s"${FILE_EXPIRED_TIME_KEY}: ${fileExpiredTime},  " +
      s"${FREE_SPACE_THRESHOLD_KEY}: ${freeSpaceThreshold}, " +
      s"${CYCLE_INTERVAL_TIME_KEY}: ${cycleIntervalTime}, " +
      s"${DEEP_CLEAN_FILE_EXPIRED_TIME_KEY}: ${deepCleanFileExpiredTime}")
  }

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(Thread.currentThread()
      .getContextClassLoader.getResource("log4j-defaults.properties"))

    initializeConfiguration(args)

    while (true) {
      logger.info("start clean job")
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

        if (checkUsedMemory(pathStr)) {
          logger.info("start deep clean job")
          doClean(path.toFile, deepCleanFileExpiredTime)
          if (checkUsedMemory(pathStr)) {
            logger.warn(s"after deep clean ${pathStr} " +
              s"used space still higher than ${freeSpaceThreshold}")
          }
        }

      })
      // Once $cycleIntervalTime
      Thread.sleep(cycleIntervalTime)
    }
  }
}
