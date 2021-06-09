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


object Main {
  val DEFAULT_TIMEOUT = 604800000
  val BLOCK_MANAGER_PREFIX = "blockmgr"
  var threshold = 70
  var shuffleDirs: Array[String] = Array.empty
  var fileTimeout = 604800000

  def doClean(dir: File, time: Long) {
    print(s"start check dir ${dir.getName} with timeLimit ${time}\n")

    dir.listFiles.filter(_.isDirectory).filter(_.getName.startsWith(BLOCK_MANAGER_PREFIX))
      .foreach(blockManagerDir => {
        print(s"start check blockManger dir ${blockManagerDir.getName}\n")
        // check blockManager directory
        blockManagerDir.listFiles.filter(_.isDirectory).foreach(subDir => {
          print(s"start check executor dir ${subDir}\n")
          // check sub directory
          subDir.listFiles.foreach(file => {
            print(s"start check file ${file.getName}\n")
            // delete file
            if (System.currentTimeMillis() - file.lastModified() > time) {
              if (file.delete()) {
                print(s"delete file ${file.getName} success\n")
              } else {
                print(s"delete file ${file.getName} fail\n")
              }
            }
          })
          // delete empty sub directory
          if (subDir.listFiles().isEmpty) {
            if (subDir.delete()) {
              print(s"delete dir ${subDir.getName} success\n")
            } else {
              print(s"delete dir ${subDir.getName} fail\n")
            }
          }
        })
      // delete empty blockManager directory
      if (blockManagerDir.listFiles().isEmpty) {
        if (blockManagerDir.delete()) {
          print(s"delete dir ${blockManagerDir.getName} success\n")
        } else {
          print(s"delete dir ${blockManagerDir.getName} fail\n")
        }
      }
    })
  }

  import scala.sys.process._

  def checkUsed(dir: String, threshold: Int) : Boolean = {
    val used = (s"df ${dir}" #| s"grep ${dir}").!!
      .split(" ").filter(_.endsWith("%")) {0}.replace("%", "")
    print(s"${dir} now used ${used}%")
    used.toInt > 100 - threshold
  }

  def initializeConfiguration(args: Array[String]) : Unit = {
    var argsEnv = args.toList
    argsEnv.foreach(arg => print(arg + "\n"))
    while(argsEnv.nonEmpty) {
      argsEnv match {
        case ("--file-timeout") :: value :: tail =>
          fileTimeout = value.toInt
          argsEnv = tail
        case ("--shuffle-dirs") :: value :: tail =>
          shuffleDirs = value.split(",")
          argsEnv = tail
        case ("--threshold") :: value :: tail =>
          threshold = value.toInt
          argsEnv = tail
        case Nil =>
          throw new IllegalArgumentException(s"the env shuffleDirs should be configured")
        case tail =>
          print(s"""
                   |Usage: Spark-cleaner [options]
                   |
                   | Options are:
                   |   --file-timeout <fileTimeout>
                   |   --shuffle-dirs <shuffle-dirs>
                   |   --threshold <threshold>
                   |""".stripMargin)
          throw new IllegalArgumentException(s"No Match configuration")
      }
    }

    if (fileTimeout < 0) {
      throw new IllegalArgumentException(s"the env fileTimeout should be greater than 0")
    }
    if (shuffleDirs == null ||shuffleDirs.length == 0) {
      throw new IllegalArgumentException(s"the env shuffleDirs should be configured")
    }
    if (threshold < 0 || threshold > 100) {
      throw new IllegalArgumentException(s"the env fileTimeout should between 0 and 100")
    }
  }

  def main(args: Array[String]): Unit = {
    initializeConfiguration(args)
    while (true) {
      print("start clean job\n")
      shuffleDirs.foreach(pathStr => {
        val path = Paths.get(pathStr)
        if (!Files.exists(path)) {
          throw new IllegalArgumentException(s"this path ${pathStr} does not exists")
        }
        if (!Files.isDirectory(path)) {
          throw new IllegalArgumentException(s"this path ${pathStr} is not directory")
        }
        // Clean up files older than 7 days
        doClean(path.toFile, fileTimeout)
        if (checkUsed(pathStr, threshold)) {
          doClean(path.toFile, fileTimeout / 10)
        }
      })
      // Once an hour
      Thread.sleep(3600000)
    }
  }
}
