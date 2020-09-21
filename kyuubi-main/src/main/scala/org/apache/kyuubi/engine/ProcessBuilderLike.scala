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

package org.apache.kyuubi.engine

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.kyuubi.Utils

trait ProcessBuilderLike {

  protected def executable: String

  protected def mainResource: Option[String]

  protected def mainClass: String

  protected def proxyUser: String

  protected def commands: Array[String]

  protected def env: Map[String, String]

  protected def workingDir: Path

  final lazy val processBuilder: ProcessBuilder = {
    val pb = new ProcessBuilder(commands: _*)

    val envs = pb.environment()
    envs.putAll(env.asJava)

    pb.directory(workingDir.toFile)
    val procLogFile =
      Paths.get(workingDir.toAbsolutePath.toString, UUID.randomUUID().toString).toFile
    pb.redirectError(procLogFile)
    pb.redirectOutput(procLogFile)
    pb
  }

  final def start: Process = {
    processBuilder.start()
  }
}
