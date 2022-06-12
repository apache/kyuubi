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

package org.apache.kyuubi.operation

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util.UUID
import java.util.jar.{JarEntry, JarOutputStream}

import scala.tools.nsc.{Global, Settings}

import org.apache.commons.io.FileUtils

object UserJarTestUtils {

  def createJarFile(
      codeText: String,
      packageRootPath: String,
      jarName: String,
      outputDir: String): File = {
    val codeFile = new File(outputDir, s"test-${UUID.randomUUID}.scala")
    FileUtils.writeStringToFile(codeFile, codeText, "UTF-8")

    val settings = new Settings
    settings.outputDirs.setSingleOutput(outputDir)
    settings.usejavacp.value = true
    val global = new Global(settings)
    val runner = new global.Run
    runner.compile(List(codeFile.getAbsolutePath))
    val jarFile = new File(outputDir, jarName)
    val targetJar = new JarOutputStream(new FileOutputStream(jarFile))
    add(s"$outputDir/$packageRootPath", targetJar, outputDir + "/")
    targetJar.close()
    jarFile
  }

  private def add(folder: String, target: JarOutputStream, replacement: String): Unit = {
    val source = new File(folder)
    if (source.isDirectory) {
      for (nestedFile <- source.listFiles) {
        add(nestedFile.getAbsolutePath, target, replacement)
      }
    } else {
      val entry = new JarEntry(source.getPath
        .replace("\\", "/")
        .replace(replacement, ""))
      entry.setTime(source.lastModified)
      target.putNextEntry(entry)
      val byteArray = Files.readAllBytes(source.toPath)
      target.write(byteArray)
      target.closeEntry()
    }
  }
}
