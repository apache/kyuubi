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

package org.apache.kyuubi.engine.flink.util

import java.io.{File, FileOutputStream}
import java.nio.file.Paths
import java.util.Collections
import java.util.jar.{JarEntry, JarOutputStream}
import javax.tools.{DiagnosticCollector, JavaFileObject, ToolProvider}

import scala.collection.JavaConverters._

import org.apache.flink.util.FileUtils

object TestUserClassLoaderJar {

  /**
   * Pack the generated UDF class into a JAR and return the path of the JAR.
   */
  def createJarFile(tmpDir: File, jarName: String, className: String, javaCode: String): File = {
    // write class source code to file
    val javaFile = Paths.get(tmpDir.toString, className + ".java").toFile
    // no inspection ResultOfMethodCallIgnored
    javaFile.createNewFile
    FileUtils.writeFileUtf8(javaFile, javaCode)
    // compile class source code
    val diagnostics = new DiagnosticCollector[JavaFileObject]
    val compiler = ToolProvider.getSystemJavaCompiler
    val fileManager = compiler.getStandardFileManager(diagnostics, null, null)
    val compilationUnit =
      fileManager.getJavaFileObjectsFromFiles(Collections.singletonList(javaFile))
    val task =
      compiler.getTask(null, fileManager, diagnostics, List.empty.asJava, null, compilationUnit)
    task.call
    // pack class file to jar
    val classFile = Paths.get(tmpDir.toString, className + ".class").toFile
    val jarFile = Paths.get(tmpDir.toString, jarName).toFile
    val jos = new JarOutputStream(new FileOutputStream(jarFile))
    val jarEntry = new JarEntry(className + ".class")
    jos.putNextEntry(jarEntry)
    val classBytes = FileUtils.readAllBytes(classFile.toPath)
    jos.write(classBytes)
    jos.closeEntry()
    jos.close()
    jarFile
  }
}
