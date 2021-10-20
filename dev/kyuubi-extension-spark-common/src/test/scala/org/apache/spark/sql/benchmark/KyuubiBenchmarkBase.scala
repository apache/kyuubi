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

package org.apache.spark.sql.benchmark

import java.io.{File, FileOutputStream, OutputStream}

import scala.collection.JavaConverters._

import com.google.common.reflect.ClassPath

trait KyuubiBenchmarkBase {
  var output: Option[OutputStream] = None

  private val prefix = {
    val benchmarkClasses = ClassPath.from(
      Thread.currentThread.getContextClassLoader
    ).getTopLevelClassesRecursive("org.apache.spark.sql").asScala.toArray
    assert(benchmarkClasses.nonEmpty)
    val benchmark = benchmarkClasses.find(_.load().getName.endsWith("Benchmark"))
    val targetDirOrProjDir =
      new File(benchmark.get.load().getProtectionDomain.getCodeSource.getLocation.toURI)
        .getParentFile.getParentFile
    if (targetDirOrProjDir.getName == "target") {
      targetDirOrProjDir.getParentFile.getCanonicalPath + "/"
    } else {
      targetDirOrProjDir.getCanonicalPath + "/"
    }
  }

  def withHeader(func: => Unit): Unit = {
    val version = System.getProperty("java.version").split("\\D+")(0).toInt
    val jdkString = if (version > 8) s"-jdk$version" else ""
    val resultFileName =
      s"${this.getClass.getSimpleName.replace("$", "")}$jdkString-results.txt"
    val dir = new File(s"${prefix}benchmarks/")
    if (!dir.exists()) {
      // scalastyle:off println
      println(s"Creating ${dir.getAbsolutePath} for benchmark results.")
      // scalastyle:on println
      dir.mkdirs()
    }
    val file = new File(dir, resultFileName)
    if (!file.exists()) {
      file.createNewFile()
    }
    output = Some(new FileOutputStream(file))

    func

    output.foreach { o =>
      if (o != null) {
        o.close()
      }
    }
  }
}
