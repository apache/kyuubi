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

package org.apache.kyuubi.engine.spark.repl

import java.io.{ByteArrayOutputStream, File}

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.JPrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.repl.{Main, SparkILoop}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.MutableURLClassLoader

private[spark] case class KyuubiSparkILoop private (
    spark: SparkSession,
    output: ByteArrayOutputStream)
  extends SparkILoop(None, new JPrintWriter(output)) {

  private val result = new DataFrameHolder()

  private def initialize(): Unit = {
    settings = new Settings
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir",
      s"${Main.outputDir.getAbsolutePath}")
    settings.processArguments(interpArguments, processAll = true)
    settings.usejavacp.value = true
    val currentClassLoader = Thread.currentThread().getContextClassLoader
    settings.embeddedDefaults(currentClassLoader)
    this.createInterpreter()
    this.initializeSynchronous()
    try {
      this.compilerClasspath
      this.ensureClassLoader()
      var classLoader = Thread.currentThread().getContextClassLoader
      while (classLoader != null) {
        classLoader match {
          case loader: MutableURLClassLoader =>
            val allJars = loader.getURLs.filter { u =>
              val file = new File(u.getPath)
              u.getProtocol == "file" && file.isFile &&
              file.getName.contains("scala-lang_scala-reflect")
            }
            this.addUrlsToClassPath(allJars: _*)
            classLoader = null
          case _ =>
            classLoader = classLoader.getParent
        }
      }
    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader)
    }

    this.beQuietDuring {
      // SparkSession/SparkContext and their implicits
      this.bind("spark", classOf[SparkSession].getCanonicalName, spark, List("""@transient"""))
      this.bind(
        "sc",
        classOf[SparkContext].getCanonicalName,
        spark.sparkContext,
        List("""@transient"""))

      this.interpret("import org.apache.spark.SparkContext._")
      this.interpret("import spark.implicits._")
      this.interpret("import spark.sql")
      this.interpret("import org.apache.spark.sql.functions._")

      // for feeding results to client, e.g. beeline
      this.bind(
        "result",
        classOf[DataFrameHolder].getCanonicalName,
        result,
        List("""@transient"""))
    }
  }

  def getResult: DataFrame = result.get()

  def getOutput: String = {
    val res = output.toString.trim
    output.reset()
    res
  }
}

private[spark] object KyuubiSparkILoop {
  def apply(spark: SparkSession): KyuubiSparkILoop = {
    val os = new ByteArrayOutputStream()
    val iLoop = new KyuubiSparkILoop(spark, os)
    iLoop.initialize()
    iLoop
  }
}
