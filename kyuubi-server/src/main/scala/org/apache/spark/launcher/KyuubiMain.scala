/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.launcher

import java.util.{ArrayList => JAList, HashMap => HMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.launcher.CommandBuilderUtils._

object KyuubiMain {

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val argAsJava = args.toList.asJava
    val printStream = System.err
    val builder = Try { new KyuubiSubmitCommandBuilder(argAsJava) } match {
      case Success(b) => b
      case Failure(e) =>
        // scalastyle:off
        printStream.println("Error: " + e.getMessage)
        printStream.println()
        // scalastyle:on
        val parser = new MainClassOptionParser
        try {
          parser.parse(argAsJava)
        } catch {
          case _: Throwable => // ignored
        }

        val help = new ArrayBuffer[String]
        if (parser.className != null) {
          help += parser.CLASS
          help += parser.className
        }
        help += parser.USAGE_ERROR
        new SparkSubmitCommandBuilder(help.toList.asJava)
    }

    val env = new HMap[String, String]()
    val cmd = builder.buildCommand(env)
    // scalastyle:off
    printStream.println("Kyuubi Command: " + join(" ", cmd))
    printStream.println("========================================")
    // scalastyle:on
    prepareBashCommand(cmd, env).asScala.foreach { c =>
      print(c)
      print('\u0000')
    }
  }

  private[this] def prepareBashCommand(
      cmd: JList[String],
      childEnv: JMap[String, String]): JList[String] = {
    if (childEnv.isEmpty) return cmd
    val newCmd = new JAList[String]
    newCmd.add("env")
    childEnv.asScala.foreach(e => newCmd.add(String.format("%s=%s", e._1, e._2)))
    newCmd.addAll(cmd)
    newCmd
  }

  private class MainClassOptionParser extends SparkSubmitOptionParser {
    private[launcher] var className: String = _
    override protected def handle(opt: String, value: String): Boolean = {
      if (CLASS == opt) {
        className = value
      }
      false
    }
    override protected def handleUnknown(opt: String) = false
    override protected def handleExtraArgs(extra: JList[String]): Unit = {}
  }

}