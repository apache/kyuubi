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

package org.apache.kyuubi.ctl

import java.io.{OutputStream, PrintStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.ctl.cli.{AdminControlCli, AdminControlCliArguments, ControlCli, ControlCliArguments}
import org.apache.kyuubi.ctl.util.CommandLineUtils

trait TestPrematureExit {
  suite: KyuubiFunSuite =>

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    val lineBuffer = ArrayBuffer[String]()
    // scalastyle:off println
    override def println(line: Any): Unit = {
      lineBuffer += line.toString
    }
    // scalastyle:on println
  }

  /** Returns true if the script exits and the given search string is printed. */
  private[kyuubi] def testPrematureExitForControlCli(
      input: Array[String],
      searchString: String,
      mainObject: CommandLineUtils = ControlCli): String = {
    val printStream = new BufferPrintStream()
    mainObject.printStream = printStream

    @volatile var exitedCleanly = false
    val original = mainObject.exitFn
    mainObject.exitFn = _ => exitedCleanly = true
    try {
      @volatile var exception: Exception = null
      val thread = new Thread {
        override def run(): Unit =
          try {
            mainObject.main(input)
          } catch {
            // Capture the exception to check whether the exception contains searchString or not
            case e: Exception => exception = e
          }
      }
      thread.start()
      thread.join()
      var joined = ""
      if (exitedCleanly) {
        joined = printStream.lineBuffer.mkString("\n")
        assert(joined.contains(searchString))
      } else {
        assert(exception != null)
        if (!exception.getMessage.contains(searchString)) {
          throw exception
        }
      }
      joined
    } finally {
      mainObject.exitFn = original
    }
  }

  def testPrematureExitForControlCliArgs(args: Array[String], searchString: String): Unit = {
    val logAppender = new LogAppender("test premature exit")
    withLogAppender(logAppender) {
      val thread = new Thread {
        override def run(): Unit =
          try {
            new ControlCliArguments(args)
          } catch {
            case e: Exception =>
              error(e)
          }
      }
      thread.start()
      thread.join()
      assert(logAppender.loggingEvents.exists(
        _.getMessage.getFormattedMessage.contains(searchString)))
    }
  }

  /** Returns true if the script exits and the given search string is printed. */
  private[kyuubi] def testPrematureExitForAdminControlCli(
      input: Array[String],
      searchString: String): String = {
    testPrematureExitForControlCli(input, searchString, AdminControlCli)
  }

  def testPrematureExitForAdminControlCliArgs(args: Array[String], searchString: String): Unit = {
    val logAppender = new LogAppender("test premature exit")
    withLogAppender(logAppender) {
      val thread = new Thread {
        override def run(): Unit =
          try {
            new AdminControlCliArguments(args)
          } catch {
            case e: Exception =>
              error(e)
          }
      }
      thread.start()
      thread.join()
      assert(logAppender.loggingEvents.exists(
        _.getMessage.getFormattedMessage.contains(searchString)))
    }
  }
}
