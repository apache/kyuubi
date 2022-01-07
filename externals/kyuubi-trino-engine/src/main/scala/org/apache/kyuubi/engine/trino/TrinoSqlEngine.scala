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

package org.apache.kyuubi.engine.trino

import scopt.OptionParser

import org.apache.kyuubi.Logging
import org.apache.kyuubi.util.SignalRegister

object TrinoSqlEngine extends Logging {
  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)

    val parser = new OptionParser[TrinoEngineArguments]("trino") {
      opt[String]("user")
        .action((v, c) => c.copy(user = v))
      opt[String]("server")
        .action((v, c) => c.copy(server = v))
      opt[String]("catalog")
        .action((v, c) => c.copy(catalog = v))
      opt[String]("schema")
        .action((v, c) => c.copy(schema = v))
      opt[String]("conf")
        .action { (v, c) =>
          val (key, value) = v.split("=") match {
            case Array(key, value) => (key, value)
          }
          val newConfigs = c.configs + (key -> value)
          c.copy(configs = newConfigs)
        }
        .maxOccurs(100)
    }

    parser.parse(args, TrinoEngineArguments()) match {
      case Some(params) => runProgram(params)
      case _ =>
        error("Parse parameters failed!")
        sys.exit(1)
    }
  }

  def runProgram(args: TrinoEngineArguments): Unit = {
    // TODO start engine
    warn("Trino engine under development...")
    info(args)
  }
}
