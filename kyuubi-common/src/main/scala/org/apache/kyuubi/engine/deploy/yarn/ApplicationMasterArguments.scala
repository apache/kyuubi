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
package org.apache.kyuubi.engine.deploy.yarn

import org.apache.kyuubi.Logging

class ApplicationMasterArguments(val args: Array[String]) extends Logging {
  var engineMainClass: String = null
  var propertiesFile: String = null

  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case ("--class") :: value :: tail =>
          engineMainClass = value
          args = tail

        case ("--properties-file") :: value :: tail =>
          propertiesFile = value
          args = tail

        case other =>
          throw new IllegalArgumentException(s"Unrecognized option $other.")
      }
    }
    validateRequiredArguments()
  }

  private def validateRequiredArguments(): Unit = {
    if (engineMainClass == null) {
      throw new IllegalArgumentException("No engine main class provided.")
    }

    if (propertiesFile == null) {
      throw new IllegalArgumentException("No properties file provided.")
    }
  }
}
