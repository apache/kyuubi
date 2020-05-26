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

package yaooqinn.kyuubi.yarn

import org.apache.kyuubi.Logging

case class AppMasterArguments(args: Array[String]) extends Logging {
  var propertiesFile: Option[String] = _

  init(args)

  private[this] def init(args: Array[String]): Unit = {
    require(args != null, "Arguments for Kyuubi Application Master can not be null")
    var tmp = args.toList

    while (tmp.nonEmpty) {
      tmp match {
        case "--properties-file" :: value :: tail =>
          propertiesFile = Some(value)
          tmp = tail
        case _ =>
          val msg = "Unknown/unsupported param " + tmp
          val usage =
            """
              |Usage: yaooqinn.kyuubi.yarn.KyuubiAppMaster [options]
              |Options:
              | --properties-file FILE Path to a custom Spark properties file.
            """.stripMargin
          error(msg)
          info(usage)
          throw new IllegalArgumentException(msg)
      }
    }
  }
}
