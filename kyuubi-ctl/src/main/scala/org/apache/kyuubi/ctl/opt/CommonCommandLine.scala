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

package org.apache.kyuubi.ctl.opt

import scopt.{OParser, OParserBuilder}

import org.apache.kyuubi.KyuubiException

trait CommonCommandLine {

  def common(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      opt[Unit]('b', "verbose")
        .action((_, c) => c.copy(commonOpts = c.commonOpts.copy(verbose = true)))
        .text("Print additional debug output."),
      opt[String]("hostUrl")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(hostUrl = v)))
        .text("Host url for rest api."),
      opt[String]("authSchema")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(authSchema = v)))
        .text("Auth schema for rest api, valid values are basic, spnego."),
      opt[String]("username")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(username = v)))
        .text("Username for basic authentication."),
      opt[String]("password")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(password = v)))
        .text("Password for basic authentication."),
      opt[String]("spnegoHost")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(spnegoHost = v)))
        .text("Spnego host for spnego authentication."),
      opt[String]("hs2ProxyUser")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(hs2ProxyUser = v)))
        .text("The value of hive.server2.proxy.user config."),
      opt[String]("conf")
        .unbounded()
        .action((v, c) => {
          v.split("=", 2).toSeq match {
            case Seq(k, v) => c.copy(conf = c.conf ++ Map(k -> v))
            case _ => throw new KyuubiException(s"Kyuubi config without '=': $v")
          }
        })
        .text("Kyuubi config property pair, formatted key=value."))
  }
}
