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

import org.apache.kyuubi.ctl.ServiceControlAction.ServiceControlAction
import org.apache.kyuubi.ctl.ServiceControlObject.ServiceControlObject

private[ctl] object ServiceControlAction extends Enumeration {
  type ServiceControlAction = Value
  val CREATE, GET, DELETE, LIST = Value
}

private[ctl] object ServiceControlObject extends Enumeration {
  type ServiceControlObject = Value
  val SERVER, ENGINE = Value
}

case class CliConfig(
    action: ServiceControlAction = null,
    service: ServiceControlObject = ServiceControlObject.SERVER,
    commonOpts: CommonOpts = CommonOpts(),
    engineOpts: EngineOpts = EngineOpts())

case class CommonOpts(
    zkQuorum: String = null,
    namespace: String = null,
    host: String = null,
    port: String = null,
    version: String = null,
    verbose: Boolean = false)

case class EngineOpts(
    user: String = null,
    engineType: String = null,
    engineSubdomain: String = null,
    engineShareLevel: String = null)
