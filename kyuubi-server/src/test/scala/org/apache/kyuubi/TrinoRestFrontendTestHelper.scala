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

package org.apache.kyuubi

import org.glassfish.jersey.client.ClientConfig
import org.glassfish.jersey.test.JerseyTest

import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols.FrontendProtocol
import org.apache.kyuubi.server.trino.api.TrinoScalaObjectMapper

trait TrinoRestFrontendTestHelper extends RestFrontendTestHelper {

  private class TrinoRestBaseSuite extends RestFrontendTestHelper.RestApiBaseSuite {
    override def configureClient(config: ClientConfig): Unit = {
      config.register(classOf[TrinoScalaObjectMapper])
    }
  }

  override protected val frontendProtocols: Seq[FrontendProtocol] =
    FrontendProtocols.TRINO :: Nil

  override protected val restApiBaseSuite: JerseyTest = new TrinoRestBaseSuite

}
