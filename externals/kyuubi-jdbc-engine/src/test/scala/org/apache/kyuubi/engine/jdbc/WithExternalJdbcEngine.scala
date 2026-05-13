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
package org.apache.kyuubi.engine.jdbc

import org.scalatest.{Args, BeforeAndAfterAll, Status, Suite}

trait WithExternalJdbcEngine extends BeforeAndAfterAll {
  this: Suite =>

  protected def externalJdbcUrl: Option[String]

  // Testcontainers-scala wraps Suite.run to start/stop containers. When an external JDBC URL
  // is supplied, bypass that wrapper but still keep BeforeAndAfterAll so the Kyuubi engine's
  // beforeAll/afterAll lifecycle runs.
  abstract override def run(testName: Option[String], args: Args): Status = {
    if (externalJdbcUrl.isDefined) super[BeforeAndAfterAll].run(testName, args)
    else super.run(testName, args)
  }
}
