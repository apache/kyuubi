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

package org.apache.kyuubi.engine.spark.connect

/**
 * Stand-ins for the Spark Connect classes the engine reaches by reflection, so that the reflection
 * itself is exercised. The engine module does not depend on `spark-connect`, and adding the real
 * jar to the test classpath is a dependency problem of its own.
 */
object FakeConnectPlugin

/** Shaped like `org.apache.spark.sql.connect.config.Connect` on a Spark that authenticates. */
object FakeConnectConfig {
  def getAuthenticateToken: Option[String] = Some("a-token")
}

/** Shaped like `org.apache.spark.sql.connect.service.SparkConnectService` with a bound port. */
object FakeConnectService {
  def localPort: Int = 15002
}
