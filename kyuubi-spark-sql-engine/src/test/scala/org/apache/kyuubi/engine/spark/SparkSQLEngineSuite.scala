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

package org.apache.kyuubi.engine.spark

import org.apache.kyuubi.KyuubiFunSuite

class SparkSQLEngineSuite extends KyuubiFunSuite {

  import org.apache.kyuubi.service.ServiceState._

  private val spark = SparkSQLEngine.createSpark()

  test("spark sql engine as a composite service") {
    val engine = new SparkSQLEngine(spark)
    assert(engine.getServiceState === LATENT)
    assert(engine.getServices.forall(_.getServiceState === LATENT))

    engine.initialize(SparkSQLEngine.kyuubiConf)
    assert(engine.getServiceState === INITIALIZED)
    assert(engine.getServices.forall(_.getServiceState === INITIALIZED))

    engine.start()
    assert(engine.getServiceState === STARTED)
    assert(engine.getServices.forall(_.getServiceState === STARTED))
  }
}
