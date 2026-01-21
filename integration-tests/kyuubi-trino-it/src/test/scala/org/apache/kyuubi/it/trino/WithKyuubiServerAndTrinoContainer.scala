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

package org.apache.kyuubi.it.trino

import com.dimafeng.testcontainers.TrinoContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.util.JavaUtils

trait WithKyuubiServerAndTrinoContainer extends WithKyuubiServer with TestContainerForAll {

  val kyuubiHome: String = JavaUtils.getCodeSourceLocation(getClass).split("integration-tests").head

  final val IMAGE_VERSION = 411
  final val DOCKER_IMAGE_NAME = s"trinodb/trino:$IMAGE_VERSION"

  override val containerDef: TrinoContainer.Def = TrinoContainer.Def(DOCKER_IMAGE_NAME)

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME_ENV_VAR_NAME", kyuubiHome)
      .set(ENGINE_TYPE, "TRINO")
      .set(ENGINE_TRINO_CONNECTION_CATALOG, "memory")
  }

  override def beforeAll(): Unit = {
    // start trino cluster containers
    withContainers { trinoContainer =>
      val trinoConnectionUrl = trinoContainer.jdbcUrl.replace("jdbc:trino", "http")
      conf.set(ENGINE_TRINO_CONNECTION_URL, trinoConnectionUrl)

      super.beforeAll()
    }
  }
}
