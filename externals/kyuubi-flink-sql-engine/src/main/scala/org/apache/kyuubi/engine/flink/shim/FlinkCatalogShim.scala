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

package org.apache.kyuubi.engine.flink.shim

import org.apache.flink.table.api.internal.TableEnvironmentInternal

import org.apache.kyuubi.Utils

trait FlinkCatalogShim {

  def getCatalogs(tableEnv: TableEnvironmentInternal): Array[String]

  protected def catalogExists(tableEnv: TableEnvironmentInternal, catalog: String): Boolean

}

object FlinkCatalogShim {

  def apply(): FlinkCatalogShim = {
    import org.apache.flink.runtime.util.EnvironmentInformation
    val flinkVersion = EnvironmentInformation.getVersion
    val (major, minor) = Utils.majorMinorVersion(flinkVersion)

    (major, minor) match {
      case (_, 12) => new CatalogShim_v1_12
      case _ => throw new IllegalArgumentException(s"Not Support flink version $flinkVersion")
    }
  }

}
