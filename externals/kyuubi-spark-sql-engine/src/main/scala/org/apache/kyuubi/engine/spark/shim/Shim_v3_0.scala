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

package org.apache.kyuubi.engine.spark.shim

import org.apache.spark.sql.{Row, SparkSession}

class Shim_v3_0 extends Shim_v2_4 {

  override def getCatalogs(ss: SparkSession): Seq[Row] = {
    val sessionState = getSessionState(ss)
    val catalogMgr = invoke(sessionState, "catalogManager")
    // get the custom v2 session catalog or default spark_catalog
    val currentCatalog = invoke(catalogMgr, "v2SessionCatalog")
    val currentCatalogName = invoke(currentCatalog, "name")
      .asInstanceOf[String]
    val catalogs = getField(catalogMgr, "catalogs")
      .asInstanceOf[scala.collection.Map[String, _]]
    (catalogs.keys ++: Seq(currentCatalogName)).distinct.map(Row(_))
  }
}
