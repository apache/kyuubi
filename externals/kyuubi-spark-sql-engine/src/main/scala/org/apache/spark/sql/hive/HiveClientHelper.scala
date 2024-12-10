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

package org.apache.spark.sql.hive

import java.net.URL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SessionResourceLoader
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

object HiveClientHelper {

  type HiveClientImpl = org.apache.spark.sql.hive.client.HiveClientImpl

  def getLoadedClasses(spark: SparkSession): Array[URL] = {
    if (spark.conf.get(CATALOG_IMPLEMENTATION).equals("hive")) {
      val loader = spark.sessionState.resourceLoader
      getHiveLoadedClasses(loader)
    } else {
      spark.sharedState.jarClassLoader.getURLs
    }
  }

  private def getHiveLoadedClasses(loader: SessionResourceLoader): Array[URL] = {
    if (loader != null) {
      val field = classOf[HiveSessionResourceLoader].getDeclaredField("client")
      field.setAccessible(true)
      val client = field.get(loader).asInstanceOf[HiveClientImpl]
      if (client != null) {
        client.clientLoader.classLoader.getURLs
      } else {
        Array.empty
      }
    } else {
      Array.empty
    }
  }
}
