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

package org.apache.kyuubi.spark.connector.hive

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.HiveExternalCatalog

import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorConf.EXTERNAL_CATALOG_SHARE_POLICY

object ExternalCatalogManager {
  private var manager: ExternalCatalogManager = _

  def getOrCreate(sparkSession: SparkSession): ExternalCatalogManager = {
    if (manager == null) {
      synchronized {
        if (manager == null) {
          val conf = sparkSession.sessionState.conf
          manager = ExternalCatalogSharePolicy.fromString(
            conf.getConf(EXTERNAL_CATALOG_SHARE_POLICY)) match {
            case OneForAllPolicy => new OneForAllPolicyManager()
            case OneForOnePolicy => OneForOnePolicyManager
          }
        }
      }
    }
    manager
  }

  private[kyuubi] def reset(): Unit = {
    if (manager != null) {
      manager.invalidateAll()
      manager = null
    }
  }
}

abstract class ExternalCatalogManager {

  def take(ticket: Ticket): HiveExternalCatalog

  def invalidateAll(): Unit = {}
}

/**
 * A [[OneForAllPolicy]] policy for the externalCatalog manager, which caches the externalCatalog
 * according to the catalogName, aiming for only one of each catalog globally.
 */
class OneForAllPolicyManager() extends ExternalCatalogManager {
  private val catalogCache = new ConcurrentHashMap[String, HiveExternalCatalog]()

  override def take(ticket: Ticket): HiveExternalCatalog = {
    catalogCache.computeIfAbsent(
      ticket.catalogName,
      _ => {
        new HiveExternalCatalog(ticket.sparkConf, ticket.hadoopConf)
      })
  }
}

/**
 * A [[OneForOnePolicy]] policy for the externalCatalog manager, It doesn't actually cache any
 * externalCatalog, each session will have its own externalCatalog.
 */
object OneForOnePolicyManager extends ExternalCatalogManager {

  override def take(ticket: Ticket): HiveExternalCatalog = {
    new HiveExternalCatalog(ticket.sparkConf, ticket.hadoopConf)
  }
}

case class Ticket(catalogName: String, sparkConf: SparkConf, hadoopConf: Configuration)
