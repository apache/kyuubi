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

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.HiveExternalCatalog

import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorConf.{EXTERNAL_CATALOG_POOL_ENABLED, EXTERNAL_CATALOG_POOL_SIZE}

object ExternalCatalogPool {
  private var pool: ExternalCatalogPool = _

  def getOrCreate(sparkSession: SparkSession): ExternalCatalogPool = {
    if (pool == null) {
      synchronized {
        val conf = sparkSession.sessionState.conf
        if (conf.getConf(EXTERNAL_CATALOG_POOL_ENABLED)) {
          pool = new PriorityPool(conf.getConf(EXTERNAL_CATALOG_POOL_SIZE))
        } else {
          pool = NoopPool()
        }
      }
    }
    pool
  }

  private[kyuubi] def reset(): Unit = {
    if (pool != null) {
      pool.invalidateAll()
      pool = null
    }
  }
}

abstract class ExternalCatalogPool {

  def take(ticket: Ticket): HiveExternalCatalog

  def invalidateAll(): Unit = {}
}

class PriorityPool(threshold: Int) extends ExternalCatalogPool {

  private val catalogPool = new ConcurrentHashMap[String, CatalogQueue]()

  override def take(ticket: Ticket): HiveExternalCatalog = {
    val catalogQueue = catalogPool.computeIfAbsent(
      ticket.catalogName,
      _ => {
        CatalogQueue(threshold, ticket)
      })
    catalogQueue.poll()
  }

  override def invalidateAll(): Unit = {
    catalogPool.clear()
  }
}

case class CatalogQueue(threshold: Int, ticket: Ticket) {
  private val catalogs = new ArrayBlockingQueue[HiveExternalCatalog](threshold)

  def poll(): HiveExternalCatalog = synchronized {
    if (canGrow) {
      val catalog = new HiveExternalCatalog(ticket.sparkConf, ticket.hadoopConf)
      assertExpectedAction(catalogs.offer(catalog))
      catalog
    } else {
      val headCatalog = catalogs.remove()
      assertExpectedAction(catalogs.offer(headCatalog))
      headCatalog
    }
  }

  private def canGrow: Boolean = {
    catalogs.size() < threshold
  }

  def assertExpectedAction(f: => Boolean): Unit = {
    val success = f

    if (!success) {
      throw KyuubiHiveConnectorException("Unexpected behavior, " +
        "please report the bug to Kyuubi community")
    }
  }
}

case class NoopPool() extends ExternalCatalogPool {

  override def take(ticket: Ticket): HiveExternalCatalog = {
    val externalCatalog = new HiveExternalCatalog(ticket.sparkConf, ticket.hadoopConf)
    externalCatalog
  }
}

case class Ticket(catalogName: String, sparkConf: SparkConf, hadoopConf: Configuration)
