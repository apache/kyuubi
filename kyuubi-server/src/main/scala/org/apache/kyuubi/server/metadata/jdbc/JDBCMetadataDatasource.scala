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

package org.apache.kyuubi.server.metadata.jdbc

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.metadata.jdbc.DatabaseType._
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataDatasource.hikariDataSource
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.service.CompositeService

class JDBCMetadataDatasource extends CompositeService("MetadataDatasource") {

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val dbType = DatabaseType.withName(conf.get(METADATA_STORE_JDBC_DATABASE_TYPE))
    val driverClassOpt = conf.get(METADATA_STORE_JDBC_DRIVER)
    val driverClass = dbType match {
      case DERBY => driverClassOpt.getOrElse("org.apache.derby.jdbc.AutoloadedDriver")
      case MYSQL => driverClassOpt.getOrElse("com.mysql.jdbc.Driver")
      case CUSTOM => driverClassOpt.getOrElse(
          throw new IllegalArgumentException("No jdbc driver defined"))
    }
    val datasourceProperties =
      JDBCMetadataStoreConf.getMetadataStoreJDBCDataSourceProperties(conf)
    val hikariConfig = new HikariConfig(datasourceProperties)
    hikariConfig.setDriverClassName(driverClass)
    hikariConfig.setJdbcUrl(conf.get(METADATA_STORE_JDBC_URL))
    hikariConfig.setUsername(conf.get(METADATA_STORE_JDBC_USER))
    hikariConfig.setPassword(conf.get(METADATA_STORE_JDBC_PASSWORD))
    hikariConfig.setPoolName("jdbc-metadata-store-pool")
    hikariDataSource = Option(new HikariDataSource(hikariConfig))
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    super.start()
  }

  override def stop(): Unit = synchronized {
    super.stop()
  }

}

object JDBCMetadataDatasource {

  @volatile var hikariDataSource: Option[HikariDataSource] = None

}
