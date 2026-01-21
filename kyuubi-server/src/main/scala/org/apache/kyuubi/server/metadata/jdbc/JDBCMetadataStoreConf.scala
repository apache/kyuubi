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

import java.util.Properties

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.{ConfigEntry, KyuubiConf, OptionalConfigEntry}
import org.apache.kyuubi.config.KyuubiConf.buildConf

object JDBCMetadataStoreConf {
  final val METADATA_STORE_JDBC_DATASOURCE_PREFIX = "kyuubi.metadata.store.jdbc.datasource"

  def getMetadataStoreJdbcUrl(conf: KyuubiConf): String = {
    Utils.substituteKyuubiEnvVars(conf.get(METADATA_STORE_JDBC_URL))
  }

  /** Get metadata store jdbc datasource properties. */
  def getMetadataStoreJDBCDataSourceProperties(conf: KyuubiConf): Properties = {
    val datasourceProperties = new Properties()
    conf.getAllWithPrefix(METADATA_STORE_JDBC_DATASOURCE_PREFIX, "").foreach { case (key, value) =>
      datasourceProperties.put(key, value)
    }
    datasourceProperties
  }

  val METADATA_STORE_JDBC_DATABASE_TYPE: ConfigEntry[String] =
    buildConf("kyuubi.metadata.store.jdbc.database.type")
      .doc("The database type for server jdbc metadata store.<ul>" +
        " <li>SQLITE: SQLite3, JDBC driver `org.sqlite.JDBC`.</li>" +
        " <li>MYSQL: MySQL, JDBC driver `com.mysql.cj.jdbc.Driver` " +
        "(fallback `com.mysql.jdbc.Driver`).</li>" +
        " <li>POSTGRESQL: PostgreSQL, JDBC driver `org.postgresql.Driver`.</li>" +
        " <li>CUSTOM: User-defined database type, need to specify corresponding JDBC driver.</li>" +
        " Note that: The JDBC datasource is powered by HiKariCP, for datasource properties," +
        " please specify them with the prefix: kyuubi.metadata.store.jdbc.datasource." +
        " For example, kyuubi.metadata.store.jdbc.datasource.connectionTimeout=10000.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .transformToUpperCase
      .createWithDefault("SQLITE")

  val METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT: ConfigEntry[Boolean] =
    buildConf("kyuubi.metadata.store.jdbc.database.schema.init")
      .doc("Whether to init the JDBC metadata store database schema.")
      .version("1.6.0")
      .serverOnly
      .booleanConf
      .createWithDefault(true)

  val METADATA_STORE_JDBC_DRIVER: OptionalConfigEntry[String] =
    buildConf("kyuubi.metadata.store.jdbc.driver")
      .doc("JDBC driver class name for server jdbc metadata store.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .createOptional

  val METADATA_STORE_JDBC_URL: ConfigEntry[String] =
    buildConf("kyuubi.metadata.store.jdbc.url")
      .doc("The JDBC url for server JDBC metadata store. By default, it is a SQLite database " +
        "url, and the state information is not shared across Kyuubi instances. To enable high " +
        "availability for multiple kyuubi instances, please specify a production JDBC url. " +
        "Note: this value support the variables substitution: `{{KYUUBI_HOME}}`, " +
        "`{{KYUUBI_WORK_DIR_ROOT}}`.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .createWithDefault("jdbc:sqlite:{{KYUUBI_HOME}}/kyuubi_state_store.db")

  val METADATA_STORE_JDBC_USER: ConfigEntry[String] =
    buildConf("kyuubi.metadata.store.jdbc.user")
      .doc("The username for server JDBC metadata store.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .createWithDefault("")

  val METADATA_STORE_JDBC_PASSWORD: ConfigEntry[String] =
    buildConf("kyuubi.metadata.store.jdbc.password")
      .doc("The password for server JDBC metadata store.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .createWithDefault("")

  val METADATA_STORE_JDBC_PRIORITY_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.metadata.store.jdbc.priority.enabled")
      .doc("Whether to enable the priority scheduling for batch impl v2. " +
        "When false, ignore kyuubi.batch.priority and use the FIFO ordering strategy " +
        "for batch job scheduling. Note: this feature may cause significant performance issues " +
        "when using MySQL 5.7 as the metastore backend due to the lack of support " +
        "for mixed order index. See more details at KYUUBI #5329.")
      .version("1.8.0")
      .serverOnly
      .booleanConf
      .createWithDefault(false)
}
