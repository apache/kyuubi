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

package org.apache.kyuubi.event.logger

import org.apache.kyuubi.config.{ConfigEntry, OptionalConfigEntry}
import org.apache.kyuubi.config.KyuubiConf.buildConf

object ElasticSearchEventLoggerConf {

  val ELASTICSEARCH_EVENT_LOGGER_HOSTS: ConfigEntry[String] =
    buildConf("event.logger.elasticsearch.hosts")
      .doc("The elasticsearch hosts of Elasticsearch event logger.")
      .version("1.5.0")
      .stringConf
      .createWithDefault("127.0.0.1:9200")

  val ELASTICSEARCH_EVENT_LOGGER_USERNAME: OptionalConfigEntry[String] =
    buildConf("event.logger.elasticsearch.username")
      .doc("The elasticsearch username of Elasticsearch event logger.")
      .version("1.5.0")
      .stringConf
      .createOptional

  val ELASTICSEARCH_EVENT_LOGGER_PASSWORD: OptionalConfigEntry[String] =
    buildConf("event.logger.elasticsearch.password")
      .doc("The elasticsearch password of Elasticsearch event logger.")
      .version("1.5.0")
      .stringConf
      .createOptional

  val ELASTICSEARCH_EVENT_LOGGER_INDICES: OptionalConfigEntry[String] =
    buildConf("event.logger.elasticsearch.indices")
      .doc("The event indices of Elasticsearch event logger. " +
        "All events use the same index, like:'kyuubi-events', " +
        "or different events can use different indexes, " +
        "like:'kyuubi-events,kyuubi_session:kyuubi-sessions,kyuubi_operation:kyuubi-operations'.")
      .version("1.5.0")
      .stringConf
      .createOptional

}
