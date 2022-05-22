--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE TABLE IF NOT EXISTS BATCH_STATE(
    `KEY_ID` bigint PRIMARY KEY AUTO_INCREMENT,
    `ID` varchar(128) NOT NULL,
    `BATCH_TYPE` varchar(1024) NOT NULL,
    `BATCH_OWNER` varchar(1024) NOT NULL,
    `KYUUBI_INSTANCE` varchar(1024) NOT NULL,
    `STATE` varchar(128) NOT NULL,
    `create_time` BIGINT NOT NULL,
    `app_id` varchar(128),
    `app_name` varchar(1024),
    `app_url` varchar(1024),
    `app_state` varchar(128),
    `app_error` mediumtext,
    `end_time` bigint
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS BATCH_META(
    `BATCHID` varchar(128) NOT NULL,
    `SESSION_CONF` mediumtext,
    `BATCH_TYPE` varchar(1024),
    `RESOURCE` varchar(1024),
    `CLASS_NAME` varchar(1024),
    `NAME` varchar(1024),
    `CONF` mediumtext,
    `ARGS` mediumtext,
    PRIMARY KEY (`BATCH_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;;
