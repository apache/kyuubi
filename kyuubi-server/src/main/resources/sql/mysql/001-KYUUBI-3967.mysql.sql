--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

SELECT '< KYUUBI-3967: Shorten column varchar length of metadata table >' AS ' ';

ALTER TABLE metadata MODIFY COLUMN session_type varchar(32) NOT NULL COMMENT 'the session type, SQL or BATCH';
ALTER TABLE metadata MODIFY COLUMN real_user varchar(255) NOT NULL COMMENT 'the real user';
ALTER TABLE metadata MODIFY COLUMN user_name varchar(255) NOT NULL COMMENT 'the user name, might be a proxy user';
ALTER TABLE metadata MODIFY COLUMN ip_address varchar(128) COMMENT 'the client ip address';
ALTER TABLE metadata MODIFY COLUMN engine_type varchar(32) NOT NULL COMMENT 'the engine type';
ALTER TABLE metadata MODIFY COLUMN engine_state varchar(32) COMMENT 'the engine application state';

DROP INDEX kyuubi_instance_index on metadata;
