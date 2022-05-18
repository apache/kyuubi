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

create table if not exists batch_state(
    id varchar(128) not null,
    batch_type varchar(1024) not null,
    batch_owner varchar(1024) not null,
    kyuubi_instance varchar(1024) not null,
    state varchar(128) not null,
    create_time BIGINT not null,
    app_id varchar(128),
    app_name varchar(1024),
    app_url varchar(1024),
    app_state varchar(128),
    app_error mediumtext,
    end_time BIGINT,
    PRIMARY KEY id
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


create table if not exists batch_meta(
    batch_id varchar(128) not null,
    session_conf mediumtext,
    batch_type varchar(1024),
    resource varchar(1024),
    className varchar(1024),
    name varchar(1024),
    conf mediumtext,
    args mediumtext,
    PRIMARY KEY batch_id
) ENGINE=InnoDB DEFAULT CHARSET=latin1;;
