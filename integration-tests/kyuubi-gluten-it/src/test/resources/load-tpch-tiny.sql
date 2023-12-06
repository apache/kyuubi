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

CREATE DATABASE IF NOT EXISTS spark_catalog.tpch_tiny;

USE spark_catalog.tpch_tiny;

--
-- Name: customer; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS customer USING parquet AS SELECT * FROM tpch.tiny.customer;

--
-- Name: orders; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS orders USING parquet AS SELECT * FROM tpch.tiny.orders;

--
-- Name: lineitem; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS lineitem USING parquet AS SELECT * FROM tpch.tiny.lineitem;

--
-- Name: part; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS part USING parquet AS SELECT * FROM tpch.tiny.part;

--
-- Name: partsupp; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS partsupp USING parquet AS SELECT * FROM tpch.tiny.partsupp;

--
-- Name: supplier; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS supplier USING parquet AS SELECT * FROM tpch.tiny.supplier;

--
-- Name: nation; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS nation USING parquet AS SELECT * FROM tpch.tiny.nation;

--
-- Name: region; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS region USING parquet AS SELECT * FROM tpch.tiny.region;
