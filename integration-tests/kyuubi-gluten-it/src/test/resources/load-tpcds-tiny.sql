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

CREATE DATABASE IF NOT EXISTS spark_catalog.tpcds_tiny;

USE spark_catalog.tpcds_tiny;

--
-- Name: catalog_sales; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS catalog_sales USING parquet PARTITIONED BY (cs_sold_date_sk)
AS SELECT * FROM tpcds.tiny.catalog_sales;

--
-- Name: catalog_returns; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS catalog_returns USING parquet PARTITIONED BY (cr_returned_date_sk)
AS SELECT * FROM tpcds.tiny.catalog_returns;

--
-- Name: inventory; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS inventory USING parquet PARTITIONED BY (inv_date_sk)
AS SELECT * FROM tpcds.tiny.inventory;

--
-- Name: store_sales; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS store_sales USING parquet PARTITIONED BY (ss_sold_date_sk)
AS SELECT * FROM tpcds.tiny.store_sales;

--
-- Name: store_returns; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS store_returns USING parquet PARTITIONED BY (sr_returned_date_sk)
AS SELECT * FROM tpcds.tiny.store_returns;

--
-- Name: web_sales; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS web_sales USING parquet PARTITIONED BY (ws_sold_date_sk)
AS SELECT * FROM tpcds.tiny.web_sales;

--
-- Name: web_returns; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS web_returns USING parquet PARTITIONED BY (wr_returned_date_sk)
AS SELECT * FROM tpcds.tiny.web_returns;

--
-- Name: call_center; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS call_center USING parquet AS SELECT * FROM tpcds.tiny.call_center;

--
-- Name: catalog_page; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS catalog_page USING parquet AS SELECT * FROM tpcds.tiny.catalog_page;

--
-- Name: customer; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS customer USING parquet AS SELECT * FROM tpcds.tiny.customer;

--
-- Name: customer_address; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS customer_address USING parquet AS SELECT * FROM tpcds.tiny.customer_address;

--
-- Name: customer_demographics; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS customer_demographics USING parquet AS SELECT * FROM tpcds.tiny.customer_demographics;

--
-- Name: date_dim; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS date_dim USING parquet AS SELECT * FROM tpcds.tiny.date_dim;

--
-- Name: household_demographics; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS household_demographics USING parquet AS SELECT * FROM tpcds.tiny.household_demographics;

--
-- Name: income_band; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS income_band USING parquet AS SELECT * FROM tpcds.tiny.income_band;

--
-- Name: item; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS item USING parquet AS SELECT * FROM tpcds.tiny.item;

--
-- Name: promotion; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS promotion USING parquet AS SELECT * FROM tpcds.tiny.promotion;

--
-- Name: reason; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS reason USING parquet AS SELECT * FROM tpcds.tiny.reason;

--
-- Name: ship_mode; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS ship_mode USING parquet AS SELECT * FROM tpcds.tiny.ship_mode;

--
-- Name: store; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS store USING parquet AS SELECT * FROM tpcds.tiny.store;

--
-- Name: time_dim; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS time_dim USING parquet AS SELECT * FROM tpcds.tiny.time_dim;

--
-- Name: warehouse; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS warehouse USING parquet AS SELECT * FROM tpcds.tiny.warehouse;

--
-- Name: web_page; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS web_page USING parquet AS SELECT * FROM tpcds.tiny.web_page;

--
-- Name: web_site; Type: TABLE; Tablespace:
--
CREATE TABLE IF NOT EXISTS web_site USING parquet AS SELECT * FROM tpcds.tiny.web_site;
