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

select
  count(*) as total,
  count(ss_sold_date_sk) as not_null_total,
  --count(distinct ss_sold_date_sk) as unique_days,
  max(ss_sold_date_sk) as max_ss_sold_date_sk,
  max(ss_sold_time_sk) as max_ss_sold_time_sk,
  max(ss_item_sk) as max_ss_item_sk,
  max(ss_customer_sk) as max_ss_customer_sk,
  max(ss_cdemo_sk) as max_ss_cdemo_sk,
  max(ss_hdemo_sk) as max_ss_hdemo_sk,
  max(ss_addr_sk) as max_ss_addr_sk,
  max(ss_store_sk) as max_ss_store_sk,
  max(ss_promo_sk) as max_ss_promo_sk
from store_sales
