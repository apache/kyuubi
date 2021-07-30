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

-- This is a new query in TPCDS v2.7
select
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  count(*) cnt1,
  avg(cd_dep_count),
  max(cd_dep_count),
  sum(cd_dep_count),
  cd_dep_employed_count,
  count(*) cnt2,
  avg(cd_dep_employed_count),
  max(cd_dep_employed_count),
  sum(cd_dep_employed_count),
  cd_dep_college_count,
  count(*) cnt3,
  avg(cd_dep_college_count),
  max(cd_dep_college_count),
  sum(cd_dep_college_count)
from
  customer c, customer_address ca, customer_demographics
where
  c.c_current_addr_sk = ca.ca_address_sk
    and cd_demo_sk = c.c_current_cdemo_sk
    and exists (
        select *
        from store_sales, date_dim
        where c.c_customer_sk = ss_customer_sk
          and ss_sold_date_sk = d_date_sk
          and d_year = 1999
          and d_qoy < 4)
    and exists (
        select *
        from (
            select ws_bill_customer_sk customsk
            from web_sales, date_dim
            where ws_sold_date_sk = d_date_sk
              and d_year = 1999
              and d_qoy < 4
        union all
        select cs_ship_customer_sk customsk
        from catalog_sales, date_dim
        where cs_sold_date_sk = d_date_sk
          and d_year = 1999
          and d_qoy < 4) x
        where x.customsk = c.c_customer_sk)
group by
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
order by
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
limit 100
