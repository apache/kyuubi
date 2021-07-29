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

SELECT sum(ss_quantity)
FROM store_sales, store, customer_demographics, customer_address, date_dim
WHERE s_store_sk = ss_store_sk
  AND ss_sold_date_sk = d_date_sk AND d_year = 2001
  AND
  (
    (
      cd_demo_sk = ss_cdemo_sk
        AND
        cd_marital_status = 'M'
        AND
        cd_education_status = '4 yr Degree'
        AND
        ss_sales_price BETWEEN 100.00 AND 150.00
    )
      OR
      (
        cd_demo_sk = ss_cdemo_sk
          AND
          cd_marital_status = 'D'
          AND
          cd_education_status = '2 yr Degree'
          AND
          ss_sales_price BETWEEN 50.00 AND 100.00
      )
      OR
      (
        cd_demo_sk = ss_cdemo_sk
          AND
          cd_marital_status = 'S'
          AND
          cd_education_status = 'College'
          AND
          ss_sales_price BETWEEN 150.00 AND 200.00
      )
  )
  AND
  (
    (
      ss_addr_sk = ca_address_sk
        AND
        ca_country = 'United States'
        AND
        ca_state IN ('CO', 'OH', 'TX')
        AND ss_net_profit BETWEEN 0 AND 2000
    )
      OR
      (ss_addr_sk = ca_address_sk
        AND
        ca_country = 'United States'
        AND
        ca_state IN ('OR', 'MN', 'KY')
        AND ss_net_profit BETWEEN 150 AND 3000
      )
      OR
      (ss_addr_sk = ca_address_sk
        AND
        ca_country = 'United States'
        AND
        ca_state IN ('VA', 'CA', 'MS')
        AND ss_net_profit BETWEEN 50 AND 25000
      )
  )
