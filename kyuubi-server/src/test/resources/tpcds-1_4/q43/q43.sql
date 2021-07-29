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

SELECT
  s_store_name,
  s_store_id,
  sum(CASE WHEN (d_day_name = 'Sunday')
    THEN ss_sales_price
      ELSE NULL END) sun_sales,
  sum(CASE WHEN (d_day_name = 'Monday')
    THEN ss_sales_price
      ELSE NULL END) mon_sales,
  sum(CASE WHEN (d_day_name = 'Tuesday')
    THEN ss_sales_price
      ELSE NULL END) tue_sales,
  sum(CASE WHEN (d_day_name = 'Wednesday')
    THEN ss_sales_price
      ELSE NULL END) wed_sales,
  sum(CASE WHEN (d_day_name = 'Thursday')
    THEN ss_sales_price
      ELSE NULL END) thu_sales,
  sum(CASE WHEN (d_day_name = 'Friday')
    THEN ss_sales_price
      ELSE NULL END) fri_sales,
  sum(CASE WHEN (d_day_name = 'Saturday')
    THEN ss_sales_price
      ELSE NULL END) sat_sales
FROM date_dim, store_sales, store
WHERE d_date_sk = ss_sold_date_sk AND
  s_store_sk = ss_store_sk AND
  s_gmt_offset = -5 AND
  d_year = 2000
GROUP BY s_store_name, s_store_id
ORDER BY s_store_name, s_store_id, sun_sales, mon_sales, tue_sales, wed_sales,
  thu_sales, fri_sales, sat_sales
LIMIT 100
