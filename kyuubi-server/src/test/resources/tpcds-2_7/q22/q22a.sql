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
with results as (
    select
      i_product_name,
      i_brand,
      i_class,
      i_category,
      avg(inv_quantity_on_hand) qoh
    from
      inventory, date_dim, item, warehouse
    where
      inv_date_sk = d_date_sk
        and inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and d_month_seq between 1212 and 1212 + 11
    group by
      i_product_name,
      i_brand,
      i_class,
      i_category),
results_rollup as (
    select
      i_product_name,
      i_brand,
      i_class,
      i_category,
      avg(qoh) qoh
    from
      results
    group by
      i_product_name,
      i_brand,
      i_class,
      i_category
    union all
    select
      i_product_name,
      i_brand,
      i_class,
      null i_category,
      avg(qoh) qoh
    from
      results
    group by
      i_product_name,
      i_brand,
      i_class
    union all
    select
      i_product_name,
      i_brand,
      null i_class,
      null i_category,
      avg(qoh) qoh
    from
      results
    group by
      i_product_name,
      i_brand
    union all
    select
      i_product_name,
      null i_brand,
      null i_class,
      null i_category,
      avg(qoh) qoh
    from
      results
    group by
      i_product_name
    union all
    select
      null i_product_name,
      null i_brand,
      null i_class,
      null i_category,
      avg(qoh) qoh
    from
      results)
select
  i_product_name,
  i_brand,
  i_class,
  i_category,
  qoh
from
  results_rollup
order by
  qoh,
  i_product_name,
  i_brand,
  i_class,
  i_category
limit 100
