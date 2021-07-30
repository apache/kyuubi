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

SELECT 1 AS `excess discount amount `
FROM
  catalog_sales, item, date_dim
WHERE
  i_manufact_id = 977
    AND i_item_sk = cs_item_sk
    AND d_date BETWEEN '2000-01-27' AND (cast('2000-01-27' AS DATE) + interval 90 days)
    AND d_date_sk = cs_sold_date_sk
    AND cs_ext_discount_amt > (
    SELECT 1.3 * avg(cs_ext_discount_amt)
    FROM catalog_sales, date_dim
    WHERE cs_item_sk = i_item_sk
      AND d_date BETWEEN '2000-01-27' AND (cast('2000-01-27' AS DATE) + interval 90 days)
      AND d_date_sk = cs_sold_date_sk)
LIMIT 100
