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

--q44.sql--

 select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
 from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 4
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 4
                                                    and ss_addr_sk is null
                                                  group by ss_store_sk))V1)V11
     where rnk  < 11) asceding,
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 4
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 4
                                                    and ss_addr_sk is null
                                                  group by ss_store_sk))V2)V21
     where rnk  < 11) descending,
 item i1, item i2
 where asceding.rnk = descending.rnk
   and i1.i_item_sk=asceding.item_sk
   and i2.i_item_sk=descending.item_sk
 order by asceding.rnk
 limit 100
            
