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
-- using default substitutions

select
    l_returnflag,
    l_linestatus,
    round(sum(l_quantity), 2) as sum_qty,
    round(sum(l_extendedprice), 2) as sum_base_price,
    round(sum(l_extendedprice * (1 - l_discount)), 2) as sum_disc_price,
    round(sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)), 2) as sum_charge,
    round(avg(l_quantity), 2) as avg_qty,
    round(avg(l_extendedprice), 2) as avg_price,
    round(avg(l_discount), 2) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
