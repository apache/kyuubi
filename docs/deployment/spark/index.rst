.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

.. image:: ../../imgs/kyuubi_logo.png
   :align: center

The Engine Configuration Guide
==============================

Kyuubi aims to bring Spark to end-users who need not qualify with Spark or something else related to the big data area.
End-users can write SQL queries through JDBC against Kyuubi and nothing more.
The Kyuubi server-side or the corresponding engines could do most of the optimization.
On the other hand, we don't wholly restrict end-users to special handling of specific cases to benefit from the following documentations.
Even if you don't use Kyuubi, as a simple Spark user, I'm sure you'll find the next articles instructive.

.. toctree::
    :maxdepth: 2
    :numbered: 2
    :glob:

    dynamic_allocation
    aqe
