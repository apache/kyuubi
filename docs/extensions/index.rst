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

Extensions
==========

Besides the base use case, Kyuubi also has some extension points for
extending use cases. By defining plugin of your own or applying
third-party ones, kyuubi allows to run your plugin's functionality at
the specific point.

The extension points can be divided into server side extensions and
engine side extensions.

Server side extensions are applied by kyuubi administrators to extend the
ability of kyuubi servers.

Engine side extensions are applied to kyuubi engines, some of them can be
managed by administrators, some of them can be applied by end-users dynamically
at runtime.


.. toctree::
    :maxdepth: 2

    Server Side Extensions <server/index>
    Engine Side Extensions <engines/index>
