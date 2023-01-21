/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.server.api

import org.glassfish.jersey.media.multipart.MultiPartFeature
import org.glassfish.jersey.server.ResourceConfig

import org.apache.kyuubi.server.api.v1.KyuubiOpenApiResource

class OpenAPIConfig extends ResourceConfig {
  packages("org.apache.kyuubi.server.api.v1")
  register(classOf[KyuubiOpenApiResource])
  register(classOf[KyuubiScalaObjectMapper])
  register(classOf[RestExceptionMapper])
  register(classOf[MultiPartFeature])
}
