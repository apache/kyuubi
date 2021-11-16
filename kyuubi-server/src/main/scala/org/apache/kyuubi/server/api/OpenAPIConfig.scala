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

import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource
import io.swagger.v3.oas.annotations.OpenAPIDefinition
import io.swagger.v3.oas.annotations.info.{Contact, Info, License}
import io.swagger.v3.oas.annotations.tags.Tag
import org.glassfish.jersey.server.ResourceConfig

@OpenAPIDefinition(
  info = new Info(
    title = "Apache Kyuubi REST API Documentation",
    version = "1.4.0",
    description = "Apache Kyuubi REST API Documentation",
    contact = new Contact(
      name = "Apache Kyuubi Community",
      url = "https://kyuubi.apache.org/issue_tracking.html",
      email = "dev@kyuubi.apache.org"),
    license = new License(
      name = "Apache 2.0",
      url = "https://www.apache.org/licenses/LICENSE-2.0.html")
  ),
  tags = Array(new Tag(name = "Session"))
)
class OpenAPIConfig extends ResourceConfig {
  packages("org.apache.kyuubi.server.api.v1")
  register(classOf[OpenApiResource]);
  register(classOf[KyuubiScalaObjectMapper])
}
