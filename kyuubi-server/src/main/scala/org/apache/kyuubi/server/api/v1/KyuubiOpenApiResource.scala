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

package org.apache.kyuubi.server.api.v1

import javax.servlet.ServletConfig
import javax.ws.rs.{GET, Path, PathParam, Produces}
import javax.ws.rs.core.{Application, Context, HttpHeaders, MediaType, Response, UriInfo}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import io.swagger.v3.jaxrs2.integration.JaxrsOpenApiContextBuilder
import io.swagger.v3.jaxrs2.integration.resources.BaseOpenApiResource
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.integration.api.OpenApiContext
import io.swagger.v3.oas.models.{Components, OpenAPI}
import io.swagger.v3.oas.models.info.{Contact, Info, License}
import io.swagger.v3.oas.models.security.{SecurityRequirement, SecurityScheme}
import io.swagger.v3.oas.models.servers.Server
import io.swagger.v3.oas.models.tags.Tag
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.server.api.ApiRequestContext

@Path("/openapi.{type:json|yaml}")
class KyuubiOpenApiResource extends BaseOpenApiResource with ApiRequestContext {
  @Context
  protected var config: ServletConfig = _

  @Context
  protected var app: Application = _

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON, "application/yaml"))
  @Operation(hidden = true)
  def getOpenApi(
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo,
      @PathParam("type") tpe: String): Response = {

    val ctxId = getContextId(config)
    val ctx: OpenApiContext = new KyuubiJaxrsOpenApiContextBuilder()
      .servletConfig(config)
      .application(app)
      .resourcePackages(resourcePackages)
      .configLocation(configLocation)
      .openApiConfiguration(openApiConfiguration)
      .ctxId(ctxId)
      .buildContext(true)

    val openApi = setKyuubiOpenAPIDefinition(ctx.read())

    if (StringUtils.isNotBlank(tpe) && tpe.trim().equalsIgnoreCase("yaml")) {
      Response.status(Response.Status.OK)
        .entity(
          ctx.getOutputYamlMapper()
            .writer(new DefaultPrettyPrinter())
            .writeValueAsString(openApi))
        .`type`("application/yaml")
        .build()
    } else {
      Response.status(Response.Status.OK)
        .entity(
          ctx.getOutputJsonMapper
            .writer(new DefaultPrettyPrinter())
            .writeValueAsString(openApi))
        .`type`(MediaType.APPLICATION_JSON_TYPE)
        .build()
    }
  }

  private def setKyuubiOpenAPIDefinition(openApi: OpenAPI): OpenAPI = {
    // TODO: to improve when https is enabled.
    val apiUrl = s"http://${fe.connectionUrl}/api"
    openApi.info(
      new Info().title("Apache Kyuubi REST API Documentation")
        .version(org.apache.kyuubi.KYUUBI_VERSION)
        .description("Apache Kyuubi REST API Documentation")
        .contact(
          new Contact().name("Apache Kyuubi Community")
            .url("https://kyuubi.apache.org/issue_tracking.html")
            .email("dev@kyuubi.apache.org"))
        .license(
          new License().name("Apache License 2.0")
            .url("https://www.apache.org/licenses/LICENSE-2.0.txt")))
      .tags(List(new Tag().name("Session"), new Tag().name("Operation")).asJava)
      .servers(List(new Server().url(apiUrl)).asJava)
      .components(Option(openApi.getComponents).getOrElse(new Components())
        .addSecuritySchemes(
          "BasicAuth",
          new SecurityScheme()
            .`type`(SecurityScheme.Type.HTTP)
            .scheme("Basic"))
        .addSecuritySchemes(
          "BearerAuth",
          new SecurityScheme()
            .`type`(SecurityScheme.Type.HTTP)
            .scheme("Bearer")
            .bearerFormat("JWT")))
      .security(List(new SecurityRequirement()
        .addList("BasicAuth")
        .addList("BearerAuth")).asJava)
  }
}

class KyuubiJaxrsOpenApiContextBuilder
  extends JaxrsOpenApiContextBuilder[KyuubiJaxrsOpenApiContextBuilder]
