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

package org.apache.kyuubi.util

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.client.{Config, ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import io.fabric8.kubernetes.client.Config.autoConfigure
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory
import okhttp3.{Dispatcher, OkHttpClient}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

object KubernetesUtils extends Logging {

  def buildKubernetesClient(conf: KyuubiConf): Option[KubernetesClient] = {
    val master = conf.get(KUBERNETES_MASTER)
    val namespace = conf.get(KUBERNETES_NAMESPACE)
    val serviceAccountToken =
      Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)).filter(_.exists)
    val serviceAccountCaCrt =
      Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)).filter(_.exists)

    val oauthTokenFile = conf.get(KUBERNETES_AUTHENTICATE_OAUTH_TOKEN_FILE)
      .map(new File(_))
      .orElse(serviceAccountToken)
    val oauthTokenValue = conf.get(KUBERNETES_AUTHENTICATE_OAUTH_TOKEN)

    KubernetesUtils.requireNandDefined(
      oauthTokenFile,
      oauthTokenValue,
      s"Cannot specify OAuth token through both a oauth token file and a " +
        s"oauth token value.")

    val caCertFile = conf
      .get(KUBERNETES_AUTHENTICATE_CA_CERT_FILE)
      .orElse(serviceAccountCaCrt.map(_.getAbsolutePath))
    val clientKeyFile = conf.get(KUBERNETES_AUTHENTICATE_CLIENT_KEY_FILE)
    val clientCertFile = conf.get(KUBERNETES_AUTHENTICATE_CLIENT_CERT_FILE)

    // Allow for specifying a context used to auto-configure from the users K8S config file
    val kubeContext = conf.get(KUBERNETES_CONTEXT).filter(_.nonEmpty)
    info("Auto-configuring K8S client using " +
      kubeContext.map("context " + _).getOrElse("current context") +
      " from users K8S config file")

    val config = new ConfigBuilder(autoConfigure(kubeContext.orNull))
      .withApiVersion("v1")
      .withOption(master) { (master, configBuilder) =>
        configBuilder.withMasterUrl(master)
      }.withNamespace(namespace)
      .withTrustCerts(conf.get(KUBERNETES_TRUST_CERTIFICATES))
      .withOption(oauthTokenValue) { (token, configBuilder) =>
        configBuilder.withOauthToken(token)
      }.withOption(oauthTokenFile) { (file, configBuilder) =>
        configBuilder.withOauthToken(Files.asCharSource(file, Charsets.UTF_8).read())
      }.withOption(caCertFile) { (file, configBuilder) =>
        configBuilder.withCaCertFile(file)
      }.withOption(clientKeyFile) { (file, configBuilder) =>
        configBuilder.withClientKeyFile(file)
      }.withOption(clientCertFile) { (file, configBuilder) =>
        configBuilder.withClientCertFile(file)
      }.build()

    // https://github.com/fabric8io/kubernetes-client/issues/3547
    val dispatcher = new Dispatcher(
      ThreadUtils.newDaemonCachedThreadPool("kubernetes-dispatcher"))
    val factoryWithCustomDispatcher = new OkHttpClientFactory() {
      override protected def additionalConfig(builder: OkHttpClient.Builder): Unit = {
        builder.dispatcher(dispatcher)
      }
    }

    debug("Kubernetes client config: " +
      new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(config))
    Some(new DefaultKubernetesClient(factoryWithCustomDispatcher.createHttpClient(config), config))
  }

  implicit private class OptionConfigurableConfigBuilder(val configBuilder: ConfigBuilder)
    extends AnyVal {

    def withOption[T](option: Option[T])(configurator: ((T, ConfigBuilder) => ConfigBuilder))
        : ConfigBuilder = {
      option.map { opt =>
        configurator(opt, configBuilder)
      }.getOrElse(configBuilder)
    }
  }

  def requireNandDefined(opt1: Option[_], opt2: Option[_], errMessage: String): Unit = {
    opt1.foreach { _ => require(opt2.isEmpty, errMessage) }
    opt2.foreach { _ => require(opt1.isEmpty, errMessage) }
  }
}
