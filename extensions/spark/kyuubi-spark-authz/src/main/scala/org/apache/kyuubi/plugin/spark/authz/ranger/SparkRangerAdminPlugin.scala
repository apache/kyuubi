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

package org.apache.kyuubi.plugin.spark.authz.ranger

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.ranger.authz.api.{RangerAuthorizer, RangerAuthorizerFactory}
import org.apache.ranger.authz.model._
import org.apache.ranger.authz.model.RangerAccessContext.CONTEXT_INFO_CLUSTER_NAME
import org.apache.ranger.authz.model.RangerAuthzResult.AccessDecision
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.ObjectType
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType._

/**
 * The entry point of Ranger authorization for Spark SQL, which delegates
 * authorization requests to a Ranger authorizer:
 * <ul>
 *   <li>org.apache.ranger.authz.remote.RangerRemoteAuthorizer (default):
 *       sends requests to a Ranger PDP server via REST APIs, with a thin client
 *       that does not download policies to the client side.</li>
 *   <li>org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer:
 *       evaluates requests locally against the policies pulled from
 *       the Ranger admin server, as the previous Ranger plugin did.</li>
 * </ul>
 *
 * The authorizer implementation is selected by `ranger.authorizer.impl.class`.
 */
object SparkRangerAdminPlugin {
  final private val LOG = LoggerFactory.getLogger(getClass)

  /**
   * The service type of the Spark SQL service definition registered in
   * Ranger admin.
   */
  final val SERVICE_TYPE: String = "spark"

  final private val APP_TYPE: String = "sparkSql"

  final private val KEY_IMPL_CLASS = "ranger.authorizer.impl.class"
  final private val KEY_PDP_URL = "ranger.authz.remote.pdp.url"
  final private val KEY_SERVICE_NAME = s"ranger.plugin.$SERVICE_TYPE.service.name"
  final private val KEY_CLUSTER_NAME = s"ranger.plugin.$SERVICE_TYPE.access.cluster.name"
  final private val KEY_AUTHORIZE_IN_SINGLE_CALL =
    s"ranger.plugin.$SERVICE_TYPE.authorize.in.single.call"

  final private val REMOTE_AUTHORIZER_CLASS: String =
    classOf[org.apache.ranger.authz.remote.RangerRemoteAuthorizer].getName

  /**
   * The security-critical configurations which are read from the Ranger configuration
   * resources only: JVM system properties can neither set nor override them, so a tenant
   * who can control the JVM options of the engine process cannot point the authorization
   * to a server they control or bypass the policies.
   */
  final private val ADMIN_ONLY_KEYS: Seq[String] =
    Seq(KEY_IMPL_CLASS, KEY_PDP_URL, KEY_SERVICE_NAME)

  final private val ADMIN_ONLY_KEY_PREFIXES: Seq[String] = Seq(
    "ranger.authz.remote.authn.",
    "ranger.authz.remote.header.",
    "ranger.authz.remote.ssl.")

  final private val CONFIG_PREFIXES = Seq("ranger.", "xasecure.")

  final private val CONFIG_RESOURCES = Seq(
    s"ranger-$SERVICE_TYPE-security.xml",
    s"ranger-$SERVICE_TYPE-audit.xml")

  private var authorizer: RangerAuthorizer = null

  /**
   * The Ranger configurations loaded from the Hadoop configuration resources,
   * e.g. ranger-spark-security.xml.
   */
  private[ranger] var config: Configuration = null

  /**
   * For a Spark SQL query, it may contain 0 or more privilege objects to verify, e.g. a typical
   * JOIN operator may have two tables and their columns to verify.
   *
   * This configuration controls whether to verify the privilege objects in single call or
   * to verify them one by one.
   */
  def authorizeInSingleCall: Boolean = config.getBoolean(KEY_AUTHORIZE_IN_SINGLE_CALL, false)

  def getServiceType: String = SERVICE_TYPE

  private def serviceName: String = config.get(KEY_SERVICE_NAME)

  /**
   * authorizer initialization
   * with cleanup shutdown hook registered
   */
  def initialize(): Unit = {
    ensureConfig()
    val props = new Properties
    config.iterator.asScala
      .filter(entry => CONFIG_PREFIXES.exists(entry.getKey.startsWith))
      .foreach(entry => props.put(entry.getKey, entry.getValue))
    System.getProperties.asScala
      .filter { case (key, _) =>
        CONFIG_PREFIXES.exists(key.startsWith) && !isAdminOnlyKey(key)
      }
      .foreach { case (key, value) => props.put(key, value) }
    validateAdminOnlyKeys(props)
    initialize(props)
  }

  private def isAdminOnlyKey(key: String): Boolean =
    ADMIN_ONLY_KEYS.contains(key) || ADMIN_ONLY_KEY_PREFIXES.exists(key.startsWith)

  /**
   * Fails fast when the Ranger configuration resources lack a configuration the
   * authorizer needs: the authorizer implementation defaults to the Ranger PDP mode,
   * which requires the Ranger PDP server address. The configuration cannot fall back
   * to JVM system properties, which tenant users may control.
   */
  private def validateAdminOnlyKeys(props: Properties): Unit = {
    val implClass = props.getProperty(KEY_IMPL_CLASS, REMOTE_AUTHORIZER_CLASS)
    if (implClass == REMOTE_AUTHORIZER_CLASS && props.getProperty(KEY_PDP_URL) == null) {
      throw new IllegalArgumentException(
        s"$KEY_PDP_URL must be configured in ${CONFIG_RESOURCES.mkString(", ")} and " +
          "cannot be set by JVM system properties")
    }
  }

  private def ensureConfig(): Unit = {
    if (config == null) {
      config = new Configuration
      CONFIG_RESOURCES.foreach(addResourceIfReadable)
    }
  }

  private def addResourceIfReadable(resource: String): Unit = {
    val loader = Thread.currentThread().getContextClassLoader
    val url = if (loader != null) loader.getResource(resource) else null
    if (url != null) {
      config.addResource(url)
    } else {
      config.addResource(resource)
    }
  }

  private[ranger] def initialize(props: Properties): Unit = synchronized {
    if (authorizer == null) {
      props.put("ranger.authz.app.type", APP_TYPE)
      authorizer = RangerAuthorizerFactory.createAuthorizer(props)
      authorizer.init()
      registerCleanupShutdownHook(authorizer)
      LOG.info(
        s"initialized ranger authorizer, service: $serviceName, " +
          s"impl: ${authorizer.getClass.getName}")
    }
  }

  /**
   * Reset the authorizer to the uninitialized state, so that the next
   * [[initialize]] call re-creates it. Intended for tests only.
   */
  private[ranger] def reset(): Unit = synchronized {
    if (authorizer != null) {
      try authorizer.close()
      catch {
        case e: Exception => LOG.warn("failed to close ranger authorizer", e)
      }
      authorizer = null
    }
  }

  private def registerCleanupShutdownHook(authorizer: RangerAuthorizer): Unit = {
    ShutdownHookManager.get().addShutdownHook(
      () => {
        if (authorizer != null) {
          LOG.info(s"clean up ranger authorizer, impl: ${authorizer.getClass.getName}")
          try authorizer.close()
          catch {
            case e: Exception => LOG.warn("failed to close ranger authorizer", e)
          }
        }
      },
      Integer.MAX_VALUE)
  }

  private def checkInitialized(): RangerAuthorizer = {
    if (authorizer == null) {
      initialize()
    }
    authorizer
  }

  private def userInfo(req: AccessRequest): RangerUserInfo =
    new RangerUserInfo(req.user, null, req.userGroups.asJava, null)

  private def permissionOf(accessType: AccessType): String = accessType match {
    // the Ranger any-access marker (RangerPolicyEngine.ANY_ACCESS) allows any access
    // type matched by policies, e.g. for SHOW commands
    case USE => "_any"
    case _ => accessType.toString.toLowerCase
  }

  private def context(): RangerAccessContext = {
    val additionalInfo = new java.util.HashMap[String, AnyRef]
    val clusterName = config.get(KEY_CLUSTER_NAME)
    if (clusterName != null) {
      additionalInfo.put(CONTEXT_INFO_CLUSTER_NAME, clusterName)
    }
    val ctx = new RangerAccessContext(SERVICE_TYPE, serviceName)
    ctx.setAccessTime(System.currentTimeMillis)
    ctx.setAdditionalInfo(additionalInfo)
    ctx
  }

  private def toAuthzRequest(
      req: AccessRequest,
      resourceInfo: RangerResourceInfo): RangerAuthzRequest = {
    val access = new RangerAccessInfo(
      resourceInfo,
      req.opType.toString,
      java.util.Collections.singleton(permissionOf(req.accessType)))
    new RangerAuthzRequest(userInfo(req), access, context())
  }

  private def toAuthzRequest(req: AccessRequest): RangerAuthzRequest =
    toAuthzRequest(req, req.resource.toResourceInfos.head)

  /**
   * batch verifying RangerAccessRequests
   * and throws exception with all disallowed privileges
   * for accessType and resources
   */
  def verify(requests: Seq[AccessRequest]): Unit = {
    if (requests.nonEmpty) {
      val authorizer = checkInitialized()
      val user = userInfo(requests.head)
      val ctx = context()
      // a uri resource has two variants, the exact path and the path with a trailing
      // slash, which are authorized as alternatives by separate requests; the single
      // call authorizes a fixed set of accesses conjunctively, so it is used only when
      // the requests contain no uri resource
      val hasUri = requests.exists(_.resource.objectType == ObjectType.URI)
      val results = if (authorizeInSingleCall && !hasUri) {
        val accesses = requests.map(toAuthzRequest(_).getAccess).asJava
        (for {
          multiResult <-
            Option(authorizer.authorize(new RangerMultiAuthzRequest(user, accesses, ctx)))
          accessResults <- Option(multiResult.getAccesses)
        } yield accessResults.asScala.map(Option(_)).toSeq).getOrElse(Seq.empty)
      } else {
        requests.map { req =>
          val variantResults = req.resource.toResourceInfos.map { resourceInfo =>
            Option(authorizer.authorize(toAuthzRequest(req, resourceInfo)))
          }
          variantResults.find(_.exists(_.getDecision == AccessDecision.ALLOW))
            .getOrElse(variantResults.head)
        }
      }

      // a missing result means the authorizer could not evaluate the request,
      // which is denied instead of skipped
      val indices = results.padTo(requests.length, None).zipWithIndex.collect {
        case (result, idx) if result.forall(_.getDecision != AccessDecision.ALLOW) => idx
      }
      if (indices.nonEmpty) {
        val accessTypeToResource =
          indices.foldLeft(LinkedHashMap.empty[String, ArrayBuffer[String]])((m, idx) => {
            val req = requests(idx)
            val accessType = permissionOf(req.accessType)
            val resource = req.resource.getAsString
            m.getOrElseUpdate(accessType, ArrayBuffer.empty[String])
              .append(resource)
            m
          })
        val errorMsg = accessTypeToResource
          .map { case (accessType, resources) =>
            s"[$accessType] ${resources.mkString("privilege on [", ",", "]")}"
          }.mkString(", ")
        throw new AccessControlException(
          s"Permission denied: user [${requests.head.user}] does not have $errorMsg")
      }
    }
  }

  private def requireResult(result: RangerAuthzResult, req: AccessRequest): RangerAuthzResult =
    if (result == null) {
      // a missing result means the authorizer could not evaluate the request,
      // which must not be treated as allowed
      throw new AccessControlException(
        s"Permission denied: no authorization result for user [${req.user}], " +
          s"resource [${req.resource.getAsString}]")
    } else {
      result
    }

  def getFilterExpr(req: AccessRequest): Option[String] = {
    val result = requireResult(checkInitialized().authorize(toAuthzRequest(req)), req)
    Option(result)
      .filter(_.getDecision == AccessDecision.ALLOW)
      .flatMap { r =>
        Option(r.getPermissions.get(permissionOf(req.accessType)))
      }
      .map(_.getRowFilter)
      .filter(rf => rf != null && rf.getFilterExpr != null && rf.getFilterExpr.nonEmpty)
      .map(_.getFilterExpr)
  }

  def getMaskingExpr(req: AccessRequest): Option[String] = {
    val col = req.resource.getColumn
    val result = requireResult(checkInitialized().authorize(toAuthzRequest(req)), req)
    Option(result)
      .filter(_.getDecision == AccessDecision.ALLOW)
      .flatMap { r =>
        Option(r.getPermissions.get(permissionOf(req.accessType)))
      }
      .map(_.getDataMask)
      .filter(dm => dm != null && dm.getMaskType != null)
      .map { dm =>
        val maskType = dm.getMaskType
        if ("MASK_NULL".equalsIgnoreCase(maskType)) {
          "NULL"
        } else if ("CUSTOM".equalsIgnoreCase(maskType)) {
          val maskVal = dm.getMaskedValue
          if (maskVal == null) {
            "NULL"
          } else {
            s"${maskVal.replace("{col}", col)}"
          }
        } else {
          maskType match {
            case "MASK" => regexp_replace(col)
            case "MASK_SHOW_FIRST_4" =>
              regexp_replace(col, hasLen = true)
            case "MASK_SHOW_LAST_4" =>
              val left = regexp_replace(s"left($col, length($col) - 4)")
              s"concat($left, right($col, 4))"
            case "MASK_HASH" => s"md5(cast($col as string))"
            case "MASK_DATE_SHOW_YEAR" => s"date_trunc('YEAR', $col)"
            case _ => Option(dm.getMaskedValue)
                .filter(_.nonEmpty)
                .map(maskedValue => s"${maskedValue.replace("{col}", col)}")
                .orNull
          }
        }
      }
      .filter(_ != null)
  }

  private def regexp_replace(expr: String, hasLen: Boolean = false): String = {
    val pos = if (hasLen) ", 5" else ""
    val upper = s"regexp_replace($expr, '[A-Z]', 'X'$pos)"
    val lower = s"regexp_replace($upper, '[a-z]', 'x'$pos)"
    val digits = s"regexp_replace($lower, '[0-9]', 'n'$pos)"
    val other = s"regexp_replace($digits, '[^A-Za-z0-9]', 'U'$pos)"
    other
  }

  /**
   * verifying whether the user has any access to the resource,
   * used for filtering outputs of SHOW commands
   */
  def isAccessAllowed(req: AccessRequest): Boolean = {
    val result = checkInitialized().authorize(toAuthzRequest(req))
    result != null && result.getDecision == AccessDecision.ALLOW
  }
}
