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

import java.util._

import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Lists
import org.apache.ranger.audit.model.AuthzAuditEvent
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler
import org.apache.ranger.plugin.model.RangerPolicy
import org.apache.ranger.plugin.policyengine.{RangerAccessRequest, RangerAccessResource, RangerAccessResult}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.ObjectType
import org.apache.kyuubi.plugin.spark.authz.OperationType
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessRequest
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessResource
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType.AccessType
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAuditHandler.{LOG, URL_RESOURCE_TYPE}

object SparkRangerAuditHandler {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[RangerDefaultAuditHandler])
  val ACCESS_TYPE_ROWFILTER: String = "ROW_FILTER"
  val ACCESS_TYPE_INSERT: String = "INSERT"
  val ACCESS_TYPE_UPDATE: String = "UPDATE"
  val ACCESS_TYPE_DELETE: String = "DELETE"
  val ACCESS_TYPE_TRUNCATE: String = "TRUNCATE"
  val ACTION_TYPE_METADATA_OPERATION: String = "METADATA OPERATION"
  val URL_RESOURCE_TYPE: String = "url"
  val CONF_AUDIT_QUERY_REQUEST_SIZE: String = "xasecure.audit.solr.limit.query.req.size"
  val DEFAULT_CONF_AUDIT_QUERY_REQUEST_SIZE: Int = Integer.MAX_VALUE
}

class SparkRangerAuditHandler extends RangerDefaultAuditHandler {

  // Implementing meaningfully audit functions
  requestQuerySize = SparkRangerAuditHandler.DEFAULT_CONF_AUDIT_QUERY_REQUEST_SIZE
  private var requestQuerySize: Int = 0
  var auditEvents: Collection[AuthzAuditEvent] = null
  var deniedExists: Boolean = false

  def createAuditEvent(result: RangerAccessResult, accessType: String, resourcePath: String): AuthzAuditEvent = {
    val request: RangerAccessRequest = result.getAccessRequest
    val resource: RangerAccessResource = request.getResource
    val resourceType: String = if (resource != null) {
      resource.getLeafName
    }
    else {
      null
    }
    val auditEvent: AuthzAuditEvent = super.getAuthzEvents(result)
    var resourcePathComputed: String = resourcePath
    if (URL_RESOURCE_TYPE == resourceType) {
      resourcePathComputed = getURLPathString(resource, resourcePathComputed)
    }
    if (LOG.isDebugEnabled) {
      LOG.debug("requestQuerySize = " + requestQuerySize)
    }
    if (StringUtils.isNotBlank(request.getRequestData) && request.getRequestData.length > requestQuerySize) {
      auditEvent.setRequestData(request.getRequestData.substring(0, requestQuerySize))
    }
    else {
      auditEvent.setRequestData(request.getRequestData)
    }
    auditEvent.setAccessType(accessType)
    auditEvent.setResourcePath(resourcePathComputed)
    auditEvent.setResourceType("@" + resourceType) // to be consistent with earlier release

    if (request.isInstanceOf[AccessRequest] && resource.isInstanceOf[AccessResource]) {
      val sparkAccessRequest: AccessRequest = request.asInstanceOf[AccessRequest]
      val sparkResource: AccessResource = resource.asInstanceOf[AccessResource]
      val sparkAccessType: AccessType = sparkAccessRequest.accessType
      if ((sparkAccessType eq AccessType.USE) && (sparkResource.objectType eq ObjectType.DATABASE) && StringUtils.isBlank(sparkResource.getDatabase)) { // this should happen only for SHOWDATABASES
        auditEvent.setTags(null)
      }
      if (sparkAccessType eq AccessType.ADMIN) {
        val sparkOperationType: String = request.getAction
        var commandStr: String = request.getRequestData
        auditEvent.setAccessType(commandStr)
      }
      val action: String = request.getAction
      auditEvent.setAccessType(action)
    }
    return auditEvent
  }

  def createAuditEvent(result: RangerAccessResult): AuthzAuditEvent = {
    var ret: AuthzAuditEvent = null
    val request: RangerAccessRequest = result.getAccessRequest
    val resource: RangerAccessResource = request.getResource
    val resourcePath: String = if (resource != null) {
      resource.getAsString
    }
    else {
      null
    }
    val policyType: Int = result.getPolicyType
    if (policyType == RangerPolicy.POLICY_TYPE_DATAMASK && result.isMaskEnabled) {
      ret = createAuditEvent(result, result.getMaskType, resourcePath)
    }
    else {
      if (policyType == RangerPolicy.POLICY_TYPE_ROWFILTER) {
        ret = createAuditEvent(result, SparkRangerAuditHandler.ACCESS_TYPE_ROWFILTER, resourcePath)
      }
      else {
        if (policyType == RangerPolicy.POLICY_TYPE_ACCESS) {
          var accessType: String = null
          if (request.isInstanceOf[AccessRequest]) {
            val sparkRequest: AccessRequest = request.asInstanceOf[AccessRequest]
            accessType = sparkRequest.getAccessType.toString
            val action: String = request.getAction
            if (SparkRangerAuditHandler.ACTION_TYPE_METADATA_OPERATION == action) {
              accessType = SparkRangerAuditHandler.ACTION_TYPE_METADATA_OPERATION
            }
            else {
              if (AccessType.UPDATE.toString.equalsIgnoreCase(accessType)) {
                val commandStr: String = request.getRequestData
                if (StringUtils.isNotBlank(commandStr)) {
                  if (StringUtils.startsWithIgnoreCase(commandStr, SparkRangerAuditHandler.ACCESS_TYPE_INSERT)) {
                    accessType = SparkRangerAuditHandler.ACCESS_TYPE_INSERT
                  }
                  else {
                    if (StringUtils.startsWithIgnoreCase(commandStr, SparkRangerAuditHandler.ACCESS_TYPE_UPDATE)) {
                      accessType = SparkRangerAuditHandler.ACCESS_TYPE_UPDATE
                    }
                    else {
                      if (StringUtils.startsWithIgnoreCase(commandStr, SparkRangerAuditHandler.ACCESS_TYPE_DELETE)) {
                        accessType = SparkRangerAuditHandler.ACCESS_TYPE_DELETE
                      }
                      else {
                        if (StringUtils.startsWithIgnoreCase(commandStr, SparkRangerAuditHandler.ACCESS_TYPE_TRUNCATE)) {
                          accessType = SparkRangerAuditHandler.ACCESS_TYPE_TRUNCATE
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          if (StringUtils.isEmpty(accessType)) {
            accessType = request.getAccessType
          }
          ret = createAuditEvent(result, accessType, resourcePath)
        }
      }
    }
    return ret
  }

  def createAuditEvents(results: Collection[RangerAccessResult]): List[AuthzAuditEvent] = {
    val auditEvents: Map[Long, AuthzAuditEvent] = new HashMap[Long, AuthzAuditEvent]
    val iterator: Iterator[RangerAccessResult] = results.iterator
    var deniedAuditEvent: AuthzAuditEvent = null
    while ( {
      iterator.hasNext && deniedAuditEvent == null
    }) {
      val result: RangerAccessResult = iterator.next
      if (result.getIsAudited) {
        if (!(result.getIsAllowed)) {
          deniedAuditEvent = createAuditEvent(result)
        }
        else {
          val policyId: Long = result.getPolicyId
          if (auditEvents.containsKey(policyId)) { // add this result to existing event by updating column values
            val auditEvent: AuthzAuditEvent = auditEvents.get(policyId)
            val request: AccessRequest = result.getAccessRequest.asInstanceOf[AccessRequest]
            val resource: AccessResource = request.getResource.asInstanceOf[AccessResource]
            val resourcePath: String = auditEvent.getResourcePath + "," + resource.getColumn
            auditEvent.setResourcePath(resourcePath)
            val tags: Set[String] = getTags(request)
            if (tags != null) {
              auditEvent.getTags.addAll(tags)
            }
          }
          else { // new event as this approval was due to a different policy.
            val auditEvent: AuthzAuditEvent = createAuditEvent(result)
            if (auditEvent != null) {
              auditEvents.put(policyId, auditEvent)
            }
          }
        }
      }
    }
    var result: List[AuthzAuditEvent] = null
    if (deniedAuditEvent == null) {
      result = new ArrayList[AuthzAuditEvent](auditEvents.values)
    }
    else {
      result = Lists.newArrayList(deniedAuditEvent)
    }
    return result
  }

  override def processResult(result: RangerAccessResult): Unit = {
    if (!(result.getIsAudited)) {
      return
    }
    if (skipFilterOperationAuditing(result)) {
      return
    }
    val auditEvent: AuthzAuditEvent = createAuditEvent(result)
    if (auditEvent != null) {
      addAuthzAuditEvent(auditEvent)
    }
  }

  /**
   * This method is expected to be called ONLY to process the results for multiple-columns in a table.
   * To ensure this, RangerHiveAuthorizer should call isAccessAllowed(Collection<requests>) only for this condition
   */
  override def processResults(results: Collection[RangerAccessResult]): Unit = {
    val auditEvents: List[AuthzAuditEvent] = createAuditEvents(results)
    import scala.collection.JavaConversions._
    for (auditEvent <- auditEvents) {
      addAuthzAuditEvent(auditEvent)
    }
  }

  def logAuditEventForDfs(userName: String, dfsCommand: String, accessGranted: Boolean, repositoryType: Int, repositoryName: String): Unit = {
    val auditEvent: AuthzAuditEvent = new AuthzAuditEvent
    auditEvent.setAclEnforcer(moduleName)
    auditEvent.setResourceType("@dfs")
    auditEvent.setAccessType("DFS")
    auditEvent.setAction("DFS")
    auditEvent.setUser(userName)
    auditEvent.setAccessResult((if (accessGranted) {
      1
    }
    else {
      0
    }).toShort)
    auditEvent.setEventTime(new Date)
    auditEvent.setRepositoryType(repositoryType)
    auditEvent.setRepositoryName(repositoryName)
    auditEvent.setRequestData(dfsCommand)
    auditEvent.setResourcePath(dfsCommand)
    if (LOG.isDebugEnabled) {
      LOG.debug("Logging DFS event " + auditEvent.toString)
    }
    addAuthzAuditEvent(auditEvent)
  }

  def flushAudit(): Unit = {
    if (auditEvents == null) {
      return
    }
    import scala.collection.JavaConversions._
    for (auditEvent <- auditEvents) {
      if (!deniedExists || auditEvent.getAccessResult == 0) { // if deny exists, skip logging for allowed results
        super.logAuthzAudit(auditEvent)
      }
    }
  }

  private def addAuthzAuditEvent(auditEvent: AuthzAuditEvent): Unit = {
    if (auditEvent != null) {
      if (auditEvents == null) {
        auditEvents = new ArrayList[AuthzAuditEvent]
      }
      auditEvents.add(auditEvent)
      if (auditEvent.getAccessResult == 0) {
        deniedExists = true
      }
    }
  }

  private def getReplCmd(cmdString: String): String = {
    var ret: String = "REPL"
    if (cmdString != null) {
      val cmd: Array[String] = cmdString.trim.split("\\s+")
      if (!(cmd.isEmpty) && cmd.length > 2) {
        ret = cmd(0) + " " + cmd(1)
      }
    }
    return ret
  }

  private def getServiceAdminCmd(cmdString: String): String = {
    var ret: String = "SERVICE ADMIN"
    if (cmdString != null) {
      val cmd: Array[String] = cmdString.trim.split("\\s+")
      if (!(cmd.isEmpty) && cmd.length > 1) {
        ret = cmd(0) + " " + cmd(1)
      }
    }
    return ret
  }

  private def getServiceAdminQueryId(cmdString: String): String = {
    var ret: String = "QUERY ID = "
    if (cmdString != null) {
      val cmd: Array[String] = cmdString.trim.split("\\s+")
      if (!(cmd.isEmpty) && cmd.length > 2) {
        ret = ret + cmd(2)
      }
    }
    return ret
  }

  private def skipFilterOperationAuditing(result: RangerAccessResult): Boolean = {
    var ret: Boolean = false
    val accessRequest: RangerAccessRequest = result.getAccessRequest
    if (accessRequest != null) {
      val action: String = accessRequest.getAction
      if (SparkRangerAuditHandler.ACTION_TYPE_METADATA_OPERATION == action && !(result.getIsAllowed)) {
        ret = true
      }
    }
    return ret
  }

  private def getURLPathString(resource: RangerAccessResource, resourcePath: String): String = {
    var ret: String = resourcePath
    var resourcePathVal: ArrayList[String] = null
    val `val`: Any = resource.getValue(URL_RESOURCE_TYPE)
    if (`val`.isInstanceOf[List[_]]) {
      resourcePathVal = `val`.asInstanceOf[ArrayList[String]]
      if (CollectionUtils.isNotEmpty(resourcePathVal)) {
        ret = resourcePathVal.get(0)
      }
    }
    return ret
  }

}
