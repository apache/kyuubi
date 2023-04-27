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
package org.apache.kyuubi.plugin.spark.authz.gen

import scala.collection.convert.ImplicitConversions._
import scala.language.implicitConversions

import org.apache.ranger.plugin.model.RangerPolicy
import org.apache.ranger.plugin.model.RangerPolicy._

import org.apache.kyuubi.plugin.spark.authz.gen.RangerClassConversions._

trait RangerObjectGenerator[T] {
  def get: T
}

object RangerClassConversions {
  implicit def getRangerObject[T](g: RangerObjectGenerator[T]): T = g.get
}

case class KRangerPolicy(
    service: String = "hive_jenkins",
    name: String,
    policyType: Int = 0,
    description: String = "",
    isAuditEnabled: Boolean = true,
    resources: Map[String, RangerPolicyResource] = Map.empty,
    conditions: List[RangerPolicyItemCondition] = List.empty,
    policyItems: List[RangerPolicyItem] = List.empty,
    denyPolicyItems: List[RangerPolicyItem] = List.empty,
    allowExceptions: List[RangerPolicyItem] = List.empty,
    denyExceptions: List[RangerPolicyItem] = List.empty,
    dataMaskPolicyItems: List[RangerDataMaskPolicyItem] = List.empty,
    rowFilterPolicyItems: List[RangerRowFilterPolicyItem] = List.empty,
    id: Int = 0,
    guid: String = "",
    isEnabled: Boolean = true,
    version: Int = 1) extends RangerObjectGenerator[RangerPolicy] {
  override def get: RangerPolicy = {
    val p = new RangerPolicy()
    p.setService(service)
    p.setName(name)
    p.setPolicyType(policyType)
    p.setDescription(description)
    p.setIsAuditEnabled(isAuditEnabled)
    p.setResources(resources)
    p.setConditions(conditions)
    p.setPolicyItems(policyItems)
    p.setAllowExceptions(allowExceptions)
    p.setDenyExceptions(denyExceptions)
    p.setDataMaskPolicyItems(dataMaskPolicyItems)
    p.setRowFilterPolicyItems(rowFilterPolicyItems)
    p.setId(id)
    p.setGuid(guid)
    p.setIsAuditEnabled(isEnabled)
    p.setVersion(version)
    p
  }
}

case class KRangerPolicyResource(
    values: List[String] = List.empty,
    isExcludes: Boolean = false,
    isRecursive: Boolean = false) extends RangerObjectGenerator[RangerPolicyResource] {
  override def get: RangerPolicyResource = {
    val r = new RangerPolicyResource()
    r.setValues(values)
    r.setIsExcludes(isExcludes)
    r.setIsRecursive(isRecursive)
    r
  }
}

object KRangerPolicyResource {
  def databaseRes(values: String*): (String, RangerPolicyResource) =
    "database" -> KRangerPolicyResource(values.toList)

  def tableRes(values: String*): (String, RangerPolicyResource) =
    "table" -> KRangerPolicyResource(values.toList)

  def columnRes(values: String*): (String, RangerPolicyResource) =
    "column" -> KRangerPolicyResource(values.toList)
}

case class KRangerPolicyItemCondition(
    `type`: String,
    values: List[String]) extends RangerObjectGenerator[RangerPolicyItemCondition] {
  override def get: RangerPolicyItemCondition = {
    val c = new RangerPolicyItemCondition()
    c.setType(`type`)
    c.setValues(values)
    c
  }
}

case class KRangerPolicyItem(
    accesses: List[RangerPolicyItemAccess] = List.empty,
    users: List[String] = List.empty,
    groups: List[String] = List.empty,
    conditions: List[RangerPolicyItemCondition] = List.empty,
    delegateAdmin: Boolean = false) extends RangerObjectGenerator[RangerPolicyItem] {
  override def get: RangerPolicyItem = {
    val i = new RangerPolicyItem()
    i.setAccesses(accesses)
    i.setUsers(users)
    i.setGroups(groups)
    i.setConditions(conditions)
    i.setDelegateAdmin(delegateAdmin)
    i
  }
}

case class KRangerPolicyItemAccess(
    `type`: String,
    isAllowed: Boolean) extends RangerObjectGenerator[RangerPolicyItemAccess] {
  override def get: RangerPolicyItemAccess = {
    val a = new RangerPolicyItemAccess
    a.setType(`type`)
    a.setIsAllowed(isAllowed)
    a
  }
}

object KRangerPolicyItemAccess {
  def allowTypes(types: String*): List[RangerPolicyItemAccess] =
    types.map(t => KRangerPolicyItemAccess(t, isAllowed = true).get).toList
}

case class KRangerDataMaskPolicyItem(
    dataMaskInfo: RangerPolicyItemDataMaskInfo,
    accesses: List[RangerPolicyItemAccess] = List.empty,
    users: List[String] = List.empty,
    groups: List[String] = List.empty,
    conditions: List[RangerPolicyItemCondition] = List.empty,
    delegateAdmin: Boolean = false) extends RangerObjectGenerator[RangerDataMaskPolicyItem] {
  override def get: RangerDataMaskPolicyItem = {
    val i = new RangerDataMaskPolicyItem
    i.setDataMaskInfo(dataMaskInfo)
    i.setAccesses(accesses)
    i.setUsers(users)
    i.setGroups(groups)
    i.setConditions(conditions)
    i.setDelegateAdmin(delegateAdmin)
    i
  }
}

case class KRangerPolicyItemDataMaskInfo(
    dataMaskType: String) extends RangerObjectGenerator[RangerPolicyItemDataMaskInfo] {
  override def get: RangerPolicyItemDataMaskInfo = {
    val i = new RangerPolicyItemDataMaskInfo
    i.setDataMaskType(dataMaskType)
    i
  }
}

case class KRangerRowFilterPolicyItem(
    rowFilterInfo: RangerPolicyItemRowFilterInfo,
    accesses: List[RangerPolicyItemAccess] = List.empty,
    users: List[String] = List.empty,
    groups: List[String] = List.empty,
    conditions: List[RangerPolicyItemCondition] = List.empty,
    delegateAdmin: Boolean = false) extends RangerObjectGenerator[RangerRowFilterPolicyItem] {
  override def get: RangerRowFilterPolicyItem = {
    val i = new RangerRowFilterPolicyItem
    i.setRowFilterInfo(rowFilterInfo)
    i.setAccesses(accesses)
    i.setUsers(users)
    i.setGroups(groups)
    i.setConditions(conditions)
    i.setDelegateAdmin(delegateAdmin)
    i
  }
}

case class KRangerPolicyItemRowFilterInfo(
    filterExpr: String) extends RangerObjectGenerator[RangerPolicyItemRowFilterInfo] {
  override def get: RangerPolicyItemRowFilterInfo = {
    val i = new RangerPolicyItemRowFilterInfo
    i.setFilterExpr(filterExpr)
    i
  }
}

object RangerAccessType {
  val select = "select"
  val update = "update"
  val create = "create"
  val drop = "drop"
  val alter = "alter"
  val index = "index"
  val lock = "lock"
  val all = "all"
  val read = "read"
  val write = "write"
  val use = "use"
}
