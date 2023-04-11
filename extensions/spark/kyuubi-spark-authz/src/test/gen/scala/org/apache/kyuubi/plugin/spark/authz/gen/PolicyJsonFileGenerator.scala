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

import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.language.implicitConversions

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.ranger.plugin.model.RangerPolicy

import org.apache.kyuubi.plugin.spark.authz.gen.KRangerPolicyItemAccess.allowTypes
import org.apache.kyuubi.plugin.spark.authz.gen.PolicyJsonFileGenerator.RangerAccessType.{all, alter, create, drop, index, lock, read, select, update, use, write, RangerAccessType}
import org.apache.kyuubi.plugin.spark.authz.gen.RangerClassConversions.getRangerObject

/**
 * Generates the policy file to test/main/resources dir.
 *
 * Usage:
 * build/mvn scala:run -pl :kyuubi-spark-authz_2.12
 * -DmainClass=org.apache.kyuubi.plugin.spark.authz.gen.PolicyJsonFileGenerator
 */
private object PolicyJsonFileGenerator {
  def main(args: Array[String]): Unit = {
    writeRangerServicePolicesJson()
  }

  final private val mapper: ObjectMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .serializationInclusion(Include.NON_NULL)
    .build()

  def writeRangerServicePolicesJson(): Unit = {
    val pluginHome = getClass.getProtectionDomain.getCodeSource.getLocation.getPath
      .split("target").head
    val policyFileName = "sparkSql_hive_jenkins.json"
    val policyFile = Paths.get(pluginHome, "src", "test", "resources", policyFileName).toFile
    // scalastyle:off println
    println(s"Writing ranger policies to $policyFileName.")
    // scalastyle:on println
    mapper.writerWithDefaultPrettyPrinter().writeValue(policyFile, servicePolicies)
  }

  private def servicePolicies: JsonNode = {
    val inputStream = Thread.currentThread().getContextClassLoader
      .getResourceAsStream("policies_base.json")
    val rootObjNode = mapper.readTree(inputStream).asInstanceOf[ObjectNode]
    val policies = genPolicies
    // scalastyle:off println
    println(s"Generated ${policies.size} policies.")
    // scalastyle:on println
    rootObjNode.set("policies", mapper.readTree(mapper.writeValueAsString(policies)))
  }

  private def genPolicies: Iterable[RangerPolicy] = {
    List[RangerPolicy](
      // access for all
      policyAccessForAllUrl,
      policyAccessForAllDbTableColumns,
      policyAccessForAllDbUdf,
      // access
      policyAccessForDbAllColumns,
      policyAccessForDefaultDbSrcTable,
      policyAccessForDefaultBobUse,
      policyAccessForDefaultBobSelect,
      policyAccessForPermViewAccessOnly,
      // row filter
      policyFilterForSrcTableKeyLessThan20,
      policyFilterForPermViewKeyLessThan20,
      // data masking
      policyMaskForPermView,
      policyMaskForPermViewUser,
      policyMaskNullifyForValue2,
      policyMaskShowFirst4ForValue3,
      policyMaskDateShowYearForValue4,
      policyMaskShowFirst4ForValue5)
      // fill the id and guid with auto-increased index
      .map(p => {
        val id = policyIdCounter.incrementAndGet()
        p.setId(id)
        p.setGuid(UUID.nameUUIDFromBytes(id.toString.getBytes()).toString)
        p
      })
  }

  final private lazy val policyIdCounter = new AtomicLong(0)

  // resource template
  private def databaseRes(values: List[String]) =
    "database" -> KRangerPolicyResource(values = values).get
  private def tableRes(values: List[String]) =
    "table" -> KRangerPolicyResource(values = values).get
  private def columnRes(values: List[String]) =
    "column" -> KRangerPolicyResource(values = values).get

  // users
  private val admin = "admin"
  private val bob = "bob"
  private val kent = "kent"
  private val permViewUser = "perm_view_user"
  private val ownerPlaceHolder = "{OWNER}"
  private val createOnlyUser = "create_only_user"
  private val defaultTableOwner = "default_table_owner"
  private val permViewOnlyUser = "user_perm_view_only"

  // db
  private val defaultDb = "default"
  private val sparkCatalog = "spark_catalog"
  private val icebergNamespace = "iceberg_ns"
  private val namespace1 = "ns1"

  // access type
  object RangerAccessType extends Enumeration {
    type RangerAccessType = Value
    val select, update, create, drop, alter, index, lock, all, read, write, use = Value
  }
  implicit def actionTypeStr(t: RangerAccessType): String = t.toString

  // resources
  private val allDatabaseRes = databaseRes(List("*"))
  private val allTableRes = tableRes(List("*"))
  private val allColumnRes = columnRes(List("*"))
  private val srcTableRes = tableRes(List("src"))

  // policy type
  private val POLICY_TYPE_ACCESS: Int = 0
  private val POLICY_TYPE_DATAMASK: Int = 1
  private val POLICY_TYPE_ROWFILTER: Int = 2

  // policies
  private val policyAccessForAllUrl = KRangerPolicy(
    name = "all - url",
    description = "Policy for all - url",
    resources = Map("url" -> KRangerPolicyResource(
      values = List("*"),
      isRecursive = true)),
    policyItems = List(KRangerPolicyItem(
      users = List(admin),
      accesses = allowTypes(select, update, create, drop, alter, index, lock, all, read, write),
      delegateAdmin = true)))

  private val policyAccessForAllDbTableColumns = KRangerPolicy(
    name = "all - database, table, column",
    description = "Policy for all - database, table, column",
    resources = Map(allDatabaseRes, allTableRes, allColumnRes),
    policyItems = List(KRangerPolicyItem(
      users = List(admin),
      accesses = allowTypes(select, update, create, drop, alter, index, lock, all, read, write),
      delegateAdmin = true)))

  private val policyAccessForAllDbUdf = KRangerPolicy(
    name = "all - database, udf",
    description = "Policy for all - database, udf",
    resources = Map(allDatabaseRes, "udf" -> KRangerPolicyResource(values = List("*"))),
    policyItems = List(KRangerPolicyItem(
      users = List(admin),
      accesses = allowTypes(select, update, create, drop, alter, index, lock, all, read, write),
      delegateAdmin = true)))

  private val policyAccessForDbAllColumns = KRangerPolicy(
    name = "all - database, udf",
    description = "Policy for all - database, udf",
    resources = Map(
      databaseRes(List(defaultDb, sparkCatalog, icebergNamespace, namespace1)),
      allTableRes,
      allColumnRes),
    policyItems = List(
      KRangerPolicyItem(
        users = List(bob, permViewUser, ownerPlaceHolder),
        accesses = allowTypes(select, update, create, drop, alter, index, lock, all, read, write),
        delegateAdmin = true),
      KRangerPolicyItem(
        users = List(defaultTableOwner, createOnlyUser),
        accesses = allowTypes(create),
        delegateAdmin = true)))

  private val policyAccessForDefaultDbSrcTable = KRangerPolicy(
    name = "default_kent",
    resources = Map(
      databaseRes(List(defaultDb, sparkCatalog)),
      srcTableRes,
      columnRes(List("key"))),
    policyItems = List(
      KRangerPolicyItem(
        users = List(kent),
        accesses = allowTypes(select, update, create, drop, alter, index, lock, all, read, write),
        delegateAdmin = true),
      KRangerPolicyItem(
        users = List(defaultTableOwner, createOnlyUser),
        accesses = allowTypes(create),
        delegateAdmin = true)))

  private val policyFilterForSrcTableKeyLessThan20 = KRangerPolicy(
    name = "src_key_less_than_20",
    policyType = POLICY_TYPE_ROWFILTER,
    resources = Map(
      databaseRes(List(defaultDb)),
      srcTableRes),
    rowFilterPolicyItems = List(
      KRangerRowFilterPolicyItem(
        rowFilterInfo = KRangerPolicyItemRowFilterInfo(filterExpr = "key<20"),
        accesses = allowTypes(select),
        users = List(bob, permViewUser))))

  private val policyFilterForPermViewKeyLessThan20 = KRangerPolicy(
    name = "perm_view_key_less_than_20",
    policyType = POLICY_TYPE_ROWFILTER,
    resources = Map(
      databaseRes(List(defaultDb)),
      tableRes(List("perm_view"))),
    rowFilterPolicyItems = List(
      KRangerRowFilterPolicyItem(
        rowFilterInfo = KRangerPolicyItemRowFilterInfo(filterExpr = "key<20"),
        accesses = allowTypes(select),
        users = List(permViewUser))))

  private val policyAccessForDefaultBobUse = KRangerPolicy(
    name = "default_bob_use",
    resources = Map(
      databaseRes(List("default_bob", sparkCatalog)),
      tableRes(List("table_use*")),
      allColumnRes),
    policyItems = List(
      KRangerPolicyItem(
        users = List(bob),
        accesses = allowTypes(update),
        delegateAdmin = true)))

  private val policyAccessForDefaultBobSelect = KRangerPolicy(
    name = "default_bob_select",
    resources = Map(
      databaseRes(List("default_bob", sparkCatalog)),
      tableRes(List("table_select*")),
      allColumnRes),
    policyItems = List(
      KRangerPolicyItem(
        users = List(bob),
        accesses = allowTypes(select, use),
        delegateAdmin = true)))

  private val policyMaskForPermView = KRangerPolicy(
    name = "src_value_hash_perm_view",
    policyType = POLICY_TYPE_DATAMASK,
    resources = Map(
      databaseRes(List(defaultDb, sparkCatalog)),
      srcTableRes,
      columnRes(List("value1"))),
    dataMaskPolicyItems = List(
      KRangerDataMaskPolicyItem(
        dataMaskInfo = KRangerPolicyItemDataMaskInfo(dataMaskType = "MASK_HASH"),
        users = List(bob),
        accesses = allowTypes(select),
        delegateAdmin = true)))

  private val policyMaskForPermViewUser = KRangerPolicy(
    name = "src_value_hash",
    policyType = POLICY_TYPE_DATAMASK,
    resources = Map(
      databaseRes(List(defaultDb, sparkCatalog)),
      tableRes(List("perm_view")),
      columnRes(List("value1"))),
    dataMaskPolicyItems = List(
      KRangerDataMaskPolicyItem(
        dataMaskInfo = KRangerPolicyItemDataMaskInfo(dataMaskType = "MASK_HASH"),
        users = List(permViewUser),
        accesses = allowTypes(select),
        delegateAdmin = true)))

  private val policyMaskNullifyForValue2 = KRangerPolicy(
    name = "src_value2_nullify",
    policyType = POLICY_TYPE_DATAMASK,
    resources = Map(
      databaseRes(List(defaultDb, sparkCatalog, icebergNamespace, namespace1)),
      srcTableRes,
      columnRes(List("value2"))),
    dataMaskPolicyItems = List(
      KRangerDataMaskPolicyItem(
        dataMaskInfo = KRangerPolicyItemDataMaskInfo(dataMaskType = "MASK"),
        users = List(bob),
        accesses = allowTypes(select),
        delegateAdmin = true)))

  private val policyMaskShowFirst4ForValue3 = KRangerPolicy(
    name = "src_value3_sf4",
    policyType = POLICY_TYPE_DATAMASK,
    resources = Map(
      databaseRes(List(defaultDb, sparkCatalog)),
      srcTableRes,
      columnRes(List("value3"))),
    dataMaskPolicyItems = List(
      KRangerDataMaskPolicyItem(
        dataMaskInfo = KRangerPolicyItemDataMaskInfo(dataMaskType = "MASK_SHOW_FIRST_4"),
        users = List(bob),
        accesses = allowTypes(select),
        delegateAdmin = true)))

  private val policyMaskDateShowYearForValue4 = KRangerPolicy(
    name = "src_value4_sf4",
    policyType = POLICY_TYPE_DATAMASK,
    resources = Map(
      databaseRes(List(defaultDb, sparkCatalog)),
      srcTableRes,
      columnRes(List("value4"))),
    dataMaskPolicyItems = List(
      KRangerDataMaskPolicyItem(
        dataMaskInfo = KRangerPolicyItemDataMaskInfo(dataMaskType = "MASK_DATE_SHOW_YEAR"),
        users = List(bob),
        accesses = allowTypes(select),
        delegateAdmin = true)))

  private val policyMaskShowFirst4ForValue5 = KRangerPolicy(
    name = "src_value5_sf4",
    policyType = POLICY_TYPE_DATAMASK,
    resources = Map(
      databaseRes(List(defaultDb, sparkCatalog)),
      srcTableRes,
      columnRes(List("value5"))),
    dataMaskPolicyItems = List(
      KRangerDataMaskPolicyItem(
        dataMaskInfo = KRangerPolicyItemDataMaskInfo(dataMaskType = "MASK_SHOW_LAST_4"),
        users = List(bob),
        accesses = allowTypes(select),
        delegateAdmin = true)))

  private val policyAccessForPermViewAccessOnly = KRangerPolicy(
    name = "someone_access_perm_view",
    resources = Map(
      databaseRes(List(defaultDb)),
      tableRes(List("perm_view")),
      allColumnRes),
    policyItems = List(
      KRangerPolicyItem(
        users = List(permViewOnlyUser),
        accesses = allowTypes(select),
        delegateAdmin = true)))
}
