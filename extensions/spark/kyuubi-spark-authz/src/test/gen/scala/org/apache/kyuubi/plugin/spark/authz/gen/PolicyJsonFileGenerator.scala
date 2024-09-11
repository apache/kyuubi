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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.ranger.plugin.model.RangerPolicy
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.gen.KRangerPolicyItemAccess.allowTypes
import org.apache.kyuubi.plugin.spark.authz.gen.KRangerPolicyResource._
import org.apache.kyuubi.plugin.spark.authz.gen.RangerAccessType._
import org.apache.kyuubi.plugin.spark.authz.gen.RangerClassConversions._
import org.apache.kyuubi.util.AssertionUtils._
import org.apache.kyuubi.util.GoldenFileUtils._

/**
 * Generates the policy file to test/main/resources dir.
 *
 * To run the test suite:
 * {{{
 *   KYUUBI_UPDATE=0 dev/gen/gen_ranger_policy_json.sh
 * }}}
 *
 * To regenerate the ranger policy file:
 * {{{
 *   dev/gen/gen_ranger_policy_json.sh
 * }}}
 */
class PolicyJsonFileGenerator extends AnyFunSuite {
  // scalastyle:on
  final private val mapper: ObjectMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .serializationInclusion(Include.NON_NULL)
    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
    .build()

  test("check ranger policy file") {
    val policyFileName = "sparkSql_hive_jenkins.json"
    val policyFilePath = Paths.get(
      s"${getCurrentModuleHome(this)}/src/test/resources/$policyFileName")
    val generatedStr = mapper.writerWithDefaultPrettyPrinter()
      .writeValueAsString(servicePolicies)

    if (sys.env.get("KYUUBI_UPDATE").contains("1")) {
      // scalastyle:off println
      println(s"Writing ranger policies to $policyFileName.")
      // scalastyle:on println
      Files.write(
        policyFilePath,
        generatedStr.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      assertFileContent(
        policyFilePath,
        Seq(generatedStr),
        "dev/gen/gen_ranger_policy_json.sh",
        splitFirstExpectedLine = true)
    }
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
      policyAccessForTable2AccessOnly,
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
      .zipWithIndex
      .map {
        case (p, index) =>
          p.setId(index)
          p.setGuid(UUID.nameUUIDFromBytes(index.toString.getBytes()).toString)
          p
      }
  }

  // resources
  private val allDatabaseRes = databaseRes("*")
  private val allTableRes = tableRes("*")
  private val allColumnRes = columnRes("*")
  private val srcTableRes = tableRes("src")

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
      databaseRes(defaultDb, sparkCatalog, icebergNamespace, namespace1, paimonNamespace),
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
      databaseRes(defaultDb, sparkCatalog),
      srcTableRes,
      columnRes("key")),
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
      databaseRes(defaultDb),
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
      databaseRes(defaultDb),
      tableRes("perm_view")),
    rowFilterPolicyItems = List(
      KRangerRowFilterPolicyItem(
        rowFilterInfo = KRangerPolicyItemRowFilterInfo(filterExpr = "key<20"),
        accesses = allowTypes(select),
        users = List(permViewUser))))

  private val policyAccessForDefaultBobUse = KRangerPolicy(
    name = "default_bob_use",
    resources = Map(
      databaseRes("default_bob", sparkCatalog),
      tableRes("table_use*"),
      allColumnRes),
    policyItems = List(
      KRangerPolicyItem(
        users = List(bob),
        accesses = allowTypes(update),
        delegateAdmin = true)))

  private val policyAccessForDefaultBobSelect = KRangerPolicy(
    name = "default_bob_select",
    resources = Map(
      databaseRes("default_bob", sparkCatalog),
      tableRes("table_select*"),
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
      databaseRes(defaultDb, sparkCatalog),
      srcTableRes,
      columnRes("value1")),
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
      databaseRes(defaultDb, sparkCatalog),
      tableRes("perm_view"),
      columnRes("value1")),
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
      databaseRes(defaultDb, sparkCatalog, icebergNamespace, namespace1),
      srcTableRes,
      columnRes("value2")),
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
      databaseRes(defaultDb, sparkCatalog),
      srcTableRes,
      columnRes("value3")),
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
      databaseRes(defaultDb, sparkCatalog),
      srcTableRes,
      columnRes("value4")),
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
      databaseRes(defaultDb, sparkCatalog),
      srcTableRes,
      columnRes("value5")),
    dataMaskPolicyItems = List(
      KRangerDataMaskPolicyItem(
        dataMaskInfo = KRangerPolicyItemDataMaskInfo(dataMaskType = "MASK_SHOW_LAST_4"),
        users = List(bob),
        accesses = allowTypes(select),
        delegateAdmin = true)))

  private val policyAccessForPermViewAccessOnly = KRangerPolicy(
    name = "someone_access_perm_view",
    resources = Map(
      databaseRes(defaultDb),
      tableRes("perm_view"),
      allColumnRes),
    policyItems = List(
      KRangerPolicyItem(
        users = List(permViewOnlyUser),
        accesses = allowTypes(select),
        delegateAdmin = true)))

  private val policyAccessForTable2AccessOnly = KRangerPolicy(
    name = "someone_access_table2",
    resources = Map(
      databaseRes(defaultDb),
      tableRes("table2"),
      allColumnRes),
    policyItems = List(
      KRangerPolicyItem(
        users = List(table2OnlyUser),
        accesses = allowTypes(select),
        delegateAdmin = true)))
}
