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

package org.apache.kyuubi.plugin.spark.authz

object RangerTestUsers {
  // authorized users used in policy generation
  val admin = "admin"
  val alice = "alice"
  val bob = "bob"
  val kent = "kent"
  val permViewUser = "perm_view_user"
  val ownerPlaceHolder = "{OWNER}"
  val createOnlyUser = "create_only_user"
  val defaultTableOwner = "default_table_owner"
  val permViewOnlyUser = "user_perm_view_only"
  val table2OnlyUser = "user_table2_only"
  val table1OnlyUserForNs = "user_table1_only_for_ns"

  // non-authorized users
  val invisibleUser = "i_am_invisible"
  val denyUser = "denyuser"
  val denyUser2 = "denyuser2"
  val someone = "someone"
}

object RangerTestNamespace {
  val defaultDb = "default"
  val sparkCatalog = "spark_catalog"
  val icebergNamespace = "iceberg_ns"
  val hudiNamespace = "hudi_ns"
  val paimonNamespace = "paimon_ns"
  val deltaNamespace = "delta_ns"
  val namespace1 = "ns1"
  val namespace2 = "ns2"
}
