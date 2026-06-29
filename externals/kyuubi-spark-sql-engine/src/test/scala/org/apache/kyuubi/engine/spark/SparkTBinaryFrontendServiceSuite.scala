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

package org.apache.kyuubi.engine.spark

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.thrift.{DelegationTokenIdentifier => HiveTokenIdent}
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.kyuubi.KyuubiFunSuite

class SparkTBinaryFrontendServiceSuite extends KyuubiFunSuite {
  test("new hive conf") {
    val hiveConf = SparkTBinaryFrontendService.hiveConf(new Configuration())
    assert(hiveConf.getClass().getName == SparkTBinaryFrontendService.HIVE_CONF_CLASSNAME)
  }

  test("merge Hive tokens preserves signature-bound tokens for multiple metastores") {
    val newTokens = Map(
      new Text("hms_a") -> newHiveToken(issueDate = 1000, service = "hms_a"),
      new Text("hms_b") -> newHiveToken(issueDate = 1000, service = "hms_b"))
    val updateCreds = new Credentials()

    SparkTBinaryFrontendService.mergeHiveTokens(
      metastoreUris = "",
      newTokens = newTokens,
      oldCreds = new Credentials(),
      updateCreds = updateCreds)

    assert(updateCreds.numberOfTokens() == 2)
    assert(updateCreds.getToken(new Text("hms_a")) != null)
    assert(updateCreds.getToken(new Text("hms_b")) != null)
  }

  test("merge Hive tokens ignores signature-bound token with earlier issue date") {
    val alias = new Text("hms_a")
    val oldCreds = new Credentials()
    oldCreds.addToken(alias, newHiveToken(issueDate = 2000, service = "hms_a"))
    val newTokens = Map(alias -> newHiveToken(issueDate = 1000, service = "hms_a"))
    val updateCreds = new Credentials()

    SparkTBinaryFrontendService.mergeHiveTokens(
      metastoreUris = "",
      newTokens = newTokens,
      oldCreds = oldCreds,
      updateCreds = updateCreds)

    assert(updateCreds.getToken(alias) == null)
  }

  test("merge Hive tokens updates signature-bound token with later issue date") {
    val alias = new Text("hms_a")
    val oldCreds = new Credentials()
    oldCreds.addToken(alias, newHiveToken(issueDate = 1000, service = "hms_a"))
    val newToken = newHiveToken(issueDate = 2000, service = "hms_a")
    val updateCreds = new Credentials()

    SparkTBinaryFrontendService.mergeHiveTokens(
      metastoreUris = "",
      newTokens = Map(alias -> newToken),
      oldCreds = oldCreds,
      updateCreds = updateCreds)

    assert(updateCreds.getToken(alias) === newToken)
  }

  test("merge Hive tokens keeps single-metastore matching for default-service tokens") {
    val metastoreUris = "thrift://hms:9083"
    val defaultAlias = new Text("hive_default")
    val oldCreds = new Credentials()
    oldCreds.addToken(defaultAlias, newHiveToken(issueDate = 1000, service = ""))
    // The map key is the metastore uris the token was issued for.
    val newToken = newHiveToken(issueDate = 2000, service = "")
    val updateCreds = new Credentials()

    SparkTBinaryFrontendService.mergeHiveTokens(
      metastoreUris = metastoreUris,
      newTokens = Map(new Text(metastoreUris) -> newToken),
      oldCreds = oldCreds,
      updateCreds = updateCreds)

    assert(updateCreds.getToken(defaultAlias) === newToken)
  }

  test("merge Hive tokens adds signature-bound tokens without touching the default path") {
    val metastoreUris = "thrift://hms:9083"
    val defaultAlias = new Text("hive_default")
    val oldCreds = new Credentials()
    oldCreds.addToken(defaultAlias, newHiveToken(issueDate = 1000, service = ""))
    // Only signature-bound tokens arrive, so the default-service matching produces nothing.
    val signatureAlias = new Text("hms_a")
    val newTokens = Map(signatureAlias -> newHiveToken(issueDate = 2000, service = "hms_a"))
    val updateCreds = new Credentials()

    SparkTBinaryFrontendService.mergeHiveTokens(
      metastoreUris = metastoreUris,
      newTokens = newTokens,
      oldCreds = oldCreds,
      updateCreds = updateCreds)

    assert(updateCreds.numberOfTokens() == 1)
    assert(updateCreds.getToken(signatureAlias) != null)
    assert(updateCreds.getToken(defaultAlias) == null)
  }

  private def newHiveToken(issueDate: Long, service: String): Token[TokenIdentifier] = {
    val who = new Text("who")
    val tokenId = new HiveTokenIdent(who, who, who)
    tokenId.setIssueDate(issueDate)
    val token = new Token[TokenIdentifier]
    token.setID(tokenId.getBytes)
    token.setKind(tokenId.getKind)
    val password = new Array[Byte](128)
    Random.nextBytes(password)
    token.setPassword(password)
    if (service.nonEmpty) {
      token.setService(new Text(service))
    }
    token
  }
}
