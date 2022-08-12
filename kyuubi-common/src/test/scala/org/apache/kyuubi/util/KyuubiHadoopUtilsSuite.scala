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

import java.io.{DataInput, DataOutput}
import java.util.stream.StreamSupport

import scala.util.Random

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier
import org.apache.hadoop.hdfs.security.token.delegation.{DelegationTokenIdentifier => HDFSTokenIdent}
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.KyuubiDelegationTokenIdentifier

class KyuubiHadoopUtilsSuite extends KyuubiFunSuite {

  test("new hadoop conf with kyuubi conf") {
    val abc = "hadoop.abc"
    val xyz = "hadoop.xyz"
    val test = "hadoop.test"
    val kyuubiConf = new KyuubiConf()
      .set(abc, "xyz")
      .set(xyz, "abc")
      .set(test, "t")
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    assert(hadoopConf.get(abc) === "xyz")
    assert(hadoopConf.get(xyz) === "abc")
    assert(hadoopConf.get(test) === "t")
  }

  test("encode/decode credentials") {
    val identifier = new KyuubiDelegationTokenIdentifier()
    val password = new Array[Byte](128)
    Random.nextBytes(password)
    val token = new Token[KyuubiDelegationTokenIdentifier](
      identifier.getBytes,
      password,
      identifier.getKind,
      new Text(""))
    val credentials = new Credentials()
    credentials.addToken(token.getKind, token)

    val decoded = KyuubiHadoopUtils.decodeCredentials(
      KyuubiHadoopUtils.encodeCredentials(credentials))
    assert(decoded.getToken(token.getKind) == credentials.getToken(token.getKind))
  }

  test("new hadoop conf with kyuubi conf with loadDefaults") {
    val abc = "kyuubi.abc"
    val kyuubiConf = new KyuubiConf()
      .set(abc, "xyz")

    var hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    assert(StreamSupport.stream(hadoopConf.spliterator(), false)
      .anyMatch(entry => entry.getKey.startsWith("hadoop") || entry.getKey.startsWith("fs")))

    hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf, loadDefaults = false)
    assert(StreamSupport.stream(hadoopConf.spliterator(), false)
      .noneMatch(entry => entry.getKey.startsWith("hadoop") || entry.getKey.startsWith("fs")))
  }

  test("get token issue date") {
    val issueDate = System.currentTimeMillis()

    def checkIssueDate(tokenIdent: TokenIdentifier, expected: Option[Long]): Unit = {
      val hdfsToken = new Token[HDFSTokenIdent]()
      hdfsToken.setKind(tokenIdent.getKind)
      hdfsToken.setID(tokenIdent.getBytes)
      assert(KyuubiHadoopUtils.getTokenIssueDate(hdfsToken) == expected)
    }

    // DelegationTokenIdentifier found in ServiceLoader
    // Such as HDFS_DELEGATION_TOKEN, OzoneToken
    val hdfsTokenIdent = new HDFSTokenIdent()
    hdfsTokenIdent.setIssueDate(issueDate)
    checkIssueDate(hdfsTokenIdent, Some(issueDate))

    // TokenIdentifier with no issue date found in ServiceLoader
    val blockTokenIdent = new BlockTokenIdentifier()
    checkIssueDate(blockTokenIdent, None)

    // DelegationTokenIdentifier not found in ServiceLoader
    // Such as HIVE_DELEGATION_TOKEN
    val testTokenIdent = new TestDelegationTokenIdentifier()
    testTokenIdent.setIssueDate(issueDate)
    checkIssueDate(testTokenIdent, Some(issueDate))

    // DelegationTokenIdentifier with custom binary format and not found in ServiceLoader
    val testTokenIdent2 = new TestDelegationTokenIdentifier2()
    testTokenIdent2.setIssueDate(issueDate)
    checkIssueDate(testTokenIdent2, None)
  }
}

private class TestDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
  override def getKind: Text = new Text("KYUUBI_TOKEN_NOT_IN_SERVICE_LOADER")
}

private class TestDelegationTokenIdentifier2 extends AbstractDelegationTokenIdentifier {
  override def getKind: Text = new Text("KYUUBI_TOKEN_OVERRIDE_WRITE")

  override def write(out: DataOutput): Unit = {
    out.writeLong(getIssueDate)
  }

  override def readFields(in: DataInput): Unit = {
    setIssueDate(in.readLong())
  }
}
