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

import org.apache.hadoop.hdfs.security.token.delegation.{DelegationTokenIdentifier => HDFSTokenIdent}
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
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

    // Typical DelegationTokenIdentifier
    val hdfsTokenIdent = new HDFSTokenIdent()
    hdfsTokenIdent.setIssueDate(issueDate)
    val hdfsToken = new Token[HDFSTokenIdent]()
    hdfsToken.setKind(hdfsTokenIdent.getKind)
    hdfsToken.setID(hdfsTokenIdent.getBytes)
    assert(KyuubiHadoopUtils.getTokenIssueDate(hdfsToken).get == issueDate)

    // DelegationTokenIdentifier with overridden `write` method
    val testTokenIdent = new TestDelegationTokenIdentifier()
    testTokenIdent.setIssueDate(issueDate)
    val testToken = new Token[TestDelegationTokenIdentifier]()
    testToken.setKind(testTokenIdent.getKind)
    testToken.setID(testTokenIdent.getBytes)
    assert(KyuubiHadoopUtils.getTokenIssueDate(testToken).get == issueDate)

    // TokenIdentifier with no issue date
    val testSimpleTokenIdent = new TestSimpleTokenIdentifier()
    val testSimpleToken = new Token[TestSimpleTokenIdentifier]()
    testSimpleToken.setKind(testSimpleTokenIdent.getKind)
    testSimpleToken.setID(testSimpleTokenIdent.getBytes)
    assert(KyuubiHadoopUtils.getTokenIssueDate(testSimpleToken).isEmpty)
  }
}

private class TestDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
  override def getKind: Text = new Text("KYUUBI_TEST_GET_ISSUE_DATE")

  override def write(out: DataOutput): Unit = {
    out.writeLong(getIssueDate)
  }

  override def readFields(in: DataInput): Unit = {
    setIssueDate(in.readLong())
  }
}

private class TestSimpleTokenIdentifier extends TokenIdentifier {
  override def getKind: Text = new Text("KYUUBI_TEST_SIMPLE")

  override def getUser: UserGroupInformation = UserGroupInformation.getCurrentUser

  override def write(dataOutput: DataOutput): Unit = {}

  override def readFields(dataInput: DataInput): Unit = {}
}
