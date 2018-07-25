/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn

import java.net.URI

import scala.collection.mutable.{HashMap, Map}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.yarn.api.records.{LocalResource, LocalResourceType, LocalResourceVisibility}
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.spark.{KyuubiSparkUtil, SparkFunSuite}
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiDistributedCacheManagerSuite extends SparkFunSuite with MockitoSugar {

  class MockClientDistributedCacheManager extends ClientDistributedCacheManager {
    override def getVisibility(conf: Configuration, uri: URI, statCache: Map[URI, FileStatus]):
    LocalResourceVisibility = {
      LocalResourceVisibility.PRIVATE
    }
  }

  test("add resource") {
    val fs = mock[FileSystem]
    val conf = new Configuration()
    val destPath = new Path("file:///foo.bar.com:8080/tmp/testing")
    val localResources = HashMap[String, LocalResource]()
    val statCache = HashMap[URI, FileStatus]()
    val status = new FileStatus()
    when(fs.getFileStatus(destPath)).thenReturn(status)
    val fileLink = "link"
    ReflectUtils.setFieldValue(
      KyuubiDistributedCacheManager, "cacheManager", new MockClientDistributedCacheManager)
    KyuubiDistributedCacheManager.addResource(
      fs, conf, destPath, localResources, LocalResourceType.FILE, fileLink, statCache)
    val res = localResources(fileLink)
    assert(res.getVisibility === LocalResourceVisibility.PRIVATE)
    assert(ConverterUtils.getPathFromYarnURL(res.getResource) === destPath)
    assert(res.getSize === 0)
    assert(res.getTimestamp === 0)
    assert(res.getType === LocalResourceType.FILE)
    val status2 = new FileStatus(
      10, false, 1, 1024, 10,
      10, null, KyuubiSparkUtil.getCurrentUserName, null, new Path("/tmp/testing2"))
    val destPath2 = new Path("file:///foo.bar.com:8080/tmp/testing2")
    when(fs.getFileStatus(destPath2)).thenReturn(status2)
    val fileLink2 = "link2"
    KyuubiDistributedCacheManager.addResource(
      fs, conf, destPath2, localResources, LocalResourceType.FILE, fileLink2, statCache)
    val res2 = localResources(fileLink2)
    assert(res2.getVisibility === LocalResourceVisibility.PRIVATE)
    assert(ConverterUtils.getPathFromYarnURL(res2.getResource) === destPath2)
    assert(res2.getSize === 10)
    assert(res2.getTimestamp === 10)
    assert(res2.getType === LocalResourceType.FILE)
  }

  test("add resource when link null") {
    val distMgr = new MockClientDistributedCacheManager()
    val fs = mock[FileSystem]
    val conf = new Configuration()
    val destPath = new Path("file:///foo.bar.com:8080/tmp/testing")
    ReflectUtils.setFieldValue(KyuubiDistributedCacheManager, "cacheManager", distMgr)
    val localResources = HashMap[String, LocalResource]()
    val statCache = HashMap[URI, FileStatus]()
    when(fs.getFileStatus(destPath)).thenReturn(new FileStatus())
    intercept[Exception] {
      KyuubiDistributedCacheManager.addResource(
        fs, conf, destPath, localResources, LocalResourceType.FILE, null, statCache)
    }
    assert(localResources.get("link") === None)
    assert(localResources.size === 0)
  }

  test("test addResource archive") {
    val distMgr = new MockClientDistributedCacheManager()
    ReflectUtils.setFieldValue(KyuubiDistributedCacheManager, "cacheManager", distMgr)
    val fs = mock[FileSystem]
    val conf = new Configuration()
    val destPath = new Path("file:///foo.bar.com:8080/tmp/testing")
    val localResources = HashMap[String, LocalResource]()
    val statCache = HashMap[URI, FileStatus]()
    val realFileStatus = new FileStatus(10, false, 1, 1024, 10, 10, null, "testOwner",
      null, new Path("/tmp/testing"))
    when(fs.getFileStatus(destPath)).thenReturn(realFileStatus)

    KyuubiDistributedCacheManager.addResource(
      fs, conf, destPath, localResources, LocalResourceType.ARCHIVE, "link", statCache)
    val resource = localResources("link")
    assert(resource.getVisibility === LocalResourceVisibility.PRIVATE)
    assert(ConverterUtils.getPathFromYarnURL(resource.getResource) === destPath)
    assert(resource.getTimestamp === 10)
    assert(resource.getSize === 10)
    assert(resource.getType === LocalResourceType.ARCHIVE)

  }

}
