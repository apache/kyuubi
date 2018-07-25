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
import org.apache.hadoop.yarn.api.records.{LocalResource, LocalResourceType}

/** Client side methods to setup the Hadoop distributed cache */
object KyuubiDistributedCacheManager {

  private[this] val cacheManager = new ClientDistributedCacheManager

  /**
   * Add a resource to the list of distributed cache resources. This list can
   * be sent to the ApplicationMaster and possibly the executors so that it can
   * be downloaded into the Hadoop distributed cache for use by this application.
   * Adds the LocalResource to the localResources HashMap passed in and saves
   * the stats of the resources to they can be sent to the executors and verified.
   *
   * @param fs FileSystem
   * @param conf Configuration
   * @param destPath path to the resource
   * @param localResources localResource hashMap to insert the resource into
   * @param resourceType LocalResourceType
   * @param link link presented in the distributed cache to the destination
   * @param statCache cache to store the file/directory stats
   */
  def addResource(
      fs: FileSystem,
      conf: Configuration,
      destPath: Path,
      localResources: HashMap[String, LocalResource],
      resourceType: LocalResourceType,
      link: String,
      statCache: Map[URI, FileStatus]): Unit = {
    cacheManager.addResource(fs, conf, destPath,
      localResources, resourceType, link, statCache, true)
  }
}
