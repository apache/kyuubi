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

package org.apache.spark.sql.execution.arrow

import org.apache.arrow.compression.{CommonsCompressionFactory, ZstdCompressionCodec}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}

/** Isolates the optional arrow-compression dependency so the uncompressed path never loads it. */
private[sql] object ArrowCompressionSupport {

  def createLoader(root: VectorSchemaRoot): VectorLoader = {
    new VectorLoader(root, CommonsCompressionFactory.INSTANCE)
  }

  def createZstdUnloader(root: VectorSchemaRoot, level: Int): VectorUnloader = {
    new VectorUnloader(root, true, new ZstdCompressionCodec(level), true)
  }
}
