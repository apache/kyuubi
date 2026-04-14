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

package org.apache.kyuubi.engine.dataagent.operation

import org.apache.kyuubi.KyuubiFunSuite

/**
 * Tests for the compaction behavior of [[IncrementalFetchIterator]].
 * Compaction removes consumed entries from the front of the internal buffer
 * when the consumed prefix exceeds COMPACT_THRESHOLD (1024).
 */
class IncrementalFetchIteratorCompactionSuite extends KyuubiFunSuite {

  private val COMPACT_THRESHOLD = 1024

  test("compaction triggers after consuming >= 1024 items") {
    val iter = new IncrementalFetchIterator[Int]()
    val total = 2000

    // Append 2000 items
    for (i <- 0 until total) iter.append(i)

    // Consume first 1024 items
    iter.fetchNext()
    for (_ <- 0 until COMPACT_THRESHOLD) {
      assert(iter.hasNext)
      iter.next()
    }
    // Position should be at 1024
    assert(iter.getPosition === COMPACT_THRESHOLD)

    // Call fetchNext() which triggers compactIfNeeded()
    iter.fetchNext()
    assert(iter.getFetchStart === COMPACT_THRESHOLD)

    // Verify remaining items are still accessible and correct
    var count = 0
    while (iter.hasNext) {
      val v = iter.next()
      assert(v === COMPACT_THRESHOLD + count, s"Expected ${COMPACT_THRESHOLD + count} but got $v")
      count += 1
    }
    assert(count === total - COMPACT_THRESHOLD)
    assert(iter.getPosition === total)
  }

  test("no compaction when consumed items below threshold") {
    val iter = new IncrementalFetchIterator[Int]()

    // Append 500 items (below threshold)
    for (i <- 0 until 500) iter.append(i)

    // Consume all 500
    iter.fetchNext()
    for (i <- 0 until 500) {
      assert(iter.next() === i)
    }

    // fetchNext triggers compactIfNeeded, but 500 < 1024 so no compaction
    iter.fetchNext()

    // Verify position tracking is correct
    assert(iter.getPosition === 500)
    assert(iter.getFetchStart === 500)
    assert(!iter.hasNext)
  }

  test("compaction preserves logical position correctness") {
    val iter = new IncrementalFetchIterator[Int]()
    // Use an exact multiple of COMPACT_THRESHOLD to avoid partial batch issues
    val total = 3 * COMPACT_THRESHOLD // 3072

    for (i <- 0 until total) iter.append(i)

    // Read in batches, triggering multiple compactions
    var nextExpected = 0
    for (_ <- 0 until 3) {
      iter.fetchNext()
      for (_ <- 0 until COMPACT_THRESHOLD) {
        assert(iter.hasNext)
        val v = iter.next()
        assert(v === nextExpected, s"Expected $nextExpected but got $v")
        nextExpected += 1
      }
    }

    // All items consumed
    iter.fetchNext()
    assert(!iter.hasNext)
    assert(nextExpected === total, s"Expected to read $total items, read $nextExpected")
  }

  test("fetchAbsolute works correctly after compaction") {
    val iter = new IncrementalFetchIterator[Int]()
    val total = 2048

    for (i <- 0 until total) iter.append(i)

    // Consume 1024 to trigger compaction
    iter.fetchNext()
    for (_ <- 0 until COMPACT_THRESHOLD) iter.next()
    iter.fetchNext() // triggers compaction

    // fetchAbsolute to a position within the remaining range
    iter.fetchAbsolute(1500)
    assert(iter.getPosition === 1500)
    assert(iter.hasNext)
    assert(iter.next() === 1500)

    // fetchAbsolute to position 0 -- before the compacted range,
    // should be clamped to trimmedCount (1024)
    iter.fetchAbsolute(0)
    assert(iter.getPosition === COMPACT_THRESHOLD)
    assert(iter.hasNext)
    assert(iter.next() === COMPACT_THRESHOLD)
  }

  test("interleaved append and consume with compaction") {
    val iter = new IncrementalFetchIterator[Int]()

    // Append 1024 items
    for (i <- 0 until COMPACT_THRESHOLD) iter.append(i)

    // Consume them all
    iter.fetchNext()
    for (i <- 0 until COMPACT_THRESHOLD) {
      assert(iter.next() === i)
    }

    // Append another 1024
    for (i <- COMPACT_THRESHOLD until 2 * COMPACT_THRESHOLD) iter.append(i)

    // fetchNext triggers compaction of first 1024
    iter.fetchNext()

    // Verify second batch is accessible
    for (i <- COMPACT_THRESHOLD until 2 * COMPACT_THRESHOLD) {
      assert(iter.hasNext)
      assert(iter.next() === i)
    }
    assert(!iter.hasNext)
    assert(iter.getPosition === 2 * COMPACT_THRESHOLD)
  }
}
