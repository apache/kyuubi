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

import java.util.concurrent.{CountDownLatch, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kyuubi.KyuubiFunSuite

class IncrementalFetchIteratorSuite extends KyuubiFunSuite {

  test("basic append and iterate") {
    val iter = new IncrementalFetchIterator[String]()
    iter.append("a")
    iter.append("b")
    iter.append("c")

    iter.fetchNext()
    assert(iter.hasNext)
    assert(iter.next() === "a")
    assert(iter.next() === "b")
    assert(iter.next() === "c")
    assert(!iter.hasNext)
    assert(iter.getPosition === 3)
  }

  test("fetchAbsolute clamps to valid range") {
    val iter = new IncrementalFetchIterator[Int]()
    for (i <- 1 to 10) iter.append(i)

    iter.fetchAbsolute(-5)
    assert(iter.getPosition === 0)
    assert(iter.getFetchStart === 0)

    iter.fetchAbsolute(100)
    assert(iter.getPosition === 10)
    assert(iter.getFetchStart === 10)

    iter.fetchAbsolute(5)
    assert(iter.getPosition === 5)
    assert(iter.next() === 6) // 0-indexed: position 5 = element at index 5
  }

  test("fetchNext sets fetchStart to current position") {
    val iter = new IncrementalFetchIterator[String]()
    iter.append("x")
    iter.append("y")

    iter.fetchNext()
    assert(iter.getFetchStart === 0)
    iter.next()
    iter.next()
    iter.fetchNext()
    assert(iter.getFetchStart === 2)
  }

  test("concurrent append and read - producer/consumer") {
    val iter = new IncrementalFetchIterator[Int]()
    val totalItems = 10000
    val consumed = new AtomicInteger(0)
    val latch = new CountDownLatch(2)

    // Producer thread
    val producer = new Thread(() => {
      try {
        for (i <- 1 to totalItems) {
          iter.append(i)
        }
      } finally {
        latch.countDown()
      }
    })

    // Consumer thread
    val consumer = new Thread(() => {
      try {
        var count = 0
        var lastSeen = 0
        val deadline = System.currentTimeMillis() + 10000
        while (count < totalItems && System.currentTimeMillis() < deadline) {
          iter.fetchNext()
          while (iter.hasNext) {
            val v = iter.next()
            assert(v === lastSeen + 1, s"Expected ${lastSeen + 1} but got $v")
            lastSeen = v
            count += 1
          }
          if (count < totalItems) Thread.`yield`()
        }
        consumed.set(count)
      } finally {
        latch.countDown()
      }
    })

    producer.start()
    consumer.start()
    assert(latch.await(30, TimeUnit.SECONDS), "Timed out waiting for producer/consumer")

    assert(consumed.get() === totalItems, s"Expected $totalItems items, got ${consumed.get()}")
  }

  test("concurrent writer does not corrupt single reader") {
    // IncrementalFetchIterator is designed for single-consumer use.
    // This test verifies that a single reader reading concurrently with
    // a single writer does not produce exceptions or corrupt data.
    val iter = new IncrementalFetchIterator[Int]()
    val totalItems = 5000
    val barrier = new CyclicBarrier(2)
    val errors = new AtomicInteger(0)
    val pool = Executors.newFixedThreadPool(2)

    // Writer
    pool.submit(new Runnable {
      override def run(): Unit = {
        barrier.await()
        for (i <- 1 to totalItems) iter.append(i)
      }
    })

    // Single reader
    pool.submit(new Runnable {
      override def run(): Unit = {
        try {
          barrier.await()
          var count = 0
          val deadline = System.currentTimeMillis() + 10000
          while (count < totalItems && System.currentTimeMillis() < deadline) {
            iter.fetchNext()
            while (iter.hasNext) {
              iter.next()
              count += 1
            }
            if (count < totalItems) Thread.`yield`()
          }
          if (count != totalItems) errors.incrementAndGet()
        } catch {
          case _: Exception => errors.incrementAndGet()
        }
      }
    })

    pool.shutdown()
    assert(pool.awaitTermination(30, TimeUnit.SECONDS))
    assert(errors.get() === 0, s"Got ${errors.get()} errors")
  }

  test("empty iterator") {
    val iter = new IncrementalFetchIterator[String]()
    assert(!iter.hasNext)
    assert(iter.getPosition === 0)
    assert(iter.getFetchStart === 0)
  }

  test("position tracking across multiple fetch cycles") {
    val iter = new IncrementalFetchIterator[String]()
    iter.append("a")
    iter.append("b")
    iter.append("c")
    iter.append("d")

    // First fetch: read 2
    iter.fetchNext()
    iter.next() // a
    iter.next() // b
    assert(iter.getPosition === 2)
    assert(iter.getFetchStart === 0)

    // Second fetch: read remaining
    iter.fetchNext()
    assert(iter.getFetchStart === 2)
    iter.next() // c
    iter.next() // d
    assert(!iter.hasNext)
    assert(iter.getPosition === 4)
  }
}
