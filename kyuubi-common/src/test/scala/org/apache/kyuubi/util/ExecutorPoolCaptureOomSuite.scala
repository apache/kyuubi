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

import java.util.concurrent.{RunnableFuture, TimeUnit}

import org.apache.kyuubi.KyuubiFunSuite

class ExecutorPoolCaptureOomSuite extends KyuubiFunSuite {

  @volatile var flag = false
  private val oomHook = new Runnable {
    override def run(): Unit = {
      flag = true
    }
  }

  override def beforeEach(): Unit = {
    flag = false
  }

  private val pool = ExecutorPoolCaptureOom(getClass.getName, 10, 10, 10, oomHook)

  test("t is RuntimeException, r is not future") {
    val exception = new RuntimeException()
    pool.execute(() => {
      throw exception
    })
    checkFalse()

    pool.afterExecute(null, exception)
    checkFalse()
  }

  test("t is OutOfMemoryError, r is not future") {
    val error = new OutOfMemoryError()
    pool.execute(() => {
      throw error
    })

    checkTrue()
    flag = false
    pool.afterExecute(null, error)
    checkTrue()
  }

  test("t is null, r is not future") {
    pool.execute(() => ())
    checkFalse()

    pool.afterExecute(null, null)
    checkFalse()
  }

  test("t is null, r is future with no exception") {
    val future = new TestRunnableFuture(1)
    pool.execute(future)
    wait(future)
    checkFalse()
  }

  test("t is null, r is future throw InterruptedException") {
    val future = new TestRunnableFuture(throw new InterruptedException())
    pool.execute(future)
    checkFalse()
  }

  test("t is null, r is future throw OutOfMemoryError") {
    val future = new TestRunnableFuture(throw new OutOfMemoryError())
    pool.execute(future)
    wait(future)
    checkTrue()
  }

  test("t is null, r is future throw RuntimeException") {
    val future = new TestRunnableFuture(throw new RuntimeException)
    pool.execute(future)
    wait(future)
    checkFalse()
  }

  def wait(future: RunnableFuture[_]): Unit = {
    while (!future.isDone) {
      Thread.sleep(10)
    }
  }

  def checkFalse(): Unit = {
    Thread.sleep(50)
    assert(!flag)
  }

  def checkTrue(): Unit = {
    Thread.sleep(50)
    assert(flag)
  }
}

class TestRunnableFuture[T](f: => T, isDone: Boolean = true) extends RunnableFuture[T]{
  override def run(): Unit = {}
  override def cancel(mayInterruptIfRunning: Boolean): Boolean = !isDone
  override def isCancelled: Boolean = isDone
  override def isDone: Boolean = isDone
  override def get(): T = f
  override def get(timeout: Long, unit: TimeUnit): T = f
}
