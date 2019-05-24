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

package yaooqinn.kyuubi.utils

import java.util.concurrent.{RunnableFuture, SynchronousQueue, TimeUnit}

import org.apache.spark.SparkFunSuite
import org.scalatest.BeforeAndAfterEach

class ThreadPoolWithOOMHookSuite extends SparkFunSuite with BeforeAndAfterEach {

  @volatile var flag = false
  private val oomHook = new Runnable {
    override def run(): Unit = {
      flag = true
    }
  }

  private val poolWithOOMHook = new ThreadPoolWithOOMHook(10, 10, 10, TimeUnit.SECONDS,
    new SynchronousQueue[Runnable], new NamedThreadFactory("oom"), oomHook)


  override def beforeEach(): Unit = {
    flag = false
  }

  test("out of memory") {
    val oom = new Runnable {
      override def run(): Unit = throw new OutOfMemoryError()
    }
    poolWithOOMHook.execute(oom)
    Thread.sleep(50)
    assert(flag, "out of memory occurred")
  }

  test("interrupted exception") {
    val future = new TestRunnableFuture[Int](throw new InterruptedException())
    poolWithOOMHook.execute(future)
    wait(future)
    poolWithOOMHook.execute(future)
    assert(!flag, "no oom occurred")
  }

  test("out of memory in after execute") {
    val future = new TestRunnableFuture[Int](throw new OutOfMemoryError())
    poolWithOOMHook.execute(future)
    wait(future)
    assert(flag, "oom occurred in after execute")
  }

  test("non fatal error in after execute either") {
    val future = new TestRunnableFuture[Int](throw new Exception)
    poolWithOOMHook.execute(future)
    wait(future)
    assert(!flag, "no oom occurred in after execute")
  }

  test("no exception") {
    val future = new TestRunnableFuture[Int](1)
    poolWithOOMHook.execute(future)
    wait(future)
    assert(!flag)

    val r = new Runnable {
      override def run(): Unit = {}
    }
    poolWithOOMHook.execute(r)
    assert(!flag)
  }

  test("non fatal exception") {
    val r = new Runnable {
      override def run(): Unit = throw new Exception()
    }
    poolWithOOMHook.execute(r)
    assert(!flag)
  }

  test("non fatal exception 2") {
    val future = new TestRunnableFuture2[Int](1)
    poolWithOOMHook.execute(future)
    assert(!flag)
  }

  def wait(future: RunnableFuture[Int]): Unit = {
    while (!future.isDone) {
      Thread.sleep(10)
    }
  }
}

class TestRunnableFuture[Int](f: => Int) extends RunnableFuture[Int]{
  private var done = false
  override def run(): Unit = done = true
  override def cancel(mayInterruptIfRunning: Boolean): Boolean = true
  override def isCancelled: Boolean = false
  override def isDone: Boolean = done
  override def get(): Int = f
  override def get(timeout: Long, unit: TimeUnit): Int = f
}

class TestRunnableFuture2[Int](f: => Int) extends RunnableFuture[Int]{
  private var done = false
  override def run(): Unit = done = true
  override def cancel(mayInterruptIfRunning: Boolean): Boolean = true
  override def isCancelled: Boolean = false
  override def isDone: Boolean = false
  override def get(): Int = f
  override def get(timeout: Long, unit: TimeUnit): Int = f
}
