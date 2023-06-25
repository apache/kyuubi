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

package org.apache.kyuubi.events

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.reflect.{classTag, ClassTag}
import scala.util.Try

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ASYNC_EVENT_HANDLER_KEEPALIVE_TIME, ASYNC_EVENT_HANDLER_POLL_SIZE, ASYNC_EVENT_HANDLER_WAIT_QUEUE_SIZE}
import org.apache.kyuubi.events.handler.EventHandler
import org.apache.kyuubi.util.ThreadUtils

/**
 * The [[EventBus]] is responsible for triggering Kyuubi event, registering event handlers and
 * distributing events to the corresponding event handlers to consume it,
 * currently offers both synchronous and asynchronous modes.
 */
sealed trait EventBus {

  def post[T <: KyuubiEvent](event: T): Unit

  def register[T <: KyuubiEvent: ClassTag](eventHandler: EventHandler[T]): EventBus

  def registerAsync[T <: KyuubiEvent: ClassTag](eventHandler: EventHandler[T]): EventBus

  def deregisterAll(): Unit = {}
}

object EventBus extends Logging {
  private val defaultEventBus = EventBusLive()
  private val conf: KyuubiConf = KyuubiConf().loadFileDefaults()

  private val poolSize = conf.get(ASYNC_EVENT_HANDLER_POLL_SIZE)
  private val waitQueueSize = conf.get(ASYNC_EVENT_HANDLER_WAIT_QUEUE_SIZE)
  private val keepAliveMs = conf.get(ASYNC_EVENT_HANDLER_KEEPALIVE_TIME)

  implicit private lazy val asyncEventExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(ThreadUtils.newDaemonQueuedThreadPool(
      poolSize,
      waitQueueSize,
      keepAliveMs,
      "async-event-handler-pool"))

  def apply(): EventBus = EventBusLive()

  // Exposed api
  def post[T <: KyuubiEvent](event: T): Unit = defaultEventBus.post[T](event)

  def register[T <: KyuubiEvent: ClassTag](et: EventHandler[T]): EventBus =
    defaultEventBus.register[T](et)

  def registerAsync[T <: KyuubiEvent: ClassTag](et: EventHandler[T]): EventBus =
    defaultEventBus.registerAsync[T](et)

  def deregisterAll(): Unit = synchronized {
    defaultEventBus.deregisterAll()
  }

  private case class EventBusLive() extends EventBus {
    private[this] lazy val eventHandlerRegistry = new Registry
    private[this] lazy val asyncEventHandlerRegistry = new Registry

    override def post[T <: KyuubiEvent](event: T): Unit = {
      asyncEventHandlerRegistry.lookup[T](event).foreach { f =>
        Future(f(event)).recover {
          case e: Throwable =>
            error("An error occurred during async event handler execution", e)
        }
      }
      eventHandlerRegistry.lookup[T](event).foreach { f =>
        Try(f(event)).recover {
          case e: Throwable =>
            error("An error occurred during sync event handler execution", e)
        }
      }
    }

    override def register[T <: KyuubiEvent: ClassTag](et: EventHandler[T]): EventBus = {
      eventHandlerRegistry.register(et)
      this
    }

    override def registerAsync[T <: KyuubiEvent: ClassTag](et: EventHandler[T]): EventBus = {
      asyncEventHandlerRegistry.register(et)
      this
    }

    override def deregisterAll(): Unit = {
      eventHandlerRegistry.deregisterAll()
      asyncEventHandlerRegistry.deregisterAll()
    }
  }

  private class Registry {
    private[this] lazy val eventHandlers = mutable.Map[Class[_], List[EventHandler[_]]]()

    def register[T <: KyuubiEvent: ClassTag](et: EventHandler[T]): Unit = {
      val clazz = classTag[T].runtimeClass
      val existEts = eventHandlers.getOrElse(clazz, Nil)
      eventHandlers.put(clazz, existEts :+ et)
    }

    def lookup[T <: KyuubiEvent](event: T): List[EventHandler[T]] = {
      val clazz = event.getClass
      for {
        parent <- getAllParentsClass(clazz)
        et <- eventHandlers.getOrElse(parent, Nil).asInstanceOf[List[EventHandler[T]]]
      } yield et
    }

    def getAllParentsClass(clazz: Class[_]): List[Class[_]] = {
      val parents = for {
        cls <- Option(clazz.getSuperclass).toList ++ clazz.getInterfaces.toList
        parent <- getAllParentsClass(cls)
      } yield parent
      clazz :: parents
    }

    def deregisterAll(): Unit = {
      eventHandlers.values.flatten.foreach(_.close())
      eventHandlers.clear()
    }
  }
}
