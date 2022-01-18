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
import scala.reflect.{classTag, ClassTag}

sealed trait EventBus {
  def post[T <: KyuubiEvent](event: T): Unit
  def register[T <: KyuubiEvent: ClassTag](et: EventHandler[T]): EventBus
}

object EventBus {
  private val defaultEventBus = EventBusLive()

  def apply(): EventBus = EventBusLive()

  // Exposed api
  def post[T <: KyuubiEvent](event: T): Unit = defaultEventBus.post[T](event)
  def register[T <: KyuubiEvent: ClassTag](et: EventHandler[T]): EventBus =
    defaultEventBus.register[T](et)

  // TODO: Asynchronous execution of event handler
  private case class EventBusLive(async: Boolean = false) extends EventBus {
    private[this] val eventHandlerRegistry = new Registry

    override def post[T <: KyuubiEvent](event: T): Unit = {
      eventHandlerRegistry.lookup[T](event).foreach(_(event))
    }

    override def register[T <: KyuubiEvent: ClassTag](et: EventHandler[T]): EventBus = {
      eventHandlerRegistry.register(et)
      this
    }
  }

  private class Registry {
    private[this] val eventHandlers = mutable.Map[Class[_], List[EventHandler[_]]]()

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

  }

}
