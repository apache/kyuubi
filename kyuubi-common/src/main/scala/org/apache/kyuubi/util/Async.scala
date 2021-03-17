package org.apache.kyuubi.util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue, ThreadFactory}

import cn.t3go.krill.common.Logging

object Async extends Logging {

  type Op = () => Unit

  private val threadPool: ExecutorService = Executors.newSingleThreadExecutor(new DaemonFactory("Async-Pool"))
  private val operationList = new LinkedBlockingQueue[Op](1000)
  val opNumber = new AtomicInteger(0)

  def async(runner: Op): Unit = {
    operationList.put(runner)
    opNumber.incrementAndGet()
  }

  threadPool.submit(new Runnable {
    override def run(): Unit = {
      while (true) {
        try {
          val op = operationList.take()
          op.apply()
        } catch {
          case e: Throwable =>
            error("Error to run async operation", e)
        } finally {
          opNumber.decrementAndGet()
        }
      }
    }
  })
}

case class DaemonFactory(name: String) extends ThreadFactory {
  override def newThread(r: Runnable): Thread = {
    val t = new Thread(r)
    t.setName(name + "-" + t.getId)
    t.setDaemon(true)
    t
  }
}