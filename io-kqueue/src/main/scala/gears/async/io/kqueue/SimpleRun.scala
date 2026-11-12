package gears.async.io.kqueue

import gears.async.Async
import gears.async.Async.Source
import gears.async.CancellationException
import gears.async.Listener
import gears.async.io.Io
import gears.async.io.Pipes
import gears.async.io.Slot

import java.io.IOException
import scala.concurrent.TimeoutException
import scala.concurrent.duration.given
import scala.scalanative.memory.PointerBuffer
import scala.scalanative.unsafe.Zone

def testProgram(io: Io[Pipes])(using Async): Unit =
  println("here we go...")
  try
    io.perform(io.backend.ops.timer(500))
    println(s"after timer observed in thread: ${Thread.currentThread().getName}")
  catch
    case e: CancellationException =>
      println(s"timer cancelled observed in thread: ${Thread.currentThread().getName}")
    case e: TimeoutException => println(s"timer timed out observed in thread: ${Thread.currentThread().getName}")

  val pRead = Slot[io.Readable]()
  val pWrite = Slot[io.Writable]()
  io.perform(io.backend.ops.pipe(pRead, pWrite))
  val read = pRead.clear()
  val write = pWrite.clear()
  println(
    s"pipe created with read: $read, write: $write in thread: ${Thread.currentThread().getName}"
  )
  Zone {
    val ptr = summon[Zone].alloc(1024)
    val buffer = PointerBuffer.wrap(ptr, 1024)
    try io.perform(io.backend.ops.read(read, buffer))
    catch
      case e: IOException =>
        println(s"read failed observed in thread: ${Thread.currentThread().getName}. ${e.getMessage}")
  }
  io.perform(io.backend.ops.close(read))
  println(s"pipe-read closed in thread: ${Thread.currentThread().getName}")
  Zone {
    val ptr = summon[Zone].alloc(1024)
    val buffer = PointerBuffer.wrap(ptr, 1024)
    try io.perform(io.backend.ops.read(read, buffer))
    catch
      case e: IOException =>
        println(s"read failed observed in thread: ${Thread.currentThread().getName}. ${e.getMessage}")
  }
  io.perform(io.backend.ops.close(write))
  println(s"pipe-write closed in thread: ${Thread.currentThread().getName}")

  // io.perform(io.backend.ops.timer(500))
  // println("done")

@main def simpleRun(): Unit = {
  given KqueueBackend.type = KqueueBackend
  Io.run { io ?=>
    testProgram(io)
  }
}
