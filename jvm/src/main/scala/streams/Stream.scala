package gears.async.streams

import gears.async.Async
import gears.async.io.Io
import gears.async.io.{Backend, Interface, RegularFiles}

import scala.language.experimental.modularity
import scala.util.boundary

import IOApp.IO

object IOApp {
  type Backend = gears.async.io.Backend { type Interface <: Platform }
  type Platform = Interface & RegularFiles

  type IO[A] = RIO[Platform, A]
  type RIO[I <: Interface, A] = (Async, Io[I]) ?=> A

  trait Simple {
    protected given backend: Backend = compiletime.deferred

    def run: IO[Unit]

    final def main(args: Array[String]): Unit =
      Io.run(run)
  }
}

/** A stream of `A`s, in direct style: a description of a loop, which runs when it is compiled and run
  * (`compile.drain`), on the caller's own stack, and may be run again. A stream pushes: running it calls the consumer
  * with one chunk after another (`foreachChunk`), so a stage is a method call per chunk, with nothing allocated per
  * element and no queue between stages, and a source that must wait, for a file's bytes say, suspends the caller as any
  * op of its `Io` does. What a stream opens it closes as its run unwinds, whether the run ends, fails, is cancelled, or
  * is stopped early by a stage that has had enough (`take`).
  *
  * `I` is what the stream needs of the `Io` it is run with, so one that needs less runs where there is more.
  */
trait Stream[-I <: Interface, +A] { self =>

  /** Runs the stream, giving `sink` its chunks in order, none of them empty, and returns at its end. */
  def foreachChunk(sink: Chunk[A] => Unit)(using Async, Io[I]): Unit

  /** Each chunk through `f`; a chunk that comes out empty is dropped. */
  def mapChunks[B](f: Chunk[A] => Chunk[B]): Stream[I, B] = new Stream[I, B] {
    def foreachChunk(sink: Chunk[B] => Unit)(using Async, Io[I]): Unit =
      self.foreachChunk { chunk =>
        val out = f(chunk)
        if out.nonEmpty then sink(out)
      }
  }

  def filter(p: A => Boolean): Stream[I, A] = mapChunks(_.filter(p))

  def map[B](f: A => B): Stream[I, B] = mapChunks(_.map(f))

  /** As `map`, with an `f` that performs ops of its own, with the `Io` the stream is run with. */
  def evalMap[J <: I, B](f: A => IOApp.RIO[J, B]): Stream[J, B] = new Stream[J, B] {
    def foreachChunk(sink: Chunk[B] => Unit)(using Async, Io[J]): Unit =
      self.foreachChunk(chunk => sink(chunk.map(a => f(a))))
  }

  /** The stream `f` makes of each element, one after another. */
  def flatMap[J <: I, B](f: A => Stream[J, B]): Stream[J, B] = new Stream[J, B] {
    def foreachChunk(sink: Chunk[B] => Unit)(using Async, Io[J]): Unit =
      self.foreachChunk(_.foreach(a => f(a).foreachChunk(sink)))
  }

  /** This stream, then `that`. */
  def ++[J <: I, B >: A](that: => Stream[J, B]): Stream[J, B] = new Stream[J, B] {
    def foreachChunk(sink: Chunk[B] => Unit)(using Async, Io[J]): Unit = {
      self.foreachChunk(sink)
      that.foreachChunk(sink)
    }
  }

  /** `elem` between each two elements. */
  def intersperse[B >: A](elem: B): Stream[I, B] = new Stream[I, B] {
    def foreachChunk(sink: Chunk[B] => Unit)(using Async, Io[I]): Unit = {
      val separator = elem.asInstanceOf[AnyRef]
      var first = true
      self.foreachChunk { chunk =>
        val n = chunk.size
        if n > 0 then {
          val out = new Array[AnyRef](2 * n)
          var i = 0
          while i < n do {
            out(2 * i) = separator
            out(2 * i + 1) = chunk(i).asInstanceOf[AnyRef]
            i += 1
          }
          if first then {
            first = false
            sink(new Chunk.Boxed[B](out, 1, 2 * n - 1))
          } else sink(new Chunk.Boxed[B](out, 0, 2 * n))
        }
      }
    }
  }

  /** The first `n` elements. Once it has them the stream before it is stopped, unwinding as it would at a failure, so
    * what it opened is closed, and nothing more is read.
    */
  def take(n: Long): Stream[I, A] = new Stream[I, A] {
    def foreachChunk(sink: Chunk[A] => Unit)(using Async, Io[I]): Unit =
      if n > 0 then {
        var left = n
        boundary {
          self.foreachChunk { chunk =>
            if chunk.size < left then {
              left -= chunk.size
              sink(chunk)
            } else {
              sink(chunk.take(left.toInt))
              boundary.break()
            }
          }
        }
      }
  }

  def through[J <: Interface, B](f: Stream[I, A] => Stream[J, B]): Stream[J, B] = f(this)

  /** The ways of running this stream to its end. */
  def compile: StreamCompiler[I, A] = new StreamCompiler(this)
}

object Stream {
  private val Empty: Stream[Interface, Nothing] = new Stream[Interface, Nothing] {
    def foreachChunk(sink: Chunk[Nothing] => Unit)(using Async, Io[Interface]): Unit = ()
  }

  def empty[A]: Stream[Interface, A] = Empty

  def apply[A](as: A*): Stream[Interface, A] = chunk(Chunk.from(as))

  def chunk[A](as: Chunk[A]): Stream[Interface, A] =
    if as.isEmpty then Empty
    else
      new Stream[Interface, A] {
        def foreachChunk(sink: Chunk[A] => Unit)(using Async, Io[Interface]): Unit = sink(as)
      }

  /** A stream of the one value `body` returns, evaluated each time the stream is run. */
  def eval[I <: Interface, A](body: IOApp.RIO[I, A]): Stream[I, A] = new Stream[I, A] {
    def foreachChunk(sink: Chunk[A] => Unit)(using Async, Io[I]): Unit = sink(Chunk.single(body))
  }
}

/** Runs a stream to its end, for what each method makes of its elements. */
final class StreamCompiler[-I <: Interface, +A](stream: Stream[I, A]) {

  /** Runs the stream for what it does, dropping its elements. */
  def drain: IOApp.RIO[I, Unit] = stream.foreachChunk(_ => ())

  def foreach(f: A => Unit): IOApp.RIO[I, Unit] = stream.foreachChunk(_.foreach(f))

  def fold[B](init: B)(f: (B, A) => B): IOApp.RIO[I, B] = {
    var acc = init
    stream.foreachChunk(chunk => acc = chunk.foldLeft(acc)(f))
    acc
  }

  def toList: IOApp.RIO[I, List[A]] = {
    val out = List.newBuilder[A]
    stream.foreachChunk(_.foreach(out += _))
    out.result()
  }
}

trait Converter extends IOApp.Simple {

  val converter: Stream[IOApp.Platform, Unit] = {
    def fahrenheitToCelsius(f: Double): Double =
      (f - 32.0) * (5.0 / 9.0)

    Files[IOApp.Platform]
      .readUtf8Lines(Path("testdata/fahrenheit.txt"))
      .filter(s => !s.trim.isEmpty && !s.startsWith("//"))
      .map(line => fahrenheitToCelsius(line.toDouble).toString)
      .intersperse("\n")
      .through(text.utf8.encode)
      .through(Files[IOApp.Platform].writeAll(Path("testdata/celsius.txt")))
  }

  def run: IO[Unit] =
    converter.compile.drain
}
