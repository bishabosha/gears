package gears.async.io
package node

import scala.scalajs.js

/** Node's globals that Scala.js has no facade for. */
private[node] object Node {

  /** Calls `body` on the event loop's next turn, after the I/O events of this one, and returns what cancels it. */
  def setImmediate(body: js.Function0[Unit]): js.Dynamic = js.Dynamic.global.setImmediate(body)

  def clearImmediate(immediate: js.Dynamic): Unit = js.Dynamic.global.clearImmediate(immediate): Unit
}

/** Node's `net` module: stream sockets, IP and Unix, and what else Node reads and writes as one, by descriptor. */
@js.native
@js.annotation.JSImport("node:net", js.annotation.JSImport.Namespace)
private[node] object Net extends js.Object {
  def connect(options: js.Object): js.Dynamic = js.native
  def createServer(options: js.Object): js.Dynamic = js.native
}

/** Node's `os` module. */
@js.native
@js.annotation.JSImport("node:os", js.annotation.JSImport.Namespace)
private[node] object Os extends js.Object {
  def networkInterfaces(): js.Dictionary[js.Array[js.Dynamic]] = js.native
}

/** Node's failures, JavaScript errors, as the interface's: `IOException`s. */
private[node] object Errors {

  /** The failure an error of Node's is, with its message, which names the system's error and the call. */
  def io(error: Any): java.io.IOException = error match {
    case failure: java.io.IOException  => failure
    case js.JavaScriptException(inner) => io(inner)
    case other                         =>
      val message = other.asInstanceOf[js.Dynamic].message
      new java.io.IOException(if js.typeOf(message) == "string" then message.asInstanceOf[String]
      else String.valueOf(other))
  }

  /** Whether a callback of Node's was given an error. */
  def failed(error: js.Dynamic): Boolean = error != null && !js.isUndefined(error)

  /** Runs a call of Node's that may throw a JavaScript error as one that throws an `IOException`. */
  inline def guard[T](inline body: T): T =
    try body
    catch case js.JavaScriptException(error) => throw io(error)
}

/** Node's `dgram` module: UDP sockets. */
@js.native
@js.annotation.JSImport("node:dgram", js.annotation.JSImport.Namespace)
private[node] object Dgram extends js.Object {
  def createSocket(options: js.Object): js.Dynamic = js.native
}

/** Node's `fs` module: files, by its calls that take a callback, and a few that return at once. */
@js.native
@js.annotation.JSImport("node:fs", js.annotation.JSImport.Namespace)
private[node] object Fs extends js.Object {
  def lstatSync(path: String, options: js.Object): js.UndefOr[js.Dynamic] = js.native
  def unlinkSync(path: String): Unit = js.native
  def renameSync(from: String, to: String): Unit = js.native

  val constants: js.Dynamic = js.native
  def open(path: String, flags: Int, done: js.Function2[js.Dynamic, js.Dynamic, Unit]): Unit = js.native
  def fstat(fd: Int, done: js.Function2[js.Dynamic, js.Dynamic, Unit]): Unit = js.native
  def read(
      fd: Int,
      into: js.typedarray.Uint8Array,
      offset: Int,
      length: Int,
      position: Double,
      done: js.Function2[js.Dynamic, js.Dynamic, Unit]
  ): Unit = js.native
  def write(
      fd: Int,
      from: js.typedarray.Uint8Array,
      offset: Int,
      length: Int,
      position: Double,
      done: js.Function2[js.Dynamic, js.Dynamic, Unit]
  ): Unit = js.native
  def fsync(fd: Int, done: js.Function1[js.Dynamic, Unit]): Unit = js.native
  def close(fd: Int, done: js.Function1[js.Dynamic, Unit]): Unit = js.native
}
