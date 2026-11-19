package java.nio.channels

/** The JDK's failure of an op on a closed channel, which Scala.js's library has not: what `gears.async.io` fails an op
  * on a closed handle with on every platform (see `Handles.close`), so that a program, and the testkit's contracts,
  * catch the same class on Scala.js as on the JVM and Scala Native.
  */
class ClosedChannelException extends java.io.IOException
