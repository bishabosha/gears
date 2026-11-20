package gears.async.io

import gears.async.Async

/** EXPERIMENT. A capability: the backend has threads of its own, a shard each, and performs an op on a named one, so
  * that it is listened for and settled there. What a program gains by it is that ops it keeps to one shard, a
  * connection's say, are settled by one thread, with no thread waking another.
  *
  * It says nothing of doing many ops with one wait: that is `SubmissionQueues`. A backend may have either, both or
  * neither, and `Server` picks how it serves by which.
  */
trait Shards extends Interface

object Shards {
  extension [I <: Shards](io: Io[I]) {

    /** How many shards there are, numbered from 0. */
    def shards: Int = io.submissionShards

    /** The same `Io`, its ops and its handles, with every op performed on `shard`: code written against `Io.perform`
      * is pinned by being given this.
      */
    def onShard(shard: Int): Io[io.ops.type] = {
      require(shard >= 0 && shard < io.submissionShards, s"No shard $shard of ${io.submissionShards}")
      new Io[io.ops.type] {
        val backend = io.backend.asInstanceOf[Backend { type Interface <: io.ops.type }]
        val ops: io.ops.type = io.ops
        private[io] def performWithin(op: Op, timeoutMillis: Long)(using Async): Unit =
          io.performPinned(shard, op, timeoutMillis)
        override private[io] def performPinned(shard: Int, op: Op, timeoutMillis: Long)(using Async): Unit =
          io.performPinned(shard, op, timeoutMillis)
        override private[io] def submissionShards: Int = io.submissionShards
        override private[io] def afterTurn(shard: Int)(run: Runnable): Unit = io.afterTurn(shard)(run)
        override private[io] def moveTo(shard: Int)(using Async): Unit = io.moveTo(shard)
        override private[io] def flowsAreContinuations: Boolean = io.flowsAreContinuations
        override private[io] def opsAwaitThroughAsync: Boolean = io.opsAwaitThroughAsync
      }
    }
  }
}
