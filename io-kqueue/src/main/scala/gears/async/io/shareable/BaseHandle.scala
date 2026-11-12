package gears.async.io.shareable

import java.util.concurrent.atomic.AtomicInteger

object HandleState:
  inline val InitialState = 0
  inline val OpenState = 1
  inline val ClosedState = -1
  type State = InitialState.type | OpenState.type | ClosedState.type

  sealed abstract class Holder:
    private val state = AtomicInteger(InitialState)
    final def getState: State = state.get().asInstanceOf[State]
    final def setState(newState: State): Unit = state.set(newState)
    final def compareAndSetState(expected: State, newState: State): Boolean =
      state.compareAndSet(expected, newState)
    final def compareAndExchangeState(expected: State, newState: State): State =
      state.compareAndExchange(expected, newState).asInstanceOf[State]

trait BaseHandle extends HandleState.Holder with AutoCloseable:
  def open(): Unit

trait BaseReadableHandle extends BaseHandle
trait BaseWritableHandle extends BaseHandle

abstract class AbstractBaseHandle extends BaseHandle:
  def cleanup(): Unit

  final def open(): Unit =
    if compareAndExchangeState(HandleState.InitialState, HandleState.OpenState) == HandleState.InitialState then ()
    else throw new IllegalStateException("Handle is already open or closed")

  final def close(): Unit =
    if compareAndSetState(HandleState.OpenState, HandleState.ClosedState) then cleanup()
    else () // ignore spurious close attempts
