package callers

import gears.async.io.Handles
import gears.async.io.Slot

import munit.FunSuite

/** Lives outside `gears.async.io`, where callers of the interface live. */
class SlotAccessSuite extends FunSuite {
  test("callers can take a slot's value but not write to it") {
    assertEquals(compileErrors("def read(slot: gears.async.io.Slot[Integer]): Integer = slot.clear()"), "")
    assert(compileErrors("def write(slot: gears.async.io.Slot[Integer]): Unit = slot.set(Integer.valueOf(1))").nonEmpty)
    assertEquals(compileErrors("def read(b: Handles)(slot: Slot[b.Handle]): b.Handle = slot.clear()"), "")
    assert(compileErrors("def write(b: Handles)(slot: Slot[b.Handle], h: b.Handle): Unit = slot.set(h)").nonEmpty)
  }

  test("callers can forge a handle neither from a number nor from another object, nor take one's number") {
    assert(compileErrors("def forge(b: Handles): b.Handle = 1").nonEmpty)
    assert(compileErrors("def forge(b: Handles): b.Handle = new AnyRef").nonEmpty)
    assert(compileErrors("def number(b: Handles)(h: b.Handle): Int = h").nonEmpty)
  }

  test("a handle of one backend is not a handle of another") {
    assert(
      compileErrors("def mix(a: Handles, b: Handles)(h: a.Handle): b.Handle = h").nonEmpty
    )
  }
}
