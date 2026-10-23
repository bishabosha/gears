package callers

import munit.FunSuite

/** Lives outside `gears.async.io`, where callers of the interface live. */
class SlotAccessSuite extends FunSuite {
  test("callers can take a slot's value but not write to it") {
    assertEquals(compileErrors("def read(slot: gears.async.io.Slot[Integer]): Integer = slot.clear()"), "")
    assert(compileErrors("def write(slot: gears.async.io.Slot[Integer]): Unit = slot.set(Integer.valueOf(1))").nonEmpty)
    assertEquals(compileErrors("def read(slot: gears.async.io.Handle.Slot): gears.async.io.Handle = slot.clear()"), "")
    assert(compileErrors("def write(slot: gears.async.io.Handle.Slot): Unit = slot.set(1)").nonEmpty)
  }

  test("callers can neither forge a handle from a number nor take a handle's number") {
    assert(compileErrors("def forge(): gears.async.io.Handle = 1").nonEmpty)
    assert(compileErrors("def number(h: gears.async.io.Handle): Int = h").nonEmpty)
    assert(compileErrors("def forge(): gears.async.io.Handle = gears.async.io.Handle.wrap(1)").nonEmpty)
    assert(compileErrors("def number(h: gears.async.io.Handle): Int = gears.async.io.Handle.unwrap(h)").nonEmpty)
  }
}
