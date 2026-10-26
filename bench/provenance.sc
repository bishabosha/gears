#!/usr/bin/env -S scala shebang
//> using scala 3.9.0
//> using dep com.lihaoyi::ujson:4.4.1

/** Writes a build's provenance record, the JSON object that build.sh and build-jvm.sh put beside each executable and
  * compare.sc saves with its results:
  *
  * {{{
  * bench/provenance.sc <file> key=string... key:=json...
  * }}}
  *
  * `key=value` gives a string, and `key:=value` a JSON value as written, such as `true` or `null`; `built`, when the
  * record was made, is added.
  */

import java.nio.file.{Files, Path}
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

if args.isEmpty then
  System.err.println("usage: provenance.sc <file> key=string... key:=json...")
  sys.exit(2)
val record = ujson.Obj()
for field <- args.tail do
  field.indexOf('=') match
    case -1                                   => System.err.println(s"not key=value: $field"); sys.exit(2)
    case at if at > 0 && field(at - 1) == ':' => record(field.take(at - 1)) = ujson.read(field.drop(at + 1))
    case at                                   => record(field.take(at)) = field.drop(at + 1)
record("built") = OffsetDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx"))
Files.writeString(Path.of(args.head), ujson.write(record, indent = 2) + "\n")
