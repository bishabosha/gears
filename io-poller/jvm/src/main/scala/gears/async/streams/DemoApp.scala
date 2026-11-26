package gears.async.streams

import scala.language.experimental.modularity

given IOApp.Backend = gears.async.io.nio.NioBackend

object ConverterConcrete extends Converter
