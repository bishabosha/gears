import org.scalajs.jsenv.nodejs.*
import org.scalajs.linker.interface.ESVersion
import sbtcrossproject.CrossPlugin.autoImport.{CrossType, crossProject}

scalaVersion := "3.9.0"

// The Scala Native nightly selected in project/plugins.sbt brings its libraries from the snapshot repository.
ThisBuild / resolvers ++= (if (sys.env.get("SCALA_NATIVE_NIGHTLY").contains("1")) Seq(Resolver.sonatypeCentralSnapshots)
                           else Nil)

LocalRootProject / publish / skip := true

val MUnitFramework = new TestFramework("munit.Framework")

// Common publishing settings for all platforms.
organization := "ch.epfl.lamp"
homepage := Some(uri("https://lampepfl.github.io/gears"))
licenses := List(License.Apache2)
developers := List(
  Developer("natsukagami", "Natsu Kagami", "nki@fastmail.com", uri("https://github.com/natsukagami"))
)

lazy val root =
  crossProject(JSPlatform, JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("."))
    .settings(
      Seq(
        name := "Gears",
        publish / skip := false,
        versionScheme := Some("early-semver"),
        libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
        testFrameworks += MUnitFramework
      )
    )
    .jvmSettings(
      Seq(
        scalacOptions += "-release:21"
      )
    )
    .nativeSettings(
      Seq(
        nativeConfig ~= { c =>
          c.withMultithreading(true)
        }
      )
    )
    .jsSettings(jsWasmSettings)

// Emit ES modules with the Wasm backend, run on Node.js 26+, which supports Wasm and JSPI, including the nested async
// stack fix.
lazy val jsWasmSettings = Seq(
  scalaJSLinkerConfig := {
    scalaJSLinkerConfig.value
      .withESFeatures(_.withESVersion(ESVersion.ES2022).withUseWebAssembly(true))
      .withWasmFeatures(_.withUseJSPI(true)) // enable js.async/js.await
      .withModuleKind(ModuleKind.ESModule) // required by the Wasm backend
  },
  jsEnv := Def.uncached {
    val config = NodeJSEnv
      .Config()
      .withArgs(
        List(
          "--stack-size=204800"
        )
      )
    new NodeJSEnv(config)
  }
)

// gears' I/O interface lives in the core project, in `gears.async.io` (shared, so every platform has it). Backends live
// in projects of their own, each in a subpackage of `gears.async.io`, which is what lets them reach what the interface
// keeps `private[io]`; the reactor backends share a runtime (`ioRuntime`, in `gears.async.io.internal`).

val munit = "org.scalameta" %% "munit" % "1.3.6"

val mainargs = "com.lihaoyi" %% "mainargs" % "0.7.8"

// What every backend is checked against, published beside the core so that a backend written elsewhere can check
// itself against the same contracts: the contract suites (`gears.async.io.testkit`), `BufferArena`, buffers each
// platform allocates, the gears demos (`gears.io.demos`) and `SimpleIO`, with `SimpleIOChecks`, in `gears.io.testkit`.
// The demos and `SimpleIO` live outside `gears.async`, so they see only the public `gears.async.io` interface. Each
// takes the `Io` and `Async` it runs with, and a backend's demo application is what starts their runs. Built for every platform
// the core is, though only the JVM has a backend to run them on yet.
lazy val ioTestkit =
  crossProject(JSPlatform, JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("io-testkit"))
    .settings(
      name := "gears-io-testkit",
      publish / skip := false,
      versionScheme := Some("early-semver"),
      libraryDependencies ++= Seq(munit, mainargs),
      testFrameworks += MUnitFramework
    )
    .jvmSettings(scalacOptions += "-release:21")
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .jsSettings(jsWasmSettings)
    .dependsOn(root)

// The reference backend: the semantics of the interface on the JDK's channels, over a plain selector poll, with no
// regard for speed. It is what the contracts are checked against, not a backend to deploy.
lazy val ioReference =
  (project in file("io-reference"))
    .settings(
      publish / skip := true,
      scalacOptions += "-release:21",
      libraryDependencies += munit % Test,
      testFrameworks += MUnitFramework,
      Test / fork := true
    )
    .dependsOn(root.jvm, ioTestkit.jvm % Test)

// The reference backend's demo application: the testkit's gears demos over it, as `ioReferenceDemos/run <command>`.
lazy val ioReferenceDemos =
  (project in file("io-reference/demos"))
    .settings(
      publish / skip := true,
      scalacOptions += "-release:21",
      Compile / mainClass := Some("referencedemos.ReferenceDemos"),
      run / fork := true
    )
    .dependsOn(ioReference, ioTestkit.jvm)

// Settings of the I/O libraries published beside the core, under its organization and versions.
def ioLibrary(artifact: String) = Seq(
  name := artifact,
  publish / skip := false,
  versionScheme := Some("early-semver")
)

// Opens what `Carriers.Reactors` needs of the JDK: a virtual thread's scheduler, so the pool can carry virtual threads,
// and the pin that keeps one on its worker while an op is submitted there (see `VirtualCarriers`).
val reactorCarrierOpens = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.vm=ALL-UNNAMED"
)

// The runtime of the reactor backends, in `gears.async.io.internal`: reactors and their ops, the pool of workers that
// drives them and runs a program's futures, and `Backend.run` for a reactor backend (`ReactorBackend`) and a backend
// that performs each op on its caller (`CallerBackend`). On Scala Native, also the Posix sockets, files and descriptors
// that its backends share. Not part of the interface: a backend built on it depends on it.
lazy val ioRuntime =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("io-runtime"))
    .settings(
      ioLibrary("gears-io-runtime"),
      libraryDependencies += munit % Test,
      testFrameworks += MUnitFramework
    )
    .jvmSettings(
      scalacOptions += "-release:21",
      Test / fork := true,
      Test / javaOptions ++= reactorCarrierOpens
    )
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .dependsOn(root, ioTestkit % Test)

// The kqueue backend (macOS, Scala Native). Its native resources override `setjmp` and `longjmp` in every program that
// links it with `_setjmp` and `_longjmp`, which leave the signal mask alone (see setjmp_override.S).
lazy val ioKqueue =
  (project in file("io-backends/kqueue"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      ioLibrary("gears-io-kqueue"),
      libraryDependencies += munit % Test,
      testFrameworks += MUnitFramework,
      // Its tests run reactors on several threads.
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(ioRuntime.native % "compile->compile;test->test", ioTestkit.native % Test)

// The JVM's backends: NIO selectors over the JDK's selectable channels, and threads for what the JDK can only block
// on (`NioBackend`), or each op on the calling thread (`NioOnCaller`).
lazy val ioNio =
  (project in file("io-backends/nio"))
    .settings(
      ioLibrary("gears-io-nio"),
      scalacOptions += "-release:21",
      libraryDependencies += munit % Test,
      testFrameworks += MUnitFramework,
      Test / fork := true,
      Test / javaOptions ++= reactorCarrierOpens
    )
    .dependsOn(ioRuntime.jvm % "compile->compile;test->test", ioTestkit.jvm % Test)

// The plain kqueue demos, over the kqueue backend's own types with no gears, as the executable `iokqueuedemos`, which
// also runs the testkit's gears demos over kqueue as `iokqueuedemos gears <command>`: kqueue's demo application.
// ported from https://github.com/bishabosha/scala-native-async-io/commit/6e8320d36c71f75c9daf85065f5c89729f0825c0
lazy val ioKqueueDemos =
  (project in file("io-backends/kqueue/demos"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("example.Main"),
      // The gears demos run futures on a pool of worker threads.
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(ioKqueue, ioTestkit.native)

// NIO's demo application: the testkit's gears demos over NIO's backends, `NioOnCaller` (`--backend caller`, the
// default) and `NioBackend` (`reactors`), as `ioNioDemos/run <command>`.
lazy val ioNioDemos =
  (project in file("io-backends/nio/demos"))
    .settings(
      publish / skip := true,
      scalacOptions += "-release:21",
      Compile / mainClass := Some("niodemos.NioDemos"),
      run / fork := true,
      // So that `--backend reactors` can carry the futures on the reactor pool.
      run / javaOptions ++= reactorCarrierOpens
    )
    .dependsOn(ioNio, ioTestkit.jvm)

// The gears bench (`GearsBench`, see docs/io/bench.md), its scenarios written against the interface alone as the demos
// are, over the backends of each platform's demo application: on Scala Native the executable `iobench` (bench/build.sc),
// whose `iobench gears bench` runs them over kqueue, `iobench bench` the same scenarios against `KqueueLoop` directly,
// and `iobench gears move-bench` kqueue's moves between reactors; on the JVM (bench/build_jvm.sc) over NIO, with the
// same scenarios on the JDK alone (`jdk-bench`). The cats-effect version of the scenarios is `ceBench`, beside it.
lazy val ioBench =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("bench/io-bench"))
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("gearsbench.Main")
    )
    .jvmSettings(
      run / fork := true,
      // So that `--backend reactors` can carry the futures on the reactor pool.
      run / javaOptions ++= reactorCarrierOpens
    )
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .jvmConfigure(_.dependsOn(ioNioDemos))
    .nativeConfigure(_.dependsOn(ioKqueueDemos))

// The bench scenarios written with cats-effect and fs2, to compare against; see bench/compare.sc. The same code on
// Scala Native (kqueue, through bench/build.sc) and on the JVM (its selector poller, through bench/build_jvm.sc).
lazy val ceBench =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("bench/ce-bench"))
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("cebench.CeBench"),
      libraryDependencies ++= Seq(
        "org.typelevel" %% "cats-effect" % "3.7.1",
        "co.fs2" %% "fs2-io" % "3.14.0",
        mainargs
      )
    )
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })

// JVM tests that drive the demo applications as subprocesses (macOS only): the plain kqueue demos, the gears demos on
// Scala Native through them and on the JVM through NIO's, and the bench of each platform.
lazy val ioDemosTests =
  (project in file("io-tests"))
    .settings(
      publish / skip := true,
      libraryDependencies += munit % Test,
      testFrameworks += MUnitFramework,
      Test / fork := true,
      Test / javaOptions += {
        val binary = fileConverter.value.toPath((ioKqueueDemos / Compile / nativeLink).value)
        s"-Dkqueue.demo.binary=${binary.toAbsolutePath}"
      },
      Test / javaOptions += {
        val binary = fileConverter.value.toPath((ioBench.native / Compile / nativeLink).value)
        s"-Dgears.bench.binary=${binary.toAbsolutePath}"
      },
      // The same gears demos on the JVM, started as `java -cp <this> niodemos.NioDemos`.
      Test / javaOptions += {
        val converter = fileConverter.value
        val classpath = (ioNioDemos / Runtime / fullClasspath).value
          .map(entry => converter.toPath(entry.data).toAbsolutePath.toString)
          .mkString(java.io.File.pathSeparator)
        s"-Dgears.demo.jvm.classpath=$classpath"
      },
      // The bench on the JVM, started as `java -cp <this> gearsbench.Main`.
      Test / javaOptions += {
        val converter = fileConverter.value
        val classpath = (ioBench.jvm / Runtime / fullClasspath).value
          .map(entry => converter.toPath(entry.data).toAbsolutePath.toString)
          .mkString(java.io.File.pathSeparator)
        s"-Dgears.bench.jvm.classpath=$classpath"
      }
    )
