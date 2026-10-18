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
    .jsSettings(
      Seq(
        // Emit ES modules with the Wasm backend
        scalaJSLinkerConfig := {
          scalaJSLinkerConfig.value
            .withESFeatures(_.withESVersion(ESVersion.ES2022).withUseWebAssembly(true))
            .withWasmFeatures(_.withUseJSPI(true)) // enable js.async/js.await
            .withModuleKind(ModuleKind.ESModule) // required by the Wasm backend
        },
        // Node.js 26+ supports Wasm and JSPI, including the nested async stack fix.
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
    )

// The platform-independent reactor model: ops, completions, the Reactor trait, and the blocker pool.
lazy val asyncio =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Pure)
    .in(file("asyncio"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework
    )

// ported from https://github.com/bishabosha/scala-native-async-io/commit/6e8320d36c71f75c9daf85065f5c89729f0825c0
lazy val nativeAsyncioLoop =
  (project in file("asyncio-native/loop"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      // Its tests run reactors on several threads.
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(asyncio.native)

// Gears on Scala Native over reactors: a scheduler that is a pool of worker threads, each driving a reactor of its
// own, and `perform` to suspend a future on an op (see asyncio/kqueue/TODO.md, "A thread-pooled scheduler").
lazy val nativeAsyncioGears =
  (project in file("asyncio-native/gears"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(nativeAsyncioLoop, root.native)

// ported from https://github.com/bishabosha/scala-native-async-io/commit/6e8320d36c71f75c9daf85065f5c89729f0825c0
lazy val kqueueDemo =
  (project in file("kqueue-demo"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("example.Main"),
      libraryDependencies += "com.lihaoyi" %% "mainargs" % "0.7.8",
      // The gears mirrors in src/main/scala/gears-example run futures on gears' fork-join pool.
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(nativeAsyncioGears)

// The kqueueDemo bench scenarios written with cats-effect and fs2, to compare against; see kqueue-demo/bench/compare.py
lazy val ceBench =
  (project in file("kqueue-demo/ce-bench"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("cebench.CeBench"),
      libraryDependencies ++= Seq(
        "org.typelevel" %% "cats-effect" % "3.7.1",
        "co.fs2" %% "fs2-io" % "3.14.0",
        "com.lihaoyi" %% "mainargs" % "0.7.8"
      ),
      nativeConfig ~= { _.withMultithreading(true) }
    )

// JVM tests that drive the linked kqueueDemo executable as a subprocess (macOS only)
lazy val kqueueDemoTests =
  (project in file("kqueue-demo/tests"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      Test / fork := true,
      Test / javaOptions += {
        val binary = fileConverter.value.toPath((kqueueDemo / Compile / nativeLink).value)
        s"-Dkqueue.demo.binary=${binary.toAbsolutePath}"
      }
    )
