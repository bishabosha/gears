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

// Buffers for tests and demos written against `asyncio` alone, allocated from an arena each platform provides: a Zone
// on Scala Native, foreign memory on the JVM. Kept out of the reactor interface on purpose.
lazy val asyncioTestkit =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("asyncio-testkit"))
    .settings(publish / skip := true)

// The platform-independent reactor model: ops, completions, the Reactor trait, and the blocker pool; and the gears
// scheduler over a pool of reactors, which a platform with suspension support for it starts.
lazy val asyncio =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Pure)
    .in(file("asyncio"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework
    )
    // Its tests take tasks across threads.
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .dependsOn(root, asyncioTestkit % Test)

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
    .dependsOn(asyncio.native % "compile->compile;test->test")

// The JVM's reactor backend: NIO selectors over the JDK's selectable channels, and threads for what the JDK can only
// block on.
lazy val jvmAsyncioNio =
  (project in file("asyncio-jvm/nio"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework
    )
    .dependsOn(asyncio.jvm % "compile->compile;test->test")

// Gears on the JVM over reactors: `ReactorFutures.run` with gears' virtual threads beside reactor threads, or, opted
// into, virtual threads carried by the reactor pool (see asyncio-jvm/nio/src/main/scala/asyncio/nio/TODO.md).
lazy val jvmAsyncioGears =
  (project in file("asyncio-jvm/gears"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      Test / fork := true,
      // `Carriers.Reactors` gives virtual threads the pool as their scheduler, which the JDK keeps to itself.
      Test / javaOptions += "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
    .dependsOn(asyncio.jvm, jvmAsyncioNio % Test, asyncioTestkit.jvm % Test)

// Gears on Scala Native over reactors: starts the pool of `asyncio`'s scheduler with Native's suspension (see
// asyncio/kqueue/TODO.md, "A thread-pooled scheduler").
lazy val nativeAsyncioGears =
  (project in file("asyncio-native/gears"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(nativeAsyncioLoop)

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
