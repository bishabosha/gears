#!/bin/bash
# Packages the JVM gears demo from the working tree for bench/compare.sc: its jars are copied into
# <output>/lib, so later builds do not change what is measured, and three executables beside them run the bench
# scenarios, each taking `gears bench <args>` as compare.sc passes them:
#
#   <output>/nio-caller     NioOnCaller: no reactor, each op performed on the future's own virtual thread (the default)
#   <output>/nio-reactors   NioBackend: futures carried by the reactor pool
#   <output>/jdk-vthreads   no reactor: blocking JDK channels on virtual threads in structured task scopes
#   <output>/jdk-selector   no reactor: as jdk-vthreads, with non-blocking channels and a selector thread a side, the
#                           least a design on Selector does (gearsexample.SelectorIo)
#   <output>/jdk-selector-here  no reactor: non-blocking channels, each waiting virtual thread selecting on the
#                           channel's own selector, with no selector thread (gearsexample.SelectorHereIo)
#   <output>/cats-effect    the same scenarios with cats-effect and fs2 (bench/ce-bench), a selector per worker;
#                           it runs as `<this> bench <args>`, so compare it as `<label>=<output>/cats-effect@raw`
#
#   bench/build-jvm.sh <output>
#   bench/compare.sc nio-caller=<output>/nio-caller nio-reactors=<output>/nio-reactors jdk=<output>/jdk-vthreads \
#     ce=<output>/cats-effect@raw
#
# Every run first runs each scenario at a fifth of its count, unreported (`--warmup`), so the JIT has compiled what is
# measured. They run the `java` on the PATH, or $JAVA. Where they came from is written to <executable>.json beside each,
# which compare.sc --save includes.
set -euo pipefail
output=${1:?usage: build-jvm.sh <output>}
case "$output" in /*) ;; *) output="$PWD/$output" ;; esac
repo=$(git rev-parse --show-toplevel)
cd "$repo"

package() { # package <project> <directory>: copies the project's runtime classpath into <output>/<directory>
  sbt --client "$1/Compile/packageBin" >/dev/null
  classpath=$(sbt --client "export $1/Runtime/fullClasspath" | grep -o 'List(.*)' | sed 's/^List(//; s/)$//')
  rm -rf "$output/$2"
  mkdir -p "$output/$2"
  IFS=',' read -ra entries <<<"$classpath"
  for entry in "${entries[@]}"; do
    path=$(echo "$entry" | sed 's/^ *//; s/>.*$//; s#\${OUT}#'"$repo"'/target/out#; s#\${CSR_CACHE}#'"$HOME"'/Library/Caches/Coursier/v1#')
    cp "$path" "$output/$2/"
  done
}
package ioDemosJVM lib
package ceBenchJVM lib-ce

java=${JAVA:-$(command -v java)}
commit=$(git rev-parse HEAD)
dirty=$(git status --porcelain | grep -q . && echo true || echo false)
jdk=$("$java" -version 2>&1 | head -1)

write() { # write <name> <java options> <command>
  cat >"$output/$1" <<EOF
#!/bin/bash
# compare.sc runs \`<this> gears bench <args>\`; see bench/build-jvm.sh.
shift 2
exec "$java" $2 -cp "$output/lib/*" gearsexample.Main $3 --warmup "\$@"
EOF
  chmod +x "$output/$1"
  "$repo/bench/provenance.sc" "$output/$1.json" commit="$commit" dirty="$dirty" jdk="$jdk" command="$3" \
    java_options="$2"
}
opens="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/jdk.internal.vm=ALL-UNNAMED"
write nio-caller "$opens -Dgears.io.nio.backend=caller" bench
write nio-reactors "$opens -Dgears.io.nio.backend=reactors" bench
write jdk-vthreads "--enable-preview" jdk-bench
write jdk-selector "--enable-preview" "jdk-bench --selector"
write jdk-selector-here "--enable-preview" "jdk-bench --selector-here"
# compare.sc runs `<this> bench <args>` for a build marked @raw.
cat >"$output/cats-effect" <<EOF
#!/bin/bash
# compare.sc runs \`<this> bench <args>\`; see bench/build-jvm.sh.
command=\$1
shift
exec "$java" -cp "$output/lib-ce/*" cebench.CeBench "\$command" --warmup "\$@"
EOF
chmod +x "$output/cats-effect"
"$repo/bench/provenance.sc" "$output/cats-effect.json" commit="$commit" dirty="$dirty" jdk="$jdk" command=bench \
  java_options=
echo "built $output: nio-caller nio-reactors jdk-vthreads jdk-selector jdk-selector-here cats-effect"
