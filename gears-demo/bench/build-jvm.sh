#!/bin/bash
# Packages the JVM gears demo from the working tree for kqueue-demo/bench/compare.py: its jars are copied into
# <output>/lib, so later builds do not change what is measured, and three executables beside them run the bench
# scenarios, each taking `gears bench <args>` as compare.py passes them:
#
#   <output>/nio-jdk        NIO, futures on gears' own virtual threads beside reactor threads (Carriers.Jdk)
#   <output>/nio-reactors   NIO, futures carried by the reactor pool (Carriers.Reactors)
#   <output>/vthread-backend  gears over VThreadBackend: no reactor; an op is tried on the future's virtual thread,
#                           yields and tries again, then waits on its handle's own selector there; the scenarios
#                           needing a reactor or blocking ops are skipped
#   <output>/jdk-vthreads   no reactor: blocking JDK channels on virtual threads in structured task scopes
#   <output>/jdk-selector   no reactor: as jdk-vthreads, with non-blocking channels and a selector thread a side, the
#                           least a design on Selector does (gearsexample.SelectorIo)
#   <output>/jdk-selector-here  no reactor: non-blocking channels, each waiting virtual thread selecting on the
#                           channel's own selector, with no selector thread (gearsexample.SelectorHereIo)
#   <output>/cats-effect    the same scenarios with cats-effect and fs2 (kqueue-demo/ce-bench), a selector per worker;
#                           it runs as `<this> bench <args>`, so compare it as `<label>=<output>/cats-effect@raw`
#
#   gears-demo/bench/build-jvm.sh <output>
#   kqueue-demo/bench/compare.py nio-jdk=<output>/nio-jdk nio-reactors=<output>/nio-reactors jdk=<output>/jdk-vthreads \
#     ce=<output>/cats-effect@raw
#
# Every run first runs each scenario at a fifth of its count, unreported (`--warmup`), so the JIT has compiled what is
# measured. They run the `java` on the PATH, or $JAVA. Where they came from is written to <executable>.json beside each,
# which compare.py --save includes.
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
package gearsDemoJVM lib
package ceBenchJVM lib-ce

java=${JAVA:-$(command -v java)}
commit=$(git rev-parse HEAD)
dirty=$(git status --porcelain | grep -q . && echo true || echo false)
jdk=$("$java" -version 2>&1 | head -1)

write() { # write <name> <java options> <command>
  cat >"$output/$1" <<EOF
#!/bin/bash
# compare.py runs \`<this> gears bench <args>\`; see gears-demo/bench/build-jvm.sh.
shift 2
exec "$java" $2 -cp "$output/lib/*" gearsexample.Main $3 --warmup "\$@"
EOF
  chmod +x "$output/$1"
  python3 -c 'import json, sys; print(json.dumps(dict(zip(sys.argv[1::2], sys.argv[2::2]))))' \
    commit "$commit" dirty "$dirty" jdk "$jdk" command "$3" java_options "$2" >"$output/$1.json"
}
opens=--add-opens=java.base/java.lang=ALL-UNNAMED
write nio-jdk "$opens -Dasyncio.gears.carriers=jdk" bench
write nio-reactors "$opens -Dasyncio.gears.carriers=reactors" bench
write vthread-backend "-Dasyncio.backend=vthreads" bench
write jdk-vthreads "--enable-preview" jdk-bench
write jdk-selector "--enable-preview" "jdk-bench --selector"
write jdk-selector-here "--enable-preview" "jdk-bench --selector-here"
# compare.py runs `<this> bench <args>` for a build marked @raw.
cat >"$output/cats-effect" <<EOF
#!/bin/bash
# compare.py runs \`<this> bench <args>\`; see gears-demo/bench/build-jvm.sh.
command=\$1
shift
exec "$java" -cp "$output/lib-ce/*" cebench.CeBench "\$command" --warmup "\$@"
EOF
chmod +x "$output/cats-effect"
python3 -c 'import json, sys; print(json.dumps(dict(zip(sys.argv[1::2], sys.argv[2::2]))))' \
  commit "$commit" dirty "$dirty" jdk "$jdk" command bench java_options "" >"$output/cats-effect.json"
echo "built $output: nio-jdk nio-reactors vthread-backend jdk-vthreads jdk-selector jdk-selector-here cats-effect"
