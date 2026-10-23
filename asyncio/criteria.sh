#!/usr/bin/env bash
# The goal's clauses as checks on the repository (see asyncio/README.md, "The goal, as criteria"): each prints its
# finding, and the script fails if any check fails. Run from the repository root: asyncio/criteria.sh
set -u
cd "$(dirname "$0")/.." || exit 2
failed=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; failed=1; }
# Code lines only: whole-line comments dropped, trailing comments cut.
code() { sed -e 's://.*$::' "$@" | grep -v '^\s*\*' | grep -v '^\s*/\*' ; }

# Orthogonality.
n=$(code asyncio/src/main/scala/asyncio/*.scala | grep -c 'asyncio\.gears\|asyncio\.unsafe\|ReactorPool\|ReactorScheduler\|ReactorWorker')
[ "$n" -eq 0 ] && pass "the interface names nothing of the runtime or the shared implementation ($n references)" \
               || fail "the interface names the runtime or the shared implementation ($n references)"
caps=$(awk '/^trait (Socket|File|Resolve|Pipe|Readiness|Blocking|UnixStream|UnixDatagram|Fifo)Backend/,/^}/' asyncio/src/main/scala/asyncio/Backend.scala | sed -e 's://.*$::')
n=$(printf '%s\n' "$caps" | grep -c 'Reactor\|Pool\b\|OnPool\|OnCaller\|[Tt]hread\|[Ww]orker')
[ "$n" -eq 0 ] && pass "no capability trait names a kind, a pool, a worker or a thread ($n lines)" \
               || fail "a capability trait names a kind, a pool, a worker or a thread ($n lines)"
kinds=$(awk '/^trait (Reactor|Caller)Backend/,/^}/' asyncio/src/main/scala/asyncio/Backend.scala | sed -e 's://.*$::')
n=$(printf '%s\n' "$kinds" | grep -c 'Socket\|File\|Pipe\|Resolve\|Readiness\|Blocking\|Fifo\|Datagram')
[ "$n" -eq 0 ] && pass "no kind names a capability ($n lines)" || fail "a kind names a capability ($n lines)"
n=$(code asyncio-jvm/gears/src/main/scala/asyncio/gears/*.scala asyncio-native/gears/src/main/scala/asyncio/gears/*.scala asyncio-gears/src/main/scala/asyncio/gears/*.scala | grep -c 'asyncio\.nio\|asyncio\.kqueue\|asyncio\.nwframework')
[ "$n" -eq 0 ] && pass "the runtime and the runners name no backend ($n references)" || fail "the runtime or a runner names a backend ($n references)"
n=$(grep -c 'perform\|submit\|`yield`' asyncio-unsafe/src/main/scala/asyncio/unsafe/Pooled.scala)
grep -q 'pool\.perform\|pool\.submit\|pool\.`yield`' asyncio-unsafe/src/main/scala/asyncio/unsafe/Pooled.scala \
  && pass "a program on a pool reaches the runtime through Pool alone (Pooled forwards perform, submit and yield to it)" \
  || fail "Pooled does not forward to Pool"

# Implementation apart from the interface.
deps=$(awk '/^lazy val asyncio =/,/dependsOn/' build.sbt | grep dependsOn)
[ "$deps" = '    .dependsOn(root, asyncioTestkit % Test)' ] && pass "module asyncio depends on the gears library alone: $deps" \
  || fail "module asyncio has other dependencies: $deps"
# A def with a right-hand side: an `=` that is not part of `=>`, `<=`, `>=` or `==`, on a line declaring a def.
bodies_names=$(code asyncio/src/main/scala/asyncio/*.scala | grep -E '^[[:space:]]*(final |override |private\[asyncio\] |inline )*def ' | sed -E 's/\([^()]*\)//g; s/\([^()]*\)//g' | grep -E '(^|[^=<>!])=([^=>]|$)' | sed -E 's/^[[:space:]]*(final |override |private\[asyncio\] |inline )*def //; s/[[:space:]]*[\[(:=].*//' | sort -u)
echo "      bodies in the interface (defs with a right-hand side, by name): $(printf '%s' "$bodies_names" | tr '\n' ' ')"
leak=$(printf '%s\n' "$bodies_names" | grep -vE '^(`yield`|apply|clear|get|ignore|isEmpty|nonEmpty|onCancel|onComplete|onFailure|perform|scoped|set|sleep|submit|trySubmit|unwrap|wrap|empty)$' || true)
[ -z "$leak" ] && pass "every body in the interface is a contract default, a data record or a utility over the contract" \
               || fail "bodies in the interface outside the allowed set: $(printf '%s' "$leak" | tr '\n' ' ')"

# Idiomatic to gears.
n=$(grep -c 'var outcome\|var failure\|var waited\|var echoed' asyncio/src/test/scala/asyncio/gears/FuturesContract.scala)
[ "$n" -eq 0 ] && pass "the contract takes every result from the value running returns ($n result vars)" || fail "the contract smuggles results out through vars ($n)"
grep -q 'def run\[T\]' asyncio-jvm/gears/src/main/scala/asyncio/gears/ReactorFutures.scala && grep -q 'def run\[T\]' asyncio-native/gears/src/main/scala/asyncio/gears/ReactorFutures.scala \
  && pass "ReactorFutures.run returns the program's value on both platforms" || fail "ReactorFutures.run does not return the program's value on both platforms"
grep -q 'trait Io extends Backend with AsyncOperations' asyncio/src/main/scala/asyncio/Io.scala && pass "Io is the program's AsyncOperations" || fail "Io is not an AsyncOperations"
grep -q 'trait Submission extends Cancellable with Async.Source\[Try\[Unit\]\]' asyncio/src/main/scala/asyncio/Io.scala \
  && pass "a Submission is a gears Cancellable and an Async.Source" || fail "Submission is not a Cancellable and a Source"
for t in "gears' sleep and withTimeout run on the backend's timers" "a submission is cancelled when the group it was made in ends" "a submission is a source"; do
  grep -q "$t" asyncio/src/test/scala/asyncio/gears/FuturesContract.scala && pass "contract test present: $t" || fail "contract test missing: $t"
done

# The lowest level stays the lowest.
n=$(code asyncio/src/main/scala/asyncio/Ops.scala | grep -c '=> ')
[ "$n" -eq 2 ] && pass "the only closures in Ops are the two blocking ops' bodies ($n)" || fail "closures in Ops: $n, expected 2"
grep -q 'the op is no longer in use anywhere' asyncio/src/main/scala/asyncio/Io.scala && pass "Io.perform states the guarantee a wrapper relies on" || fail "Io.perform does not state the guarantee"
n=$(code higherlevel/src/main/scala/higherlevel/*.scala | grep -c 'asyncio\.gears\|asyncio\.unsafe\|asyncio\.nio\|asyncio\.kqueue\|asyncio\.nwframework\|bind(\|\.set(\|wrap(')
[ "$n" -eq 0 ] && pass "the higher level is written over the interface's public contract alone ($n references)" || fail "the higher level reaches past the interface ($n references)"
deps=$(awk '/^lazy val higherLevel =/,/dependsOn/' build.sbt | grep dependsOn)
[ "$deps" = '    .dependsOn(asyncio % "compile->compile;test->test")' ] && pass "module higherlevel depends on the interface alone: $deps" || fail "module higherlevel has other dependencies: $deps"
n=$(grep -l 'HigherLevel\.' gears-demo/shared/src/main/scala/gearsexample/*.scala | wc -l | tr -d ' ')
[ "$n" -ge 2 ] && pass "programs use the higher level ($n demos)" || fail "no program uses the higher level ($n demos)"
n=$(grep -l 'with HigherLevelChecks' -r --include='*.scala' asyncio-jvm asyncio-native | wc -l | tr -d ' ')
[ "$n" -ge 3 ] && pass "the higher level's checks run in every backend's contract suite ($n suite files)" || fail "the higher level's checks do not run on every backend ($n suite files)"

[ "$failed" -eq 0 ] && echo "all checks passed" || echo "some checks failed"
exit $failed
