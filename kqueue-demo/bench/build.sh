#!/bin/bash
# Builds a kqueue demo executable to benchmark: a commit linked in release-fast mode against the Scala Native nightly
# (see project/plugins.sbt), in a temporary worktree with its own sbt, so the working tree and its sbt server are left
# alone.
#
#   kqueue-demo/bench/build.sh <commit> <output>
#
# Then compare executables with kqueue-demo/bench/compare.py. <commit> may be WORKTREE, for HEAD with the working tree's
# changes, untracked files included (ignored ones are not). Where the executable came from is written beside it, to
# <output>.json, which compare.py --save includes in its results.
set -euo pipefail
commit=${1:?usage: build.sh <commit> <output>}
output=${2:?usage: build.sh <commit> <output>}
case "$output" in /*) ;; *) output="$PWD/$output" ;; esac

repo=$(git rev-parse --show-toplevel)
worktree_changes=false
changes_hash=""
if [ "$commit" = WORKTREE ]; then
  worktree_changes=true
  commit=HEAD
  # Identifies the working-tree changes built: the diff and the untracked files, with their contents.
  changes_hash=$(
    cd "$repo" && {
      git diff HEAD --binary
      git ls-files --others --exclude-standard -z | xargs -0 shasum 2>/dev/null
    } | shasum | cut -c1-12
  )
fi
commit_sha=$(git -C "$repo" rev-parse "$commit^{commit}")
commit_subject=$(git -C "$repo" log -1 --format=%s "$commit_sha")
worktree=$(mktemp -d "${TMPDIR:-/tmp}/kqueue-bench-build.XXXXXX")
cleanup() {
  (cd "$worktree" && sbt --client shutdown >/dev/null 2>&1) || true
  git -C "$repo" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$repo" worktree add --quiet --detach "$worktree" "$commit"
if $worktree_changes; then
  # Bring over the working tree: changes to tracked files, then untracked files.
  git -C "$repo" diff HEAD --binary | git -C "$worktree" apply --allow-empty
  git -C "$repo" ls-files --others --exclude-standard -z | (cd "$repo" && xargs -0 -I{} rsync -R "{}" "$worktree/")
fi
cd "$worktree"
# Commits from before the nightly switch existed get it from the working tree.
cp "$repo/project/plugins.sbt" project/plugins.sbt
grep -q SCALA_NATIVE_NIGHTLY build.sbt || printf '\nThisBuild / resolvers += Resolver.sonatypeCentralSnapshots\n' >>build.sbt
echo "building $(git log --oneline -1) in release-fast against the Scala Native nightly..." >&2
log="$worktree/build.log"
if ! SCALA_NATIVE_NIGHTLY=1 sbt -batch \
  'set kqueueDemo/nativeConfig ~= (_.withMode(scala.scalanative.build.Mode.releaseFast)); show kqueueDemo/libraryDependencies; kqueueDemo/nativeLink' \
  >"$log" 2>&1; then
  tail -40 "$log" >&2
  exit 1
fi
scala_native=$(grep -o "nativelib:0\.5\.13-[^ ]*" "$log" | head -1 | cut -d: -f2)
[ -n "$scala_native" ] || { echo "the build did not use the Scala Native nightly" >&2; exit 1; }
binary=target/out/native0.5/scala-3.9.0/kqueuedemo/kqueuedemo
"$binary" target-info | grep -q "release: true" || { echo "the executable is not a release build" >&2; exit 1; }
mkdir -p "$(dirname "$output")"
cp "$binary" "$output"
COMMIT="$commit_sha" SUBJECT="$commit_subject" CHANGES="$worktree_changes" CHANGES_HASH="$changes_hash" \
  SCALA_NATIVE="$scala_native" python3 - "$output.json" <<'PY'
import datetime, json, os, sys
json.dump({
    "commit": os.environ["COMMIT"],
    "subject": os.environ["SUBJECT"],
    "worktree_changes": os.environ["CHANGES"] == "true",
    "worktree_changes_hash": os.environ["CHANGES_HASH"] or None,
    "mode": "release-fast",
    "scala_native": os.environ["SCALA_NATIVE"],
    "built": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
}, open(sys.argv[1], "w"), indent=2)
PY
echo "$output" >&2
