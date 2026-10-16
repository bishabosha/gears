#!/usr/bin/env python3
"""Compares `kqueuedemo gears bench` across builds of the demo executable.

Build the executables to compare with kqueue-demo/bench/build.sh, which links a commit in release-fast mode against the
Scala Native nightly (0.5.12's GC can crash multithreaded release builds), then compare them:

    kqueue-demo/bench/build.sh HEAD~ /tmp/kqueuedemo.before
    kqueue-demo/bench/build.sh HEAD /tmp/kqueuedemo.after
    kqueue-demo/bench/compare.py before=/tmp/kqueuedemo.before after=/tmp/kqueuedemo.after raw=/tmp/kqueuedemo.after@raw

A build runs the gears benchmark, `gears bench`; suffix its path with `@raw` to run the same scenarios written against
`KqueueLoop` directly (`bench`), as a reference: `raw=<path>@raw`.

Each build runs `--runs` times, alternating between builds so drift in machine load affects them alike. The table
shows each scenario's median microseconds per operation, and each later build's change against the first.
Arguments after `--` go to `bench`, for example `-- --scenarios pingpong,pipelined --scale 0.5`.

--save writes the results as JSON to kqueue-demo/bench/results/<date>-<labels>.json (ignored by git), and --save-to FILE
to FILE: every run's numbers and the medians, each build's provenance from build.sh (the <executable>.json beside it),
the arguments, the date, and the load average.
"""

import argparse
import datetime
import json
import os
import pathlib
import statistics
import subprocess
import sys


def run_once(path, bench_args, retries, crashes):
    command = [path[: -len("@raw")], "bench"] if path.endswith("@raw") else [path, "gears", "bench"]
    for attempt in range(retries + 1):
        result = subprocess.run([*command, *bench_args], capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            break
        crashes.append(f"{' '.join(command)} exited with {result.returncode}: {result.stderr.strip()[-300:]}")
        if attempt == retries:
            sys.exit(f"{' '.join(command)} failed {retries + 1} times; last output:\n{result.stdout}{result.stderr}")
        print(f"  failed with exit code {result.returncode}; retrying", file=sys.stderr)
    output = result.stdout
    results = {}
    for line in output.splitlines():
        fields = line.split()
        # bench: <scenario> <ops> <seconds> <ops/s> <us/op>
        if len(fields) == 6 and fields[0] == "bench:" and fields[1] != "scenario":
            results[fields[1]] = float(fields[5])
    if not results:
        sys.exit(f"{path} printed no bench results:\n{output}")
    return results


def main():
    argv = sys.argv[1:]
    bench_args = []
    if "--" in argv:
        split = argv.index("--")
        argv, bench_args = argv[:split], argv[split + 1 :]
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "builds", nargs="+", metavar="label=path[@raw]", help="a demo executable and a label for it"
    )
    parser.add_argument("--runs", type=int, default=3, help="runs per build (default 3)")
    parser.add_argument(
        "--retries", type=int, default=0, help="times to rerun a run that fails before giving up (default 0)"
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="write the results as JSON to kqueue-demo/bench/results/<date>-<labels>.json",
    )
    parser.add_argument("--save-to", metavar="FILE", help="write the results as JSON to FILE")
    args = parser.parse_args(argv)

    builds = []
    for build in args.builds:
        label, sep, path = build.partition("=")
        if not sep:
            label, path = build, build
        builds.append((label, path))

    samples = {label: {} for label, _ in builds}
    crashes = []
    started = datetime.datetime.now().astimezone()
    load_before = os.getloadavg()
    for run in range(args.runs):
        for label, path in builds:
            print(f"run {run + 1}/{args.runs}: {label}", file=sys.stderr)
            for scenario, us in run_once(path, bench_args, args.retries, crashes).items():
                samples[label].setdefault(scenario, []).append(us)

    scenarios = list(dict.fromkeys(s for label, _ in builds for s in samples[label]))
    base = builds[0][0]
    header = f"{'us/op (median)':<18}" + "".join(f"{label:>22}" for label, _ in builds)
    print(header)
    print("-" * len(header))
    for scenario in scenarios:
        base_median = statistics.median(samples[base][scenario]) if scenario in samples[base] else None
        row = f"{scenario:<18}"
        for label, _ in builds:
            if scenario not in samples[label]:  # e.g. the raw benchmark has no `mixed`
                row += f"{'-':>22}"
                continue
            median = statistics.median(samples[label][scenario])
            if label == base or base_median is None:
                row += f"{median:>22.2f}"
            else:
                change = (median - base_median) / base_median * 100
                row += f"{median:>13.2f} ({change:+6.1f}%)"
        print(row)

    if crashes:
        print(f"\n{len(crashes)} run(s) failed and were retried:")
        for crash in crashes:
            print(f"  {crash}")

    if args.save or args.save_to:
        save(args.save_to, builds, samples, crashes, bench_args, args.runs, started, load_before)


def provenance(path):
    """The build.sh record beside an executable, if there is one."""
    record = pathlib.Path(path.removesuffix("@raw") + ".json")
    return json.loads(record.read_text()) if record.exists() else None


def save(file, builds, samples, crashes, bench_args, runs, started, load_before):
    if not file:
        results = pathlib.Path(__file__).resolve().parent / "results"
        labels = "-".join(label for label, _ in builds)
        file = results / f"{started.strftime('%Y%m%d-%H%M%S')}-{labels}.json"
    file = pathlib.Path(file)
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(
        json.dumps(
            {
                "started": started.isoformat(timespec="seconds"),
                "finished": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
                "load_average": {"before": load_before, "after": os.getloadavg()},
                "runs": runs,
                "bench_args": bench_args,
                "unit": "microseconds per operation",
                "builds": [
                    {
                        "label": label,
                        "executable": path,
                        "benchmark": "raw" if path.endswith("@raw") else "gears",
                        "build": provenance(path),
                        "median": {scenario: statistics.median(values) for scenario, values in samples[label].items()},
                        "samples": samples[label],
                    }
                    for label, path in builds
                ],
                "failed_runs": crashes,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"\nsaved {file}")

if __name__ == "__main__":
    main()
