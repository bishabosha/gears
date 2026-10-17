#!/usr/bin/env python3
"""Counts the system calls each bench scenario makes per operation, using lldb breakpoints, so it needs neither root
nor SIP disabled as dtruss does.

    kqueue-demo/bench/syscalls.py [--save FILE] [--scenarios a,b] <executable>[@raw]

Each scenario runs twice, at about 100 and 200 operations (more where a scenario works in batches), and the difference
divided by the extra operations is the count per operation, without connection setup and teardown. Calls are counted
on the client thread, the main thread, which runs the reactor under test; the bench's echo server and any workers
are counted apart, as `other`. `kevent` is split by what the call does: registration changes only, a poll that returns
at once, or a wait. Under the debugger the server answers slowly, so the split between polls and waits leans to
waits after spinning, and is only indicative; the other counts do not depend on timing.
"""
import argparse
import collections
import json
import os
import subprocess
import sys

SYMBOLS = ['kevent', 'read', 'write', '__recvfrom', '__recvmsg', '__sendto', '__accept', '__connect', '__fcntl',
           '__ioctl', 'close', 'sigprocmask', '__sigaltstack', '__psynch_cvsignal']

# Operations at scale 1, and how many to count with (the second run counts twice as many).
SCENARIOS = {
    'pingpong': (50_000, 100), 'pingpong-fresh-ops': (50_000, 100), 'pingpong-deadline': (50_000, 100),
    'concurrent': (100_000, 640), 'connections': (20_000, 100), 'pipelined': (200_000, 200), 'blocking': (100_000, 100),
    'timers': (200_000, 2000), 'fairness': (200_000, 200), 'mixed': (50_000, 100),
}
GEARS_ONLY = {'pingpong-fresh-ops', 'fairness', 'mixed'}


def count(command):
    """Runs `command` under lldb and returns its syscall counts: {'client': {name: n}, 'other': {...}}."""
    sys.path.append(subprocess.run(['lldb', '-P'], capture_output=True, text=True, check=True).stdout.strip())
    import lldb

    debugger = lldb.SBDebugger.Create()
    debugger.SetAsync(False)
    target = debugger.CreateTarget(command[0])
    names = {target.BreakpointCreateByName(s, 'libsystem_kernel.dylib').GetID(): s for s in SYMBOLS}
    info = lldb.SBLaunchInfo(command[1:])
    info.AddOpenFileAction(1, os.devnull, False, True)
    error = lldb.SBError()
    process = target.Launch(info, error)
    if not error.Success():
        sys.exit(f'failed to launch {command}: {error}')
    counts = {'client': collections.Counter(), 'other': collections.Counter()}
    client = process.GetThreadAtIndex(0).GetThreadID()
    while process.GetState() == lldb.eStateStopped:
        for thread in process:
            if thread.GetStopReason() != lldb.eStopReasonBreakpoint:
                continue
            name = names[thread.GetStopReasonDataAtIndex(0)]
            if name == 'kevent':
                frame = thread.GetFrameAtIndex(0)
                nchanges, nevents, timeout = (frame.FindRegister(r).GetValueAsUnsigned() for r in ('x2', 'x4', 'x5'))
                if nevents == 0:
                    name = 'kevent (changes)'
                else:
                    zero = timeout != 0 and process.ReadMemory(timeout, 16, error) == bytes(16)
                    name = ('kevent (poll now)' if zero else 'kevent (wait)') + (' + changes' if nchanges else '')
            counts['client' if thread.GetThreadID() == client else 'other'][name] += 1
        process.Continue()
    if process.GetExitStatus() != 0:
        sys.exit(f'{command} exited with {process.GetExitStatus()}')
    return {kind: dict(c) for kind, c in counts.items()}


def per_operation(path, raw, name):
    full, ops = SCENARIOS[name]
    runs = []
    for n in (ops, 2 * ops):
        command = [path] + ([] if raw else ['gears']) + ['bench', '--scale', repr(n / full), '--scenarios', name]
        runs.append(json.loads(subprocess.run([sys.executable, __file__, '--count', *command],
                                              capture_output=True, text=True, check=True).stdout))
    result = {}
    for kind in ('client', 'other'):
        diffs = {k: round((runs[1][kind].get(k, 0) - runs[0][kind].get(k, 0)) / ops, 2)
                 for k in sorted(set(runs[0][kind]) | set(runs[1][kind]))}
        result[kind] = {k: v for k, v in diffs.items() if v != 0}
    return result


def main():
    if sys.argv[1:2] == ['--count']:  # one run, in its own process so that lldb starts afresh
        print(json.dumps(count(sys.argv[2:])))
        return
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('executable', help='a kqueuedemo executable; with @raw, its hand-written benchmark')
    parser.add_argument('--scenarios', help='comma-separated scenarios, or all')
    parser.add_argument('--save', metavar='FILE', help='write the counts, with the build provenance, as JSON')
    args = parser.parse_args()
    raw = args.executable.endswith('@raw')
    path = args.executable.removesuffix('@raw')
    chosen = args.scenarios.split(',') if args.scenarios else list(SCENARIOS)
    results = {}
    for name in chosen:
        if raw and name in GEARS_ONLY:
            continue
        results[name] = per_operation(path, raw, name)
        print(f'{name:20} ' + ', '.join(f'{k} {v:g}' for k, v in results[name]['client'].items()), flush=True)
    if args.save:
        provenance = path + '.json'
        build = json.load(open(provenance)) if os.path.exists(provenance) else None
        with open(args.save, 'w') as f:
            json.dump({'executable': path, 'benchmark': 'raw' if raw else 'gears', 'build': build,
                       'unit': 'system calls per operation', 'scenarios': results}, f, indent=2)
            f.write('\n')
        print(f'saved {args.save}')


main()
