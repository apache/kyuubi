#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


def dump_threads(group, directory):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f"threads-{time.time_ns()}.txt").open("wb") as dump:
        # Include the Maven JVM and forked test/engine JVMs in its process group.
        processes = subprocess.check_output(["ps", "-eo", "pid=,pgid=,comm="], text=True)
        deadline = time.monotonic() + 30
        for line in processes.splitlines():
            pid, pgid, command = line.split(None, 2)
            if int(pgid) != group or Path(command).name != "java":
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                dump.write(b"Thread dump time budget exhausted.\n")
                break
            dump.write(f"\n=== JVM {pid} ===\n".encode())
            dump.flush()
            try:
                subprocess.run(
                    ["jcmd", pid, "Thread.print", "-l"],
                    stdout=dump,
                    stderr=subprocess.STDOUT,
                    timeout=min(5, remaining),
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as error:
                dump.write(f"{error}\n".encode())
        if dump.tell() == 0:
            dump.write(b"No Java processes found in the test process group.\n")


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    # Keep the group leader unreaped until the final signal. A child may still
    # be stuck even if Maven exits in response to SIGTERM.
    time.sleep(10)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def run(command, directory, idle_seconds, dump_gap_seconds):
    with tempfile.NamedTemporaryFile() as console, open(console.name, "rb") as reader:
        process = subprocess.Popen(
            command, stdout=console, stderr=subprocess.STDOUT, start_new_session=True
        )
        last_output = time.monotonic()
        first_dump = None
        try:
            while True:
                output = reader.read()
                if output:
                    sys.stdout.buffer.write(output)
                    sys.stdout.buffer.flush()
                    last_output = time.monotonic()
                    first_dump = None

                status = process.poll()
                if status is not None:
                    sys.stdout.buffer.write(reader.read())
                    sys.stdout.buffer.flush()
                    return status if status >= 0 else 128 - status

                now = time.monotonic()
                if first_dump is None and now - last_output >= idle_seconds:
                    print(f"No build output for {idle_seconds}s. Capturing JVM stacks.", flush=True)
                    dump_threads(process.pid, directory)
                    first_dump = time.monotonic()
                elif first_dump is not None and now - first_dump >= dump_gap_seconds:
                    dump_threads(process.pid, directory)
                    if reader.peek(1) or process.poll() is not None:
                        continue
                    print("Build is still stalled. Stopping to preserve diagnostics.", flush=True)
                    return 124
                time.sleep(1)
        finally:
            if process.poll() is None:
                stop_group(process)


def terminate(signum, _frame):
    raise SystemExit(128 + signum)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Capture JVM stacks when a CI build stalls.")
    parser.add_argument("--dump-dir", required=True, type=Path)
    parser.add_argument("--idle-seconds", type=int, default=30 * 60)
    parser.add_argument("--dump-gap-seconds", type=int, default=15)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.idle_seconds < 1 or args.dump_gap_seconds < 1:
        parser.error("a command and positive time limits are required")
    signal.signal(signal.SIGTERM, terminate)
    sys.exit(run(command, args.dump_dir, args.idle_seconds, args.dump_gap_seconds))
