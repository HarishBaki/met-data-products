#!/usr/bin/env python3
"""Track the New York URMA outer-store campaign and log every 10 finished jobs."""

from __future__ import annotations

import argparse
import subprocess
import time
from datetime import datetime
from pathlib import Path

TERMINAL_STATES = {
    "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL",
    "OUT_OF_MEMORY", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "REVOKED",
}


def snapshot(user: str, starttime: str, first_job_id: int) -> tuple[int, int, int, int]:
    result = subprocess.run(
        [
            "sacct", "-X", "-n", "-P", "--user", user, "--starttime", starttime,
            "--format=JobIDRaw,JobName,State,ExitCode",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    submitted = completed = failed = 0
    for line in result.stdout.splitlines():
        fields = line.split("|")
        if len(fields) < 4:
            continue
        job_id, name, state, _exit_code = fields[:4]
        if name.strip() != "URMA" or not job_id.strip().isdigit():
            continue
        if int(job_id) < first_job_id:
            continue
        submitted += 1
        state = state.strip().split()[0]
        if state == "COMPLETED":
            completed += 1
        elif state in TERMINAL_STATES:
            failed += 1
    return submitted, completed + failed, completed, failed


def log_line(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now().astimezone().isoformat(timespec='seconds')} {message}\n"
    with path.open("a", encoding="utf-8") as stream:
        stream.write(line)
        stream.flush()
    print(line, end="", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user", default="hb533188")
    parser.add_argument("--starttime", default="2026-10-08T07:26:00")
    parser.add_argument("--first-job-id", type=int, default=129724)
    parser.add_argument("--expected-jobs", type=int, default=116)
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument(
        "--log", type=Path,
        default=Path(__file__).parent / "slurmout" / "ny_urma_outer_progress.log",
    )
    parser.add_argument("--once", action="store_true", help="print one snapshot and exit")
    args = parser.parse_args()

    submitted, finished, completed, failed = snapshot(
        args.user, args.starttime, args.first_job_id
    )
    if args.once:
        print(
            f"submitted={submitted} finished={finished} completed={completed} "
            f"failed={failed} expected={args.expected_jobs}"
        )
        return

    log_line(
        args.log,
        f"monitor_started first_job_id={args.first_job_id} expected_jobs={args.expected_jobs} "
        f"poll_seconds={args.poll_seconds}",
    )
    last_checkpoint = (finished // 10) * 10
    while finished < args.expected_jobs:
        time.sleep(args.poll_seconds)
        try:
            submitted, finished, completed, failed = snapshot(
                args.user, args.starttime, args.first_job_id
            )
        except subprocess.CalledProcessError as exc:
            log_line(args.log, f"accounting_query_failed exit={exc.returncode}")
            continue

        checkpoint = (finished // 10) * 10
        while checkpoint > last_checkpoint:
            last_checkpoint += 10
            log_line(
                args.log,
                f"campaign_progress finished={last_checkpoint}/{args.expected_jobs} "
                f"submitted={submitted} completed={completed} failed={failed} "
                f"not_yet_finished={args.expected_jobs - last_checkpoint}",
            )

    log_line(
        args.log,
        f"campaign_processing_jobs_terminal finished={finished}/{args.expected_jobs} "
        f"submitted={submitted} completed={completed} failed={failed}",
    )


if __name__ == "__main__":
    main()
