#!/usr/bin/env python3
import argparse
import asyncio
from pathlib import Path
import json
import os
import re
import sys
from urllib.parse import parse_qs, urlparse

import requests
import websockets


ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
JUPYTER_URL_RE = re.compile(r"https?://\S+")


def strip_ansi(text):
    return ANSI_RE.sub("", text).replace("\r", "")


def normalize_jupyter_url(url):
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Invalid Jupyter URL: {url}")
    return f"{parsed.scheme}://{parsed.netloc}"


def parse_jupyter_session_url(url):
    parsed = urlparse(url.strip())
    token = parse_qs(parsed.query).get("token", [None])[0]
    base_url = normalize_jupyter_url(url)
    return base_url, token


def discover_latest_slurmout(slurmout_dir):
    candidates = sorted(Path(slurmout_dir).glob("jupyter-*.out"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No Jupyter slurm output files found in {slurmout_dir}")
    return candidates[0]


def discover_session_from_slurmout(slurmout_file):
    text = Path(slurmout_file).read_text()
    match = JUPYTER_URL_RE.search(text)
    if not match:
        raise ValueError(f"Could not find a Jupyter URL in {slurmout_file}")
    return parse_jupyter_session_url(match.group(0))


async def run_terminal(url, token, command, cwd=None, timeout=30):
    http_base = url.rstrip("/")
    if http_base.startswith("https://"):
        ws_base = "wss://" + http_base[len("https://") :]
    elif http_base.startswith("http://"):
        ws_base = "ws://" + http_base[len("http://") :]
    else:
        raise ValueError("Jupyter URL must start with http:// or https://")

    response = requests.post(f"{http_base}/api/terminals", params={"token": token}, timeout=20)
    response.raise_for_status()
    terminal_name = response.json()["name"]

    output_chunks = []
    disconnected = False
    try:
        ws_url = f"{ws_base}/terminals/websocket/{terminal_name}?token={token}"
        async with websockets.connect(ws_url, open_timeout=20, max_size=2**22) as ws:
            setup = await asyncio.wait_for(ws.recv(), timeout=timeout)
            output_chunks.append(setup)

            if cwd:
                await ws.send(json.dumps(["stdin", f"cd {cwd}\n"]))
            await ws.send(json.dumps(["stdin", command + "\n"]))
            await ws.send(json.dumps(["stdin", "exit\n"]))

            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    break

                output_chunks.append(msg)
                try:
                    parsed = json.loads(msg)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, list) and parsed and parsed[0] == "disconnect":
                    disconnected = True
                    break
    finally:
        try:
            requests.delete(f"{http_base}/api/terminals/{terminal_name}", params={"token": token}, timeout=20)
        except Exception:
            pass

    text_parts = []
    for chunk in output_chunks:
        try:
            parsed = json.loads(chunk)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list) and len(parsed) >= 2 and parsed[0] == "stdout":
            text_parts.append(parsed[1])

    cleaned = strip_ansi("".join(text_parts))
    return cleaned, disconnected


def parse_args():
    parser = argparse.ArgumentParser(description="Execute a shell command in a remote Jupyter terminal.")
    parser.add_argument("--url", default=os.environ.get("JUPYTER_SERVER_URL"), help="Jupyter server base URL.")
    parser.add_argument("--token", default=os.environ.get("JUPYTER_SERVER_TOKEN"), help="Jupyter token.")
    parser.add_argument(
        "--slurmout-file",
        default=None,
        help="Specific Jupyter slurm output file to parse, for example slurmout/jupyter-63179.out.",
    )
    parser.add_argument(
        "--slurmout-dir",
        default="slurmout",
        help="Directory containing jupyter-*.out files. The newest file is used if --slurmout-file is not set.",
    )
    parser.add_argument("--cwd", default=None, help="Working directory on the remote host.")
    parser.add_argument("--timeout", type=int, default=30, help="Receive timeout in seconds.")
    parser.add_argument("command", nargs="+", help="Command to execute remotely.")
    args = parser.parse_args()

    if not args.url or not args.token:
        try:
            slurmout_file = args.slurmout_file or discover_latest_slurmout(args.slurmout_dir)
            discovered_url, discovered_token = discover_session_from_slurmout(slurmout_file)
        except Exception as exc:
            parser.error(
                "Could not determine Jupyter URL/token. Provide --url/--token, set "
                "JUPYTER_SERVER_URL/JUPYTER_SERVER_TOKEN, or ensure a valid slurmout/jupyter-*.out exists. "
                f"Details: {exc}"
            )

        args.url = args.url or discovered_url
        args.token = args.token or discovered_token

    args.url = normalize_jupyter_url(args.url)
    if not args.token:
        parser.error("Jupyter token is missing. Provide --token, set JUPYTER_SERVER_TOKEN, or use a valid slurmout file.")
    return args


def main():
    args = parse_args()
    command = " ".join(args.command)
    output, _ = asyncio.run(
        run_terminal(
            url=args.url,
            token=args.token,
            command=command,
            cwd=args.cwd,
            timeout=args.timeout,
        )
    )
    sys.stdout.write(output)


if __name__ == "__main__":
    main()
