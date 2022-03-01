#! /usr/bin/python3

import subprocess
import sys
import io
import asyncio

args = sys.argv[1:] # First arg is our script

#o = io.BytesIO()
#e = io.BytesIO()
#process = subprocess.run(file, stdin=sys.stdin, stdout=o, stderr=e)

# Yeeted from: https://stackoverflow.com/questions/63887183/is-it-possible-to-read-multiple-asyncio-streams-concurrently
async def _read_all(stream, echo):
    # helper function to read the whole stream, optionally
    # displaying data as it arrives
    buf = io.BytesIO()  # BytesIO is preferred to +=
    while True:
        chunk = await stream.read(4096)
        if len(chunk) == 0:
            break
        buf.write(chunk)
        if echo:
            sys.stdout.buffer.write(chunk)
    return buf.getvalue()

async def run_command(*args, stdin=None, echo=False):
    process = await asyncio.create_subprocess_exec(
        *args,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    if stdin is not None:
        process.stdin.write(stdin)

    process.stdin.close()
    stdout, stderr = await asyncio.gather(
        _read_all(process.stdout, echo),
        _read_all(process.stderr, echo)
    )
    return process.returncode, stdout, stderr

async def main():
    code, stdout, stderr = await run_command("python3", *args, echo=True)

    if code != 0:
        o = open('output.log', 'w')
        o.write(stdout.decode().strip())
        o.close()

        e = open('error.log', 'w')
        e.write(stderr.decode().strip())
        e.close()

    sys.exit(code)

asyncio.run(main())