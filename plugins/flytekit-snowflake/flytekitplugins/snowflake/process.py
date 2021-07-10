import asyncio
import dataclasses
import sys
from asyncio import streams
from subprocess import CalledProcessError
from typing import Callable, List, Optional, Any

import dataclasses_json


@dataclasses_json.dataclass_json
@dataclasses.dataclass
class ExecutedProcessResult(object):
    return_code: int
    stdout: bytes
    stderr: bytes


class ExecutedProcessError(CalledProcessError):
    """Raised when execute() is called with check=True and the process
    returns a non-zero exit status.
    """

    def __str__(self):
        return f"{super().__str__()} StdOut: {self.output.decode('utf-8')} StdErr: {self.stderr.decode('utf-8')}"


async def _read_stream(stream: streams.StreamReader, cb: Callable[[bytes], None]):
    while True:
        line = await stream.readline()
        if line:
            cb(line)
        else:
            break


def _output_collector(final: bytearray, delegated_cb: Callable[[bytes], None]) -> Callable[[bytes], None]:
    def res(x: bytes):
        final.extend(x)
        delegated_cb(x)

    return res


def _tee(line: bytes, sink: bytearray, pipe: Optional[Any]):
    sink.extend(line)
    if pipe:
        print(line, file=pipe)


async def _stream_subprocess(cmd: List[str], stdout: Optional[Any],
                             stderr: Optional[Any], **kwds) -> ExecutedProcessResult:
    process = await asyncio.create_subprocess_exec(*cmd,
                                                   stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                                                   **kwds)

    collected_stdout = bytearray()
    collected_stderr = bytearray()

    loop = asyncio.get_event_loop()
    task1 = loop.create_task(
        _read_stream(process.stdout, lambda line: _tee(line, collected_stdout, stdout))
    )

    task2 = loop.create_task(
        _read_stream(process.stderr, lambda line: _tee(line, collected_stderr, stderr))
    )

    await asyncio.wait([task1, task2])

    return_code = await process.wait()
    return ExecutedProcessResult(return_code=return_code, stdout=bytes(collected_stdout),
                                 stderr=bytes(collected_stderr))


def execute(cmd: List[str], stdout: Optional[Any] = None, stderr: Optional[Any] = None,
            check: bool = False, **kwds) -> ExecutedProcessResult:
    """
    Executes a command and streams output and error to passed pipes as well as collect and return them after the process
    exits.
    """

    loop = asyncio.new_event_loop()
    rc = loop.run_until_complete(_stream_subprocess(cmd, stdout, stderr, **kwds))
    loop.close()

    if check and rc.return_code != 0:
        raise ExecutedProcessError(returncode=rc.return_code, cmd=cmd, output=rc.stdout, stderr=rc.stderr)

    return rc


if __name__ == '__main__':
    print(ExecutedProcessError(returncode=1, cmd=["hello"], output=bytes(4), stderr=bytes(4)))
    print(execute(
        cmd=["bash", "-c", "echo regular && sleep 1 && echo bad 1>&2 && sleep 1 && echo done"],
        stdout=sys.stdout,
        stderr=sys.stderr,
        check=True,
    ))
