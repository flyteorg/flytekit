from __future__ import absolute_import, print_function

import logging
import shlex as _schlex
import subprocess as _subprocess
import tempfile as _tempfile


def check_call(cmd_args, **kwargs):
    if not isinstance(cmd_args, list):
        cmd_args = _schlex.split(cmd_args)

    # Jupyter notebooks hijack I/O and thus we cannot dump directly to stdout.
    with _tempfile.TemporaryFile() as std_out:
        with _tempfile.TemporaryFile() as std_err:
            ret_code = _subprocess.Popen(cmd_args, stdout=std_out, stderr=std_err, **kwargs).wait()

            # Dump sub-process' std out into current std out
            std_out.seek(0)
            logging.info("Output of command '{}':\n{}\n".format(cmd_args, std_out.read()))

            if ret_code != 0:
                std_err.seek(0)
                err_str = std_err.read()
                logging.error("Error from command '{}':\n{}\n".format(cmd_args, err_str))

                raise Exception(
                    "Called process exited with error code: {}.  Stderr dump:\n\n{}".format(ret_code, err_str)
                )

    return 0
