#!/usr/bin/python3
"""
description: enable execution of shell executables as a Python sub-process.

usage: from helpers.cmd_line import CMD

"""
from dataclasses import dataclass, field
from subprocess import PIPE, CalledProcessError, run
from typing import TYPE_CHECKING, List, Union

if TYPE_CHECKING:
    from helpers.inputs import Inputs


@dataclass
class CMD:
    """
    Builds out components for a single shell-executable command.
    """

    # required parameters
    inputs: "Inputs"

    # interal parameters
    _job_cmd: List[str] = field(default_factory=list, repr=False, init=False)
    _log_msg: str = field(default="", init=False, repr=False)

    def __post_init__(self) -> None:
        self._n_new_lines = 0

    def execute(
        self,
        command_list: List[str],
        type: str,
        interactive_mode: bool = False,
        std_input: Union[None, str] = None,
        keep_output: bool = False,
        bypass_errors: bool = False,
    ) -> Union[None, List[str]]:
        """
        Run a command line subprocess and check the output.
        """
        if "query" in type:
            query_list = [c.replace("\t", "\\t") for c in command_list]
            _command_str = " ".join(query_list)
        else:
            _command_str = " ".join(command_list)

        if interactive_mode:
            try:
                if self.inputs.dry_run_mode:
                    self.inputs.logger.info(
                        f"{self.inputs.logger_msg}: pretending to execute the following | '{_command_str}'"
                    )
                    return

                if self.inputs.debug_mode:
                    self.inputs.logger.debug(
                        f"{self.inputs.logger_msg}: starting '{type}' --------------------"
                    )
                if keep_output:
                    result = run(
                        _command_str,
                        check=True,
                        capture_output=True,
                        text=True,
                        shell=True,
                        input=std_input,
                    )
                else:
                    result = run(command_list, check=True, stdout=PIPE)

                if result.returncode == 0:
                    self._errorcode = result.returncode
                    if not result.stdout and result.stderr:
                        output = str(result.stderr).strip()
                    else:
                        output = str(result.stdout).strip()

                    if output == "":
                        return None
                    else:
                        return output.split("\n")
                else:
                    self.inputs.logger.error(
                        f"{self.inputs.logger_msg}: command used | '{_command_str}'"
                    )
                    self.inputs.logger.error(
                        f"{self.inputs.logger_msg}: {result.stdout}"
                    )
                    raise ChildProcessError(f"unable to complete '{type}'")

            except CalledProcessError as err:
                self._errorcode = err.returncode

                if bypass_errors:
                    self._error_trace = [
                        f"unable to execute '{err.cmd}'",
                        err.stderr.strip().split("\n")[-1].split(":")[1].strip(),
                    ]
                    return err.stdout.strip().split("\n")
                else:
                    self.inputs.logger.error(
                        f"{self.inputs.logger_msg}: unable to execute '{err.cmd}'",
                    )
                    print(err)
                    self.inputs.logger.error(f"{err.stderr.strip()}\nExiting... ")
                    exit(err.returncode)
        else:
            self._n_new_lines += 1

            if self._job_cmd:
                self._job_cmd.append(_command_str)
            else:
                self._job_cmd = [_command_str]
