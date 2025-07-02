#!/usr/bin/python3
"""
description: custom model to help write new python3 modules

example usage: from helpers.module_builder import CustomModule

"""

import argparse
from dataclasses import dataclass, field
from os import path as p
from pathlib import Path
from sys import path
from traceback import extract_stack
from typing import Union, List, TYPE_CHECKING

if TYPE_CHECKING:
    import logging.Logger
    from argparse import ArgumentParser, Namespace
    from pathlib import Path

abs_path = Path(__file__).resolve()
module_path = str(abs_path.parent.parent)
path.append(module_path)
from helpers.utils import get_logger
from helpers.wrapper import Wrapper, timestamp


@dataclass
class CustomModule:
    """
    Enable fast development of custom Python modules.
    """
    # Optional parameters
    output_required: bool = True
    
    # internal parameters
    _args: Union[None, "Namespace"] = field(default=None, init=False, repr=False)
    _current_file: Union[None, str] = field(default=None, init=False, repr=False)
    _debug_mode: bool = field(default=False, init=False, repr=False)
    _dry_run_mode: bool = field(default=False, init=False, repr=False)
    _input_path: Union[None, "Path"] = field(default=None, init=False, repr=False)
    _logger: Union[None, "logging.Logger"] = field(default=None, init=False, repr=False)
    _logger_msg: Union[None, str] = field(default=None, init=False, repr=False)
    _module_name: Union[None, str] = field(default=None, init=False, repr=False)
    _output_path: Union[None, "Path"] = field(default=None, init=False, repr=False)
    _overwrite: bool = field(default=False, init=False, repr=False) 
    _parser: Union[None, "ArgumentParser"] = field(default=None, init=False, repr=False)

    def get_caller_file(self) -> None:
        """
        Retrieves the filename of the script that called the function.
        """
        if __name__ == "__main__":
            self._current_file = __name__
            # print(self.__module__)
            # print(__name__)
        else:
            stack = extract_stack()
            modules = [x for x in stack if "importlib" not in x.filename]
            self._current_file = modules[0].filename

        self._module_name = p.splitext(p.basename(self._current_file))[0]
        self._logger = get_logger(self._module_name)

    def build_args(self) -> None:
        """
        Defines standard command line arguments which are repeatedly used.
        """
        self._parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        if self.output_required:
            self._parser.add_argument(
                "-O",
                "--output-path",
                dest="out_path",
                type=str,
                help="[REQUIRED]\noutput path\nLocation to save resulting file(s).",
                metavar="</path/>",
            )
        self._parser.add_argument(
            "-I",
            "--input",
            dest="input",
            type=str,
            help="[REQUIRED]\ninput path\nIf a directory is provided, multiple inputs will be identified.\nIf a file is provided, only that file will be used as input.",
            metavar="</path/to/dir/> or </path/to/file>",
        )
        self._parser.add_argument(
            "--overwrite",
            dest="overwrite",
            help="If True, enables re-writing files.",
            default=False,
            action="store_true",
        )
        self._parser.add_argument(
            "-d",
            "--debug",
            dest="debug",
            help="If True, enables printing detailed messages.",
            default=False,
            action="store_true",
        )
        self._parser.add_argument(
            "--dry-run",
            dest="dry_run",
            help="If True, display results to the screen, rather than to a file.",
            action="store_true",
        )
        self._parser.add_argument(
            "-v",
            "--version",
            action="version",
            version="%(prog)s version 0.1",
            help="show program's version number and exit",
        )

    def collect_args(self, manual_args: Union[List[str], None] = None) -> None:
        """
        Process command line argument to execute a script.
        """
        if manual_args is None:
            self._args = self._parser.parse_args()
        else:
            self._args = self._parser.parse_args(manual_args)
        
        if self._args.debug:
            str_args = "COMMAND LINE ARGS USED: "
            for key, val in vars(self._args).items():
                str_args += f"{key}={val} | "

            self._logger.debug(str_args)

        if self._args.dry_run:
            self._logger.info(
                "[DRY_RUN]: output will display to screen and not write to a file"
            )

    def check_args(self) -> None:
        """
        With "--debug", display command line args provided.
        With "--dry-run", display a msg.
        Then, check to make sure all required flags are provided.
        """
        if self.output_required:
            assert (
                self._args.out_path
            ), "missing [REQUIRED] flag: --output; Please provide a directory or file name for saving results"

            # Resolve potential relative path entered
            _resolved_out_path = Path(self._args.out_path).resolve()
            self._args.out_path = _resolved_out_path
        
        assert (
            self._args.input
        ), "missing [REQUIRED] flag: --input; Please provide either a directory location or an existing file"
        
        # Resolve potential relative path entered
        _resolved_in_path = Path(self._args.input).resolve()
        
        # Confirm resolved Path is valid
        assert (_resolved_in_path.is_file() or _resolved_in_path.is_dir()), f"unable to find the input path | '{_resolved_in_path}'"
        self._args.input = _resolved_in_path
    
    def get_arg_default(self, arg_name: str = "input") -> str:
        """
        Provide the default value for a command-line argument.
        """
        return self._parser.get_default(arg_name)
     
    def process_args(self) -> None:
        """
        Handle expected defaults provide at the command line.
        """
        if self._args.dry_run:
            self._logger_msg = f"[DRY_RUN] - [{self._module_name}]"
        else:
            self._logger_msg = f"[{self._module_name}]"

        if self.output_required:
            self._output_path = Path(self._args.out_path).resolve()
        else:
            self._output_path = None
        
        self._input_path = Path(self._args.input).resolve()
        self._debug_mode = self._args.debug
        self._dry_run_mode = self._args.dry_run
        self._overwrite = self._args.overwrite

    def start_module(self) -> None:
        """
        Initialize a new module.
        """
        # Define the module being created
        self.get_caller_file()

        # Collect start time
        Wrapper(self._current_file, "start").wrap_script(timestamp())

        # Collect command line arguments
        self.build_args()

        # If you want to add customized arguments, do
        # self._parser.add_argument() after start_module()
        # NOTE: If any of them are [REQUIRED], be sure to add
        # additional assert () statements after check_args()!

    def end_module(self) -> None:
        """
        Terminate a new module.
        """
        Wrapper(self._current_file, "end").wrap_script(timestamp())
