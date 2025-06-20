#!/usr/bin/python3
"""
description: handler for generic command line inputs

example usage: from helpers.inputs import InputManager

"""
from argparse import Namespace
from logging import Logger
from dataclasses import dataclass
from pathlib import Path


@dataclass
class InputManager:
    """
    Save the user-provided inputs for repeated use.
    """
    # required parameters
    args: Namespace
    logger: Logger
    phase: str
    
    # Internal parameters
    _input_path: Union[Path,None] = None
    _output_path: Union[Path,None] = None

    def update_mode(self) -> None:
        self.overwrite = self.args.overwrite
        self.debug_mode = self.args.debug
        self.dry_run_mode = self.args.dry_run

    def create_logging_msg(self) -> None:
        if self.args.dry_run:
            self.logger_msg = f"[DRY_RUN] - [{self.phase}]"
        else:
            self.logger_msg = f"[{self.phase}]"

    def create_a_dir(self, dir_name: Path) -> None:
        """
        Create a new directory.
        """
        if not dir_name.exists() or not dir_name.is_dir():
            if self.dry_run_mode:
                self.logger.info(
                    f"{self.logger_msg}: pretending to create a new directory | '{str(dir_name)}'"
                )
            else:
                if self.debug_mode:
                    self.logger.debug(
                        f"{self.logger_msg}: creating a new directory | '{str(dir_name)}'"
                    )
                dir_name.mkdir(exist_ok=True, parents=True)
