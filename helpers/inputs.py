#!/usr/bin/python3
"""
description: handler for generic command line inputs

example usage: from helpers.inputs import InputManager

"""
from argparse import Namespace
from logging import Logger
from dataclasses import dataclass
from pathlib import Path
from json import load

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
    
    def load_slurm_resources(self) -> None:
        """
        Open the JSON config file, and confirm the user provided the 'ntasks' parameter as required by Cue
        """
        with open(str(self.args.resource_config), mode="r") as file: # type: ignore
            self.resource_dict = load(file)

        # This is only required for Cue, so update accordingly when updating generic variant caller
        # check_resources = [key for key in self.resource_dict.keys() if key.lower() in 'ntasks']

        # if not check_resources:
        #     self.inputs.logger.error(f"{self.inputs.logger_msg}: missing the 'ntasks' SBATCH parameter in resources file | {self.inputs.args.resource_config}\nExiting...")
        #     exit(1)
