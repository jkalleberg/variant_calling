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
from typing import Dict, List, Union
from sys import exit

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

    def create_a_dir(self, dir_name: Path, updated_log_msg: Union[None, str] = None) -> None:
        """
        Create a new directory.
        """
        if updated_log_msg is not None:
            _log_msg = updated_log_msg
        else:
            _log_msg = self.logger_msg
        
        if not dir_name.exists() or not dir_name.is_dir():
            if self.dry_run_mode:
                self.logger.info(
                    f"{_log_msg}: pretending to create a new directory | '{str(dir_name)}'"
                )
            else:
                if self.debug_mode:
                    self.logger.debug(
                        f"{_log_msg}: creating a new directory | '{str(dir_name)}'"
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
    
    def add_to_dict(
        self,
        update_dict: Dict[str, Union[str, int, float]],
        new_key: str,
        new_val: Union[str, int, float],
        valid_keys: Union[List[str], None] = None,
        replace_value: bool = False,
        updated_log_msg: Union[None, str] = None,
        verbose: bool = False,
    ) -> None:
        """
        Add a key=value pair to a dictionary object.

        If the key is missing from the dictionary the 'key=value pair' to the results dictionary.

        Parameters
        ----------
        update_dict : Dict[str, Union[str, int, float]]
            the dictionary object to be altered
        new_key : str
            unique hash key
        new_val : Union[str, int, float]
            the value returned by the new key
        valid_keys : Union[List[str], None], optional
            if provided, compare the new against this list to catch typos or invalid entries, by default None
        replace_value : bool, optional
            if True, overwrite the value of an existing key, by default False
        """
        if updated_log_msg is not None:
            _log_msg = updated_log_msg
        else:
            _log_msg = self.logger_msg
            
        if valid_keys is not None:
            if new_key not in valid_keys:
                self.logger.error(f"{_log_msg}: invalid metadata key | '{new_key}'")
                valid_key_string: str = ", ".join(valid_keys)
                self.logger.error(
                    f"{_log_msg}: use one of the following valid keys | '{valid_key_string}'\nExiting..."
                )
                exit(1)

        if new_key not in update_dict.keys():
            update_dict[new_key] = new_val
            if self.debug_mode:
                self.logger.debug(f"{_log_msg}: dictionary updated with | '{new_key}={new_val}'")
        
        elif new_key in update_dict.keys() and replace_value:
            old_value = update_dict[new_key]
            update_dict[new_key] = new_val
            if self.debug_mode and verbose:
                self.logger.debug(
                    f"{_log_msg}: previous value '{new_key}={old_value}' | new value '{new_key}={new_val}'"
                )
            else:
                self.logger.debug(
                    f"{_log_msg}: previous value replaced with | '{new_key}={new_val}'"
                )
        else:
            self.logger.warning(
                f"{_log_msg}: unable to overwrite value for an existing key | '{new_key}'"
            )

