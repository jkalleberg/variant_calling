#!/usr/bin/python3
"""
description: handler for generic command line inputs

example usage: from helpers.inputs import InputManager

"""
# from argparse import Namespace
# from logging import Logger
from dataclasses import dataclass, field
from pathlib import Path
from json import load
from typing import Dict, List, Union, TYPE_CHECKING
from sys import exit
from regex import search, Pattern
from natsort import natsorted
from os import listdir

if TYPE_CHECKING:
    from helpers.module_builder import CustomModule



@dataclass
class InputManager:
    """
    Save the user-provided inputs for repeated use.
    """
    # required parameters
    custom_module: "CustomModule"
    phase: str

    # Internal parameters
    _files: List[str] = field(default_factory=list, init=False, repr=False)
    _input_path: Union[Path,None] = field(default=None, init=False, repr=False)
    _n_files: int = field(default=0, init=False, repr=False)
    _n_unique_files: int = field(default=0, init=False, repr=False)
    _output_path: Union[Path, None] = field(default=None, init=False, repr=False)
    _outputs_exist: bool = field(default=False, init=False, repr=False)
    _unique_files: List[str] = field(default_factory=list, init=False, repr=False)

    def update_mode(self) -> None:
        self.args = self.custom_module._args
        self.logger = self.custom_module._logger
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
        Open the JSON config file, and save to an internal variable.
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

        If the key is missing from the dictionary, add the 'key=value pair' to the dictionary.

        Args:
            update_dict (Dict[str, Union[str, int, float]]): the dictionary object to be altered.
            new_key (str): unique hash key.
            new_val (Union[str, int, float]): the new value for a new key.
            valid_keys (Union[List[str], None], optional): if provided, compare the new key against this list to catch typos or invalid entries. Defaults to None.
            replace_value (bool, optional): if True, overwrite the value of an existing key. Defaults to False.
            updated_log_msg (Union[None, str], optional): if provided, edit the content of logging messages. Defaults to None.
            verbose (bool, optional): if True, provide additional logging messages. Defaults to False.
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
                self.logger.info(
                    f"{_log_msg}: previous value replaced with | '{new_key}={new_val}'"
                )
        else:
            self.logger.warning(
                f"{_log_msg}: unable to overwrite value for an existing key | '{new_key}'"
            )

    def check_outputs(self,
                      search_path: Path,
                      match_pattern: Pattern,
                      file_type: str,
                      updated_log_msg: Union[None, str] = None,
                      ) -> None:
        """
        Confirms if file(s) matching a regular expression already exist, and counts the number of matches found.

        Args:
            search_path (Path): absolute path to search for file(s).
            match_pattern (Pattern): regular expression for file name(s).
            file_type (str): a description of the files being searched.
            updated_log_msg (Union[None, str], optional): if provided, edit the content of logging messages. Defaults to None.
        """
        if updated_log_msg is not None:
            _log_msg = updated_log_msg
        else:
            _log_msg = self.logger_msg

        _files = list()

        if search_path.exists():
            if Path(search_path).is_dir():
                for file in listdir(str(search_path)):
                    match = search(match_pattern, str(file))
                    if match:
                        _files.append(match.group())

                self._unique_files = list(natsorted(set(_files)))
                self._n_unique_files = len(self._unique_files)

                if self.debug_mode:
                    self.logger.debug(f"{_log_msg} - [outputs]: files found | {self._unique_files}")

                for file in _files:
                    filename: Path = search_path / file
                    if filename.exists():
                        self._n_files += 1  

            self._files = _files         
        else:
            if not self.dry_run_mode:
                self.logger.warning(
                        f"{_log_msg} - [outputs]: unable to search a non-existent path | '{str(search_path)}'"
                )

        if self._n_files > self._n_unique_files:
            self.logger.warning(f"{_log_msg} - [outputs]: pattern provided returns duplicate files")
            self.logger.error(f"{_log_msg} - [outputs]: please use a more specific regex.\nExiting...")
            exit(1)

        if self._n_files == 0:
            if self.debug_mode:
                self.logger.info(f"{_log_msg}: missing {file_type}")
        else:
            if self.debug_mode:
                self.logger.debug(f"{_log_msg} - [outputs]: found [{int(self._n_files):,}] {file_type}")
            self._outputs_exist = True

    # def check_expected_outputs(
    #     self,
    #     outputs_found: int,
    #     outputs_expected: int,
    #     file_type: str,
    #     verbose: bool = True,
    #     updated_log_msg: Union[None, str] = None,
    # ) -> bool:
    #     """Confirms if expected outputs were made correctly.

    #     Parameters
    #     ----------
    #     outputs_found : int
    #         how many outputs were identified
    #     outputs_expected : int
    #         how many outputs should be identified
    #     file_type : str
    #         general descriptor for the files to find
    #     verbose: bool
    #         if True, print additional logging msgs

    #     Returns
    #     -------
    #     bool
    #         if True, 1+ expected files are missing
    #     """
    #     if updated_log_msg is not None:
    #         _log_msg = updated_log_msg
    #     else:
    #         _log_msg = self.logger_msg

    #     if outputs_found == outputs_expected:
    #         if outputs_expected == 1:
    #             if verbose:
    #                 self.logger.info(
    #                     f"{_log_msg}: found the {int(outputs_found):,} expected {file_type}... SKIPPING AHEAD"
    #                 )
    #         else:
    #             self.logger.info(
    #                 f"{_log_msg}: found all {int(outputs_found):,} expected {file_type}... SKIPPING AHEAD"
    #             )
    #         missing_outputs = False
    #     else:
    #         if int(outputs_expected) > int(outputs_found):
    #             self.logger.info(
    #                 f"{_log_msg}: missing {int(int(outputs_expected) - int(outputs_found)):,}-of-{int(outputs_expected):,} {file_type}"
    #             )
    #             missing_outputs = True
    #         else:
    #             self.logger.warning(
    #                 f"{_log_msg}: found {int(int(outputs_found)-int(outputs_expected)):,} more {file_type} than expected!"
    #             )
    #             missing_outputs = False

    #     return missing_outputs
