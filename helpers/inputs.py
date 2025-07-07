#!/usr/bin/python3
"""
description: handler for generic command line inputs

example usage: from helpers.inputs import InputManager

"""
from argparse import Namespace
from logging import Logger
from dataclasses import dataclass, field
from pathlib import Path
from json import load
from typing import Dict, List, Union
from sys import exit
from regex import search, Pattern
from natsort import natsorted
from os import listdir

from helpers.utils import partial_match_case_insensitive, check_if_all_same

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
    _files: List[str] = field(default_factory=list, init=False, repr=False)
    _input_path: Union[Path,None] = field(default=None, init=False, repr=False)
    _n_files: int = field(default=0, init=False, repr=False)
    _n_unique_files: int = field(default=0, init=False, repr=False)
    _output_path: Union[Path, None] = field(default=None, init=False, repr=False)
    _outputs_exist: bool = field(default=False, init=False, repr=False)
    _unique_files: List[str] = field(default_factory=list, init=False, repr=False)

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
        Open the JSON config file, and save to an internal variable.
        """
        with open(str(self.args.resource_config), mode="r") as file: # type: ignore
            self.resource_dict = load(file)

            # This is only required for Cue, so update accordingly when updating generic variant caller
            # check_resources = [key for key in self.resource_dict.keys() if key.lower() in 'ntasks']

            # if not check_resources:
            #     self.inputs.logger.error(f"{self.inputs.logger_msg}: missing the 'ntasks' SBATCH parameter in resources file | {self.inputs.args.resource_config}\nExiting...")
            #     exit(1)

    def load_model_config(self) -> None:
        """
        Iterate through a list of JSON config file(s), and save the values as a list of dictionaries.
        """
        for config in self.args.model_config:
            print("CONFIG:", config)
            breakpoint()
        # with open(str(self.args.resource_config), mode="r") as file:  # type: ignore
        #     self.resource_dict = load(file)
        
        # # Check for supported variant callers
        # _use_deepvariant = partial_match_case_insensitive("deepvariant", _ckpt_list)
        # _use_cue = partial_match_case_insensitive("cue", _ckpt_list)

        # # Confirm at least one supported variant caller was provided
        # _no_valid_checkpoint = check_if_all_same([_use_deepvariant, _use_cue], None)
        # assert (_no_valid_checkpoint is False), f"unable to find a supported checkpoint (e.g., DeepVariant or Cue) | '{run._args.model_prefix}'"

        # # Get the expected default checkpoint path (custom bovid-trained WGS AF)
        # _default_ckpt_prefix = Path(run.get_arg_default("model_prefix")).resolve()

        # # Create an empty list to store valid checkpoint paths
        # _list_of_ckpt_prefixes = list()

        # if _use_deepvariant and len(_use_deepvariant) == 1:

        #     # Get the value of 'BIN_VERSION_DV', return None if not set
        #     _dv_version = getenv("BIN_VERSION_DV")

        #     # Confirm this environment variable exists
        #     assert (
        #         _dv_version is not None
        #     ), f"missing [REQUIRED] environment variable: ($BIN_VERSION_DV); Please double check that this variable is included in your modules.sh file"

        #     # Do not allow the user to deviate from v1.4.0
        #     assert (
        #         _dv_version == "1.4.0"
        #     ), f"invalid environment variable ($BIN_VERSION_DV); Please edit your modules.sh file to use the expected version of DeepVariant"
        #     # NOTE: In future, newer versions may become supported, but as they are untested, we do not encourage deviating from this expectation.

        #     # Identify the DeepVariant checkpoint prefix entered
        #     _user_ckpt_prefix = Path(_use_deepvariant[0]).resolve()

        #     # Determine if using the pipeline's default DeepVariant checkpoint (model.ckpt-282383),
        #     if _user_ckpt_prefix == _default_ckpt_prefix:

        #         # If so, make the flag --allele-freq [REQUIRED]
        #         assert (
        #             run._args.pop_file
        #         ), "missing [REQUIRED] flag: --allele-freq; Please add a PopVCF to use the custom bovine-trained checkpoint (model.ckpt-282383)"

        #         # Resolve any relative path entered for --allele-freq
        #         _resolved_pop_path = Path(run._args.pop_file).resolve()

        #         # Confirm the PopVCF file is available
        #         assert (_resolved_pop_path.is_file() is True), f"unable to find the PopVCF file | '{_resolved_pop_path}'"
        #         run._args.pop_file = _resolved_pop_path

        #         _list_of_ckpt_prefixes.append(_user_ckpt_prefix)

        #     else:
        #         print("ADD LOGIC FOR DIFFERENT DEEPVARIANT CHECKPOINTS")
        #         breakpoint()

        #     # Confirm that all the expected DeepVariant v1.4 checkpoint files are available
        #     _checkpoint_files = iterdir_with_prefix(
        #         absolute_path=_user_ckpt_prefix.parent,
        #         prefix=_user_ckpt_prefix.name,
        #         valid_suffixes=[".data-00000-of-00001", ".json", ".index", ".meta",],
        #         )

        #     assert (len(_checkpoint_files) == 4), f"unable to find all four DeepVariant checkpoint files | '{_user_ckpt_prefix}'"

        # if _use_cue:
        #     print("ADD LOGIC CUE CHECKPOINT")
        #     breakpoint()

        # # Confirm that a model checkpoint was entered
        # assert (len(_list_of_ckpt_prefixes) >= 1), f"unable to find at least one valid checkpoint | '{_user_ckpt_prefix}'"

        # # Save the list as a new command-line argument
        # run._args.model_prefix = _list_of_ckpt_prefixes

        # Determine the variant caller(s) requested by the user
        # Currently supported valid options:
        #   DeepVariant v1.4.0
        # In future, we plan to support:
        #   DeepVariant v1.5.0+
        #   DeepTrio v1.5.0
        #   Cue v####
        # NOTE: this process expects the input checkpoint to be formatted as:
        #       ./tutorial/existing_ckpts/<MODEL_TYPE>/<MODEL_VERSION>/<CHECKPOINT_NAME>

        # Save info about the model(s) requested
        # _variant_callers = dict()
        # for ckpt in _ckpt_list:
        #     _checkpoint_path = Path(ckpt).resolve()
        #     _model_type = _checkpoint_path.parent.parent.name
        #     _model_version = _checkpoint_path.parent.name
        #     _checkpoint_name = _checkpoint_path.name
        #     _variant_callers[_model_type] = {"version": _model_version,
        #                                      "checkpoint_name": _checkpoint_name,
        #                                      "checkpoint_path": _checkpoint_path.parent}



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
