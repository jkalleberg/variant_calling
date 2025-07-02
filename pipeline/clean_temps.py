#!/usr/bin/python3
"""
description: define a single unit for the generic variant calling pipeline

example usage: from pipeline.clean_temps import CleanUp

"""
from dataclasses import dataclass, field
from pathlib import Path
from shutil import rmtree
from subprocess import check_output
from typing import TYPE_CHECKING, Dict, List, Union
from natsort import natsorted
from regex import compile, search

if TYPE_CHECKING:
    from genome import Genome
    from helpers.files import File

from helpers.cmd_line import CMD


@dataclass
class CleanUp:
    """
    Remove any known intermediate files produced during single-sample variant calling.
    """

    # required parameters
    genome: "Genome"

    # internal parameters
    _cmd: List[str] = field(default_factory=list, repr=False, init=False)
    _human_readable: bool = field(default=False, repr=False, init=False)
    _list_of_dirs: List[Path] = field(default_factory=list, repr=False, init=False)
    _num_files: int = field(default=0, init=False, repr=False)
    _num_sub_dirs: int = field(default=0, init=False, repr=False)
    _space_saved_list: List = field(default_factory=list, init=False, repr=False)
    _space_saved: float = field(default=0, init=False, repr=False)
    _tmp_regex: Union[None, str] = field(default_factory=list, init=False, repr=False)
    _total_files: int = field(default=0, init=False, repr=False)
    _valid_dirs_and_files: Dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:

        # Uncomment for Cue
        # confirm the VCF provided already exists
        # self.genome._outputs.check_all_outputs()
        self._command_line = CMD(cl_inputs=self.genome.pipeline_inputs.cl_inputs)
        self._digits_only = compile(r"[-+]?(?:\d*\.*\d+)")

    def check_output(self, default_output: "File") -> None:
        """
        Confirm that the single-sample VCF or BCF file was created successfully.
        """
        # Confirm default output file exists:
        default_output.check_status(should_file_exist=True)
        assert (
            default_output.file_exists and default_output.path.is_file()
        ), f"missing the required VCF or BCF file | '{default_output.path}'\nPlease update the default_output in the pickled Genome() object."

        # Confirm that the default output file has the VCF or BCF file extension
        assert any(
            s in default_output.path.name.lower() for s in ["vcf", "bcf"]
        ), f"missing a VCF or BCF file extension in default_output() | '{default_output.path}'\nPlease update the default_output in the pickled Genome() object."

        # Build a command for BCFtools that will confirm file format is expected
        self._check_cmd = [
            "bcftools",
            "query",
            "--list-samples",
            str(default_output.path),
        ]

        # Determine what type of command is being executed
        if any("zip" in c for c in self._check_cmd):
            type = self._check_cmd[0]
        else:
            type = " ".join(self._check_cmd[:2])

        # Run the BCFtools command as a sub-process
        _result = self._command_line.execute(
            command_list=self._check_cmd,
            type=type,
            interactive_mode=True,
            keep_output=True,
        )

        # Validate that the file contains one sample
        if not self.genome.pipeline_inputs.cl_inputs.dry_run_mode:
            _num_samples = len(_result)
            assert (
                _num_samples == 1
            ), f"expected a single-sample VCF or BCF file, not {_num_samples} samples | '{default_output.path}'\nPlease update the default_output in the pickled Genome() object."

    def set_search_path(self) -> None:
        """
        Determine where to delete intermediate files.
        """
        # Cue-specific paths
        if self.genome._reports_dir is not None:
            for chr in self.genome.pipeline_inputs._chr_names:
                _chr_predictions = (
                    self.genome._sample_dir / chr / "reports" / "predictions"
                )
                if _chr_predictions.is_dir():
                    self._list_of_dirs.append(_chr_predictions)

            _genome_wide_predictions = self.genome._reports_dir / "predictions"
            if _genome_wide_predictions.is_dir():
                self._list_of_dirs.append(_genome_wide_predictions)

            if self.genome._scratch_dir is not None:
                self._list_of_dirs.append(self.genome._scratch_dir)
                _scratch_regex = r".*auxindex"
        else:
            _scratch_regex = None

        # DeepVariant specific variables
        _tmp_regex = r".*tfrecord.*"

        self._tmp_regex = "|".join([r for r in [_scratch_regex, _tmp_regex] if r is not None])

        # Generic paths
        if self.genome._tmp_dir.is_dir():
            self._list_of_dirs.append(self.genome._tmp_dir)

        # Create a dictionary of {"path": ["list", "of", "files"]},
        # where list() contains all intermediate files to be deleted
        for dir in self._list_of_dirs:
            if Path(dir).is_dir():
                self._valid_dirs_and_files.update(
                    {
                        f"{dir}": [child for child in Path(dir).iterdir()],
                    }
                )        

    def check_storage_size(self, path: Union[str, Path]) -> str:
        """
        Provide the human-readable, total size required to store a path.
        """
        valid_units = ["k", "m", "g", "t"]

        # Estimate file space usage with 'du'
        # -s = display the total only
        # -h = human-readable
        if self._human_readable:
            size = check_output(["du", "-sh", path]).split()[0].decode("utf-8")
        else:
            size = check_output(["du", "-s", path]).split()[0].decode("utf-8")

        if any(v in size.lower() for v in valid_units):
            return size
        else:
            return f"{size}K"

    def convert_KtoM(self, value: Union[int, float]) -> float:
        if self._human_readable:
            return value / 1000
        else:
            return value / 1024

    def convert_MtoG(self, value: Union[int, float]) -> float:
        if self._human_readable:
            return value / 1000
        else:
            return value / 1024

    def convert_GtoM(self, value: Union[int, float]) -> float:
        if self._human_readable:
            return value * 1000
        else:
            return value * 1024

    def convert_MtoK(self, value: Union[int, float]) -> float:
        if self._human_readable:
            return value * 1000
        else:
            return value * 1024

    def find_float(self, input: str) -> float:
        """
        Convert an alpha-numeric string to a float.

        Args:
            input (str): alpha-numeric string containing a file size.

        Returns:
            float: numeric float representing file size.
        """
        match = self._digits_only.search(input)
        if match:
            return float(match.group())
        else:
            return float(0)

    def calc_space_saved(self) -> None:
        """
        Determine how much disk saved will be saved after file removal.

        Args:
            total (str): alpha-numeric string containing a file size.
        """
        K_space = 0
        M_space = 0
        G_space = 0

        for val in self._space_saved_list:
            if "g" in val.lower():
                current_number = self.find_float(input=val)
                G_space += current_number

                # Running total uses K
                _increment_by = self.convert_MtoK(self.convert_GtoM(current_number))

            elif "m" in val.lower():
                current_number = self.find_float(input=val)
                M_space += current_number

                # Running total uses K
                _increment_by = self.convert_MtoK(current_number)

            elif "k" in val.lower():
                current_number = self.find_float(input=val)
                K_space += current_number
                _increment_by = current_number

        # Update running total of space saved
        self._space_saved += _increment_by

    def check_space_saved(self, total: str) -> None:
        """_summary_
        """
        _total_val = self.find_float(input=total)
        if "g" in total.lower():
            _total_g = _total_val
        elif "m" in total.lower():
            _total_g = self.convert_MtoG(_total_val)
        elif "k" in total.lower():
            _total_g = self.convert_MtoG(self.convert_KtoM(_total_val))

        _total_saved_g = self.convert_MtoG(self.convert_KtoM(self._space_saved))

        _str = f"{round(_total_saved_g, ndigits=3)}G-of-{round(_total_g, ndigits=3)}G"

        if self.genome.pipeline_inputs.cl_inputs.dry_run_mode:
            self.genome.pipeline_inputs.cl_inputs.logger.info(
                    f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: pretending to delete {self._things_to_delete}-of-{self._num_files} items would save {_str}"
                )
        else:
            self.genome.pipeline_inputs.cl_inputs.logger.info(
                f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: disk space cleared after deleting {self._things_to_delete}-of-{self._num_files} items | {_str}"
            )

    def remove_specific_file(self, file_path: Path) -> None:
        """
        Selectively remove a single file based on a regular expression for the file extension.
        """
        # If input file matches a pattern...
        file = search(pattern=self._tmp_regex, string=str(file_path.name))

        # And a match was found
        if file is not None:

            file_found = file.group()

            self._num_files += 1
            self._things_to_delete += 1

            _size = self.check_storage_size(path=file_path)
            self._space_saved_list.append(_size)

            if self._human_readable:
                size = _size
            else:
                if "k" in _size.lower() and self.find_float(_size) <= 1000:
                    size = f"<= 1K"
                else:
                    _size_G = self.convert_MtoG(self.convert_KtoM(self.find_float(_size)))
                    size = f"{round(_size_G, ndigits=2)}G"

            # if DRY RUN, nothing will be deleted
            # otherwise, EXISTING FILES WILL BE DELETED PERMANENTLY

            if self.genome.pipeline_inputs.cl_inputs.dry_run_mode is True:
                self.genome.pipeline_inputs.cl_inputs.logger.info(
                    f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: pretending to remove {size} within a file\t| '{file_found}'"
                )
            else:
                if self.genome.pipeline_inputs.cl_inputs.debug_mode:
                    self.genome.pipeline_inputs.cl_inputs.logger.debug(
                        f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: removed {size} within a file\t| '{file_found}'"
                    )
                file_path.unlink()

            if self._things_to_delete % 100 == 0 or self._things_to_delete == self._total_files:
                self.genome.pipeline_inputs.cl_inputs.logger.info(
                    f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: running total of files reviewed | {int(self._num_files):,}-of-{int(self._total_files):,} file(s)"
                )
        else:
            self._num_files += 1
            print("NO FILE FOUND")
            breakpoint()

    def remove_dir(self, dir_path: Path) -> None:
        """
        Handle any sub-files. Then, remove an empty dir.
        """
        self._num_sub_dirs += 1
        size = self.check_storage_size(path=dir_path)
        self._space_saved.append(size)
        self._things_to_delete += 1
        if self.genome.pipeline_inputs.cl_inputs.dry_run_mode:
            self.genome.pipeline_inputs.cl_inputs.logger.info(
                f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: pretending to remove {size} within directory\t| '{str(dir_path)}'"
            )
        else:
            rmtree(dir_path)
            if self.genome.pipeline_inputs.cl_inputs.debug_mode:
                self.genome.pipeline_inputs.cl_inputs.logger.debug(
                    f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: removed {size} within directory\t| '{str(dir_path)}'"
                )

    def remove_all_intermediates(self) -> None:
        """
        Save storage space by removing the bigger Cue intermediate files.
        """
        total_space = self.check_storage_size(path=self.genome._sample_dir)
        self.set_search_path()

        self._things_to_delete = 0

        for dir, children in self._valid_dirs_and_files.items():

            self._total_files = len(children)

            if self.genome.pipeline_inputs.cl_inputs.dry_run_mode:
                self.genome.pipeline_inputs.cl_inputs.logger.info(
                    f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: directory found | '{dir}', contains {self._total_files:,} files."
                )

            if Path(dir).exists():
                dir_size = self.check_storage_size(path=dir)

                # Remove nested sub-directories and specific files
                for child in natsorted(children):
                    if child.is_dir():
                        self.remove_dir(dir_path=child)
                    elif child.is_file() and self._tmp_regex is not None:
                        self.remove_specific_file(file_path=child)
                        self.calc_space_saved()
                    else:
                        continue

        if self._things_to_delete == 0:
            self.genome.pipeline_inputs.cl_inputs.logger.info(
                f"{self.genome.pipeline_inputs.cl_inputs.logger_msg}: there is nothing to delete."
            )
        else:
            self.calc_space_saved()
            self.check_space_saved(total=total_space)
