from dataclasses import dataclass, field
from pathlib import Path
from shutil import rmtree
from subprocess import check_output
from typing import TYPE_CHECKING, Dict, List, Union

from regex import compile, search

if TYPE_CHECKING:
    from genome import Genome


@dataclass
class CleanUp:
    """
    Provide descriptions of a SV VCF produced by Cue.
    """

    # required parameters
    genome: "Genome"

    # interal parameters
    _cmd: List[str] = field(default_factory=list, repr=False, init=False)
    _list_of_dirs: List[str] = field(default_factory=list, repr=False, init=False)
    _num_files: int = field(default=0, init=False, repr=False)
    _num_sub_dirs: int = field(default=0, init=False, repr=False)
    _space_saved: List = field(default_factory=list, init=False, repr=False)
    _total_files: int = field(default=0, init=False, repr=False)
    _valid_dirs_and_files: Dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        # confirm the VCF provided already exists
        self.genome._outputs.check_all_outputs()

    def set_search_path(self) -> None:
        """
        Determine where to delete intermediate files
        """
        for chr in self.genome.iter._chr_names:
            self._list_of_dirs.append(
                self.genome._sample_dir / chr / "reports" / "predictions"
            )
        self._list_of_dirs.append(self.genome._reports_dir / "predictions")
        self._tmp_dir = Path(self.genome._reports_dir.parent) / "tmp"
        self._scratch_dir = Path(self.genome._reports_dir.parent) / "scratch"

    def create_search_patterns(self) -> None:
        """
        Defines the fnmatch search pattern for temporary files in multiple directories.
        """
        self._tmp_regex = r".*auxindex"
        self._digits_only = compile(r"[-+]?(?:\d*\.*\d+)")
        if self._tmp_dir.is_dir():
            self._valid_dirs_and_files.update(
                {
                    f"{self._tmp_dir}": [file for file in self._tmp_dir.iterdir()],
                }
            )

        if self._scratch_dir.is_dir():
            self._valid_dirs_and_files.update(
                {
                    f"{self._scratch_dir}": [
                        file for file in self._scratch_dir.iterdir()
                    ],
                }
            )
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
        size = check_output(["du", "-sh", path]).split()[0].decode("utf-8")
        if any(v in size.lower() for v in valid_units):
            return size
        else:
            return f"{size}K"

    def convert_KtoM(self, value: Union[int, float]) -> float:
        return value / 1000

    def convert_MtoG(self, value: Union[int, float]) -> float:
        return value / 1000

    def convert_GtoM(self, value: Union[int, float]) -> float:
        return value * 1000

    def find_float(self, input: str) -> float:
        match = self._digits_only.search(input)
        if match:
            return float(match.group())
        else:
            return float(0)

    def calc_space_saved(self, total: str) -> None:
        K_space = 0
        M_space = 0
        G_space = 0

        total_number = self.find_float(input=total)

        for val in self._space_saved:
            if "k" in val.lower():
                current_number = self.find_float(input=val)
                K_space += current_number
            elif "m" in val.lower():
                current_number = self.find_float(input=val)
                M_space += current_number
            elif "g" in val.lower():
                current_number = self.find_float(input=val)
                G_space += current_number

        if "g" in total.lower() and M_space != 0:
            G_space += self.convert_MtoG(M_space)
            if round(G_space, ndigits=1) == total_number:
                self.genome.iter.inputs.logger.info(
                    f"{self.genome._log_msg}: deleted {self._things_to_delete} items | saved {total_number}G"
                )
            else:
                self.genome.iter.inputs.logger.info(
                    f"{self.genome._log_msg}: deleted {self._things_to_delete} items | {round(G_space, ndigits=1)}G-of-{total_number}G"
                )
        elif "m" in total.lower() and K_space != 0:
            M_space += self.convert_KtoM(K_space)
            if round(M_space, ndigits=1) == total_number:
                self.genome.iter.inputs.logger.info(
                    f"{self.genome._log_msg}: deleted {self._things_to_delete} items | saved {total_number}M"
                )
            else:
                self.genome.iter.inputs.logger.info(
                    f"{self.genome._log_msg}: deleted {self._things_to_delete} items | {round(M_space, ndigits=1)}M-of-{total_number}M"
                )
        else:
            print("UNEXPECTED CONDITION... FIX ME!")

    def remove_file(self, file_path: Path) -> None:
        """
        Deletes file_path if it has an extension found extensions_list.
        """
        # if a file matches a pattern...
        file = search(pattern=self._tmp_regex, string=str(file_path.name))

        if file is not None:
            file_found = file.group()
            self._num_files += 1
            self._things_to_delete += 1
            size = self.check_storage_size(path=file_path)
            self._space_saved.append(size)
            # if DRY RUN, nothing will be deleted
            # otherwise, EXISTING FILES WILL BE DELETED PERMANENTLY
            self._things_to_delete += 1
            if self.genome.iter.inputs.dry_run_mode:
                self.genome.iter.inputs.logger.info(
                    f"{self.genome._log_msg}: pretending to remove {size} within a file\t| '{file_found}'"
                )
            else:
                if self.genome.iter.inputs.debug_mode:
                    self.genome.iter.inputs.logger.debug(
                        f"{self.genome._log_msg}: removed {size} within a file\t| '{file_found}'"
                    )
                file_path.unlink()

            if self._num_files % 100 == 0:
                self.genome.iter.inputs.logger.info(
                    f"{self.genome._log_msg}: running total of files for removal | {int(self._num_files):,}-of-{int(self._total_files):,}"
                )

    def remove_dir(self, dir_path: Path) -> None:
        """
        Handle any sub-files. Then, remove an empty dir.
        """
        self._num_sub_dirs += 1
        size = self.check_storage_size(path=dir_path)
        self._space_saved.append(size)
        self._things_to_delete += 1
        if self.genome.iter.inputs.dry_run_mode:
            self.genome.iter.inputs.logger.info(
                f"{self.genome._log_msg}: pretending to remove {size} within directory\t| '{str(dir_path)}'"
            )
        else:
            rmtree(dir_path)
            self.genome.iter.inputs.logger.info(
                f"{self.genome._log_msg}: removed {size} within directory\t| '{str(dir_path)}'"
            )

    def remove_lg_intermediates(self) -> None:
        """
        Save storage space by removing the bigger Cue intermediate files.
        """
        total_space = self.check_storage_size(path=self.genome._sample_dir)
        self.set_search_path()
        self.create_search_patterns()
        self._things_to_delete = 0
        for dir, children in self._valid_dirs_and_files.items():
            self._total_files = len(children)
            if Path(dir).exists():
                # dir_size = self.check_storage_size(path=dir)
                for child in children:
                    if child.is_dir():
                        self.remove_dir(dir_path=child)
                    elif child.is_file():
                        self.remove_file(file_path=child)
                        # self.calc_space_saved(total=dir_size)
                    else:
                        continue

        if self._things_to_delete == 0:
            self.genome.iter.inputs.logger.info(
                f"{self.genome._log_msg}: there is nothing to delete."
            )
        else:
            self.calc_space_saved(total=total_space)