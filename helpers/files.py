#!/usr/bin/python3
"""
description: flexible custom handler for frequently used file format(s).

usage:
    from helpers.files import File
"""
from csv import QUOTE_NONE, DictReader, DictWriter, writer
from dataclasses import dataclass, field
from logging import Logger
from pathlib import Path
from typing import Dict, List, Union, TYPE_CHECKING
from pickle import HIGHEST_PROTOCOL, dump, load
import pandas as pd
import json

from helpers.suffix import remove_suffixes

if TYPE_CHECKING:
    from helpers.inputs import InputManager
    from pipeline.genome import Genome


class TestFile:
    """Confirm if a file already exists or not."""

    def __init__(self, file: Union[str, Path], logger: Logger) -> None:
        self.file = str(file)
        self.path = Path(file)
        self.file_exists: bool
        self.logger = logger
        self.clean_filename = remove_suffixes(self.path)

    def check_missing(
        self, logger_msg: Union[str, None] = None, debug_mode: bool = False
    ) -> None:
        """
        Confirms if a file is non-existent.
        """
        if logger_msg is None:
            msg = ""
        else:
            msg = f"{logger_msg}: "
        if self.path.is_file():
            if debug_mode:
                self.logger.debug(
                    f"{msg}'{str(self.path)}' already exists... SKIPPING AHEAD"
                )
            self.file_exists = True
        else:
            if debug_mode:
                self.logger.debug(f"{msg}file is missing, as expected | '{self.file}'")
            self.file_exists = False

    def check_existing(
        self, logger_msg: Union[str, None] = None, debug_mode: bool = False
    ) -> None:
        """
        Confirms if a file exists already.
        """
        if logger_msg is None:
            msg = ""
        else:
            msg = f"{logger_msg}: "

        if self.path.is_file() and self.path.stat().st_size != 0:
            if debug_mode:
                self.logger.debug(
                    f"{msg}'{str(self.path)}' already exists... SKIPPING AHEAD"
                )
            self.file_exists = True
        else:
            self.file_exists = False
            if debug_mode:
                self.logger.debug(f"{msg}unexpectedly missing a file | '{self.path}'")


@dataclass
class File:
    """
    Check for an existing file, before saving/opening multiple types of file formats.

    Attributes:
        path_to_file -- the absolute path to a file  
        cl_inputs -- an InputManager() object  
    """

    # required parameters
    path_to_file: Union[Path, str]
    cl_inputs: "InputManager"
    
    # optional parameters
    logger_msg: Union[str, None] = None

    # internal parameters
    file_exists: bool = field(default=False, init=False, repr=False)
    file_lines: List[str] = field(default_factory=list, init=False, repr=False)
    file_dict: Dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _existing_data: List[Dict[str,str]] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        self.path = Path(self.path_to_file)
        self.file_name = self.path.name
        self.path_only = self.path.parent
        
        if self.logger_msg is None and self.cl_inputs.logger_msg is not None:
            self.logger_msg = self.cl_inputs.logger_msg
            
        self._test_file = TestFile(self.path, self.cl_inputs.logger)

    def check_status(
        self,
        should_file_exist: bool = False
    ) -> None:
        """
        Confirm that the file is non-existent.
        """
        if should_file_exist is True:
            self._test_file.check_existing(logger_msg=self.logger_msg, debug_mode=self.cl_inputs.debug_mode)
        else:
            self._test_file.check_missing(
                logger_msg=self.logger_msg, debug_mode=self.cl_inputs.debug_mode
            )
        self.file_exists = self._test_file.file_exists

    def write_list(self, line_list: List[str]) -> None:
        """
        Take an iterable list of lines and write them to a text file.
        """
        if self.cl_inputs.dry_run_mode:
            if self.cl_inputs.logger_msg is None:
                self.cl_inputs.logger.info(
                    f"[DRY_RUN]: pretending to write a list of lines | '{str(self.path)}'"
                )
            else:
                self.cl_inputs.logger.info(
                    f"{self.cl_inputs.logger_msg}: pretending to write a list of lines | '{str(self.path)}'"
                )

            print("---------------------------------------------")
            for line in line_list:
                print(line)
            print("---------------------------------------------")
        else:
            with open(f"{self.path}", mode="a", encoding="UTF-8") as file:
                file.writelines(f"{line}\n" for line in line_list)

            # Confirm the expected number of lines was written
            with open(
                f"{self.path}/{self.file}", mode="r", encoding="UTF-8"
            ) as filehandle:
                self.file_lines = filehandle.readlines()

            assert len(line_list) == len(
                self.file_lines
            ), f"expected {len(line_list)} lines in {self.file}, but there were {len(self.file_lines)} found"

    def write_list_of_dicts(
        self, line_list: List[Dict[str, str]], delim: Union[str, None] = None
    ) -> None:
        """
        Take an iterable list of dictionaries and write them to a text file.
        """
        keys = line_list[0].keys()
        if delim is None:
            _delim = ","
        else:
            _delim = delim

        if self.cl_inputs.dry_run_mode:
            if self.cl_inputs.logger_msg is None:
                self.cl_inputs.logger.info(
                    f"[DRY_RUN]: pretending to write a list of dictionaries | '{str(self.path)}'"
                )
            else:
                self.cl_inputs.logger.info(
                    f"{self.cl_inputs.logger_msg}: pretending to write a list of dictionaries | '{str(self.path)}'"
                )

            print("---------------------------------------------")
            header = f"{_delim}".join(keys)
            print(header)
            for dict in line_list[0:10]:
                line = f"{_delim}".join([str(value) for value in dict.values()])
                print(line)
            print("---------------------------------------------")
        else:
            with open(f"{self.path}", mode="a", encoding="UTF-8") as file:
                dict_writer = DictWriter(file, fieldnames=keys, delimiter=_delim)
                dict_writer.writeheader()
                dict_writer.writerows(rowdicts=line_list)

    def add_rows(self, col_names: List[str], data_dict: Dict[str, str]) -> None:
        """
        Append rows to a csv.
        """
        if self.cl_inputs.dry_run_mode:
            print(",".join(data_dict.values()))
        else:
            if self.path.exists():
                if self.cl_inputs.debug_mode:
                    debug_msg = f"appending [{self.file}] with a new row"
                    if self.cl_inputs.logger_msg is None:
                        self.cl_inputs.logger.debug(debug_msg)
                    else:
                        self.cl_inputs.logger.debug(f"{self.cl_inputs.logger_msg}: {debug_msg}")

                with open(str(self.path), mode="a") as file:
                    dictwriter = DictWriter(file, fieldnames=col_names)
                    dictwriter.writerow(data_dict)
                    self.file_dict.update(data_dict)
            else:
                if self.cl_inputs.debug_mode:
                    debug_msg = f"initializing | '{self.file}'"
                    if self.cl_inputs.logger_msg is None:
                        self.cl_inputs.logger.debug(debug_msg)
                    else:
                        self.cl_inputs.logger.debug(f"{self.cl_inputs.logger_msg}: {debug_msg}")

                with open(str(self.path), mode="w") as file:
                    dictwriter = DictWriter(file, fieldnames=col_names)
                    dictwriter.writeheader()
                    dictwriter.writerow(data_dict)

                self.file_dict = data_dict

    def write_csv(self, write_dict: Dict[str, str]) -> None:
        """
        Save or display counts from [run_name]-[iteration]-[test_number] only.
        """
        # If only testing, display to screen.
        if self.cl_inputs.dry_run_mode:
            if self.cl_inputs.logger_msg is None:
                self.cl_inputs.logger.info(
                    f"[DRY_RUN]: pretending to write CSV file | '{str(self.path)}'"
                )
            else:
                self.cl_inputs.logger.info(
                    f"{self.cl_inputs.logger_msg}: pretending to write CSV file | '{str(self.path)}'"
                )

            print("---------------------------------------------")
            for key, value in write_dict.items():
                if type(value) is list:
                    v = ",".join(value)
                else:
                    v = value
                print(f"{key},{v}")
            print("---------------------------------------------")

        # Otherwise, write an intermediate CSV output file
        else:
            with open(str(self.path), mode="w") as file:
                write_file = writer(file)
                for key, value in write_dict.items():
                    if type(value) is list:
                        write_file.writerow([key] + value)
                    else:
                        write_file.writerow([key, value])

            if self.path.is_file():
                logging_msg = f"created intermediate CSV file | '{self.file}'"
                if self.cl_inputs.logger_msg is None:
                    self.cl_inputs.logger.info(logging_msg)
                else:
                    self.cl_inputs.logger.info(f"{self.cl_inputs.logger_msg}: {logging_msg}")

    def write_dataframe(self,
                        df: pd.DataFrame,
                        keep_index: bool = False,
                        keep_header: bool = True,
                        delim: str = ",") -> None:
        if delim == ",":
            _format = "CSV"
        elif delim == "\t":
            _format = "TSV"
        else:
            _format = f"'{delim}'-separated"
        
        if self.cl_inputs.dry_run_mode:
            self.cl_inputs.logger.info(
                f"{self.cl_inputs.logger_msg}: pretending to write {_format} file | '{str(self.path)}'"
            )

            print("---------------------------------------------")
            # Print the DataFrame without headers and without the index
            print(df.to_csv(sep=delim, header=keep_header, index=keep_index))
            print("---------------------------------------------")
        else:
            self.cl_inputs.logger.info(
                f"{self.cl_inputs.logger_msg}: writing a {_format} file | '{str(self.path)}'"
            )
            df.to_csv(
                str(self.path),
                doublequote=False,
                quoting=QUOTE_NONE,
                sep=delim,
                index=keep_index,
                header=keep_header,
            )
    
    def write_pickle(self, obj: Union["Genome", Dict[int, List[str]]]) -> None:
        """
        Save a dataclass object to a file, for use with downstream scripts.

        Parameters
        ----------
        obj : Type[&quot;Genome&quot;]
            custom dataclass containing all data for a specific sample
        """
        self._test_file.check_missing()

        if not isinstance(obj, dict):
            _msg = obj._log_msg
        else:
            _msg = self.cl_inputs.logger_msg

        if self.cl_inputs.dry_run_mode:
            if self._test_file.file_exists:
                _info = "pretending to re-write the existing"
            else:
                _info = "pretending to write a new"
        else:
            if self._test_file.file_exists:
                _info = "re-writing the existing"
            else:
                _info = "writing a new"
        
        self.cl_inputs.logger.info(
                f"{_msg}: {_info} pickle file | '{self._test_file.file}'"
            )
        
        if self.cl_inputs.dry_run_mode:
            return
        else:
            with open(str(self._test_file.path), "wb") as outp:
                dump(obj, outp, HIGHEST_PROTOCOL)

    def load_txt_file(self) -> None:
        """
        Read in and save a \n-delimited file as a list.
        """
        with open(str(self.path), mode="r", encoding="utf-8-sig") as data:
            for line in data.readlines():
                _clean_line = line.strip()
                self._existing_data.append(_clean_line)

    def load_csv(self) -> None:
        """
        Read in and save the CSV file as a dictionary.
        """
        if "gz" in self.path.suffix:
            import gzip

            logging_msg = f"handling a compressed file | '{self.path.stem}'"
            if self.cl_inputs.logger_msg is None:
                self.cl_inputs.logger.info(logging_msg)
            else:
                self.cl_inputs.logger.info(f"{self.cl_inputs.logger_msg}: {logging_msg}")
            with gzip.open(str(self.path), mode="rt") as data:
                reader = DictReader(data)
                self._existing_data = [dict(row) for row in reader]
        else:
            with open(str(self.path), mode="r", encoding="utf-8-sig") as data:
                reader = DictReader(data)
                self._existing_data = [dict(row) for row in reader]

    def load_tsv(self, header_list: List[str]) -> List[Dict[str, str]]:
        """
        Read in and save a TSV file as a list of lines.
        """
        if self._existing_data:
            reader = DictReader(
                self._existing_lines, fieldnames=header_list, delimiter="\t"
            )

            return [line for line in reader]
        else:
            list_of_line_dicts = []
            with open(str(self.path), mode="r", encoding="utf-8-sig") as data:
                reader = DictReader(data, fieldnames=header_list, delimiter="\t")
                for line in reader:
                    # If the file contains headers, skip them
                    contents = list(line.values())
                    if reader.fieldnames and any(
                        i in contents for i in reader.fieldnames
                    ):
                        if self.cl_inputs.debug_mode:
                            self.cl_inputs.logger.debug(
                                f"{self.cl_inputs.logger_msg}: SKIPPING HEADERS"
                            )
                        continue
                    else:
                        list_of_line_dicts.append(line)
            self._existing_data = list_of_line_dicts
            return list_of_line_dicts

    def load_vcf(self) -> None:
        """
        Read in and save a VCF file as a list of lines.
        """
        with open(str(self.path), mode="r", encoding="utf-8-sig") as data:
            Lines = data.readlines()
            for line in Lines:
                if line.startswith("##"):
                    self._vcf_header_lines.append(line.strip())
                elif line.startswith("#CHROM"):
                    self._vcf_header_lines.append(line.strip())
                    self._col_names = line.strip("#\n").split("\t")

        with open(str(self.path), mode="r", encoding="utf-8-sig") as data:
            reader = DictReader(data, fieldnames=self._col_names, delimiter="\t")
            for line in reader:
                # SKIP the VCF header lines saved previously
                if any(v.startswith("#") for v in line.values()):
                    continue
                self._list_of_line_dicts.append(line)

    def load_pickle(
        self,
    ) -> Union[Dict[int, List[str]], None]:
        """
        Load in a pickled file's contents.

        Returns
        -------
        Union[str, None]
            a pickled 'Genome' object
        """
        self._file.check_existing()

        if self._file._file_exists:
            with open(self._file.path, "rb") as inp:
                return load(inp)

    def load_json_file(self) -> None:
        """
        Open a JSON config file and save as a dictionary object.
        """
        with open(str(self.path), mode="r") as file:
            self.file_dict = json.load(file)
