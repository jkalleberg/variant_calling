#!/usr/bin/python3
"""
description: combine generic command-line inputs with pipeline-specific command-line inputs 

example usage: from pipeline.input import PipelineInputManager

"""

from dataclasses import dataclass, field
from pathlib import Path
from sys import exit
from subprocess import run, CalledProcessError
from pandas import read_csv
from typing import Dict, List, Union, TYPE_CHECKING
from csv import DictReader
from regex import compile
import pandas as pd

if TYPE_CHECKING:
    from helpers.inputs import InputManager
    from helpers.files import File

from helpers.files import TestFile, File
from helpers.wrapper import timestamp


@dataclass
class PipelineInputManager:
    """
    Save the custom, user-provided inputs for repeated use.
    """
    # required parameters
    cl_inputs: "InputManager"
    variant_callers: Dict[str, Dict[str, str]]

    # internal parameters
    _all_genomes: Dict[int, List[str]] = field(default_factory=dict, init=False, repr=False)
    _chr_names: List[str] = field(default_factory=list, init=False, repr=False)
    _num_chrs: int = field(default=0, init=False, repr=False)
    _total_num_rows: int = field(default=0, init=False, repr=False) 
    _total_num_genomes: int = field(default=0, init=False, repr=False)
    _new_samples_csv: Union[None, "File"] = field(default=None, init=False, repr=False)
    _benchmarking_file: Union[None, "File"] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.track_resources = self.cl_inputs.args.benchmark
   
    def find_ref_dict(self) -> None:
        """
        Locate a PICARD reference genome .DICT file in the same directory as cl_inputs.args.ref_file (prefix only).
        
        If missing, create a new file to identify chromosome names agnostic of species.
        """          
        picard_dict_name = f"{self.cl_inputs.args.ref_file.stem}.dict"
        picard_dict_path = self.cl_inputs.args.ref_file.parent / picard_dict_name
        
        ref_dict = TestFile(
            file=picard_dict_path,
            logger=self.cl_inputs.logger)
        
        ref_dict.check_existing()
        
        if not ref_dict.file_exists:
            try:
                self.cl_inputs.logger.info(
                    f"{self.cl_inputs.logger_msg}: missing the reference .dict file; creating one now...",
                )
                # Creating a .dict file with Picard
                picard = run(
                    [
                        "picard",
                        "CreateSequenceDictionary",
                        "--REFERENCE",
                        f"{str(self.cl_inputs.args.ref_file)}",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
            except CalledProcessError as err:
                self.cl_inputs.logger.error(
                    f"{self.cl_inputs.logger_msg}: unable to create a reference .dict file",
                )
                self.cl_inputs.logger.error(f"{err}\n{err.stderr}\nExiting... ")
                exit(err.returncode)

            ref_dict.check_existing()
            if ref_dict.file_exists:
                    self._ref_dict_file = ref_dict
            else:
                raise FileNotFoundError(
                    f"{self.cl_inputs.logger_msg}: missing a dictionary file for the reference genome | '{str(self.cl_inputs.args.ref_file)}'"
                )
        else:
            self._ref_dict_file = ref_dict
            if self.cl_inputs.debug_mode:
                self.cl_inputs.logger.debug(
                    f"{self.cl_inputs.logger_msg}: found a PICARD reference dictionary... SKIPPING AHEAD",
                    )
    
    def transform_dictionary(self, exclude_chrs_list: Union[None, List[str]] = None) -> None:
        """
        Transform the PICARD reference genome's .DICT file into a pandas DataFrame for easier manipulations.
        
        NOTE: For human genomes, use exclude_chrs_list=["M", "EBV"]
        """
        input_data = read_csv(
            self._ref_dict_file.file,
            sep="\t",
            skipinitialspace=True,
            skiprows=1,
            header=None,
            usecols=[1, 2],
        )
        
        # Identify the chromosome/scaffolds naming conventions of the reference genome
        chromosome_names = input_data[1].str.split(":", n=1, expand=True)[1] # a pd.Series()
        
        if exclude_chrs_list is None:
            exclude_chrs_list = list()

        # TO DO: Make 'unmapped_reads' a list? or perhaps go back to "ignore_chrs" flag?
        if self.cl_inputs.args.unmapped_reads and self.cl_inputs.args.unmapped_reads not in exclude_chrs_list: 
            exclude_chrs_list.append(self.cl_inputs.args.unmapped_reads)

        # test for 'chr' in chromosome names
        name_test = chromosome_names.str.match(pat='chr', case=False)
        if len(chromosome_names) == sum(name_test):
            chr_to_exclude = [f"chr{x}" if "chr" not in x else x for x in exclude_chrs_list]
        else:
            chr_to_exclude = exclude_chrs_list
        
        # Only exclude chromosomes that match expectations based on the Reference .dict file
        for e in chr_to_exclude:
            find_exclude = chromosome_names.str.match(pat=e, case=False)
            if sum(find_exclude) == 0:
                chr_to_exclude.remove(e)
        
        # Construct the regex pattern
        pattern = '|'.join(chr_to_exclude)

        # Create a boolean mask for rows NOT containing partial matches to excluded chromosome names
        mask = chromosome_names.str.contains(pattern, case=False) # case=False for case-insensitive matching

        # Get the values based on the mask
        self._chr_names = chromosome_names[~mask]
        
        # Get the index values based on the mask
        matching_indices = chromosome_names.index[~mask]
        
        # self._chr_names = [
        #     chr
        #     for chr in chromosome_names
        #     if chr.isalnum() and chr not in chr_to_exclude
        # ]
        self._num_chrs = len(self._chr_names)
        
        # Set the start position to be 0 for each chromosome
        start_pos = [0] * self._num_chrs

        # Determine the end length of the chromosomes, 
        # based on the reference .dict file        
        chrs_length = pd.to_numeric(
            input_data[2].str.split(":", n=1, expand=True)[1]
        )
        
        # Only include rows with index values matching the valid chromosomes rows
        filtered_end = chrs_length[matching_indices]
        # filtered_chrs_length = chrs_length[:self._num_chrs])       

        # Create a default regions file with only valid chromosomes
        self._default_BED_data = pd.concat(
            {
                "chromosome": pd.Series(self._chr_names),
                "start": pd.Series(start_pos),
                "stop": filtered_end,
            },
            axis=1,
        )
    
    def default_regions_BED(
        self,
    ) -> None:
        """
        Produces the a default regions file to automatically exclude the unmapped contigs during whole genome calling variants
        """
        # NOTE: when using the new Cattle model, unable to genotype all unmapped contigs due to file num limits for DV.
        # Therefore, by default, ignore the unmapped contigs using a default regions file, either created or existing.
        # NOTE: when scaling this up thousands of samples, split up jobs per chromosome, rather than entire genome, 
        # "have the list of unmapped contig names in an array and do "interval" operations on chunks of say 50 contigs
        # in single threaded processes" like Bob does with GATK
        
        _default_bed_path = self.cl_inputs.args.ref_file.parent
        _default_bed_prefix = self.cl_inputs.args.ref_file.stem 
        _default_BED_path = _default_bed_path / f"{_default_bed_prefix}.bed"

        self._default_BED_file = File(
            path_to_file = _default_BED_path,
            cl_inputs = self.cl_inputs,
        )
        self._default_BED_file.check_status()

        if not self._default_BED_file.file_exists:
            if self.cl_inputs.debug_mode:
                self.cl_inputs.logger.debug(
                    f"{self.cl_inputs.logger_msg}: creating the default BED file now..."
                )
            
            self._default_BED_file.write_dataframe(
                df=self._default_BED_data,
                keep_index=False,
                keep_header=False,
                delim="\t",
            )
            self._default_BED_file.check_status()
            if not self.cl_inputs.dry_run_mode and not self._default_BED_file.file_exists:
                self.cl_inputs.logger.error(
                    f"{self.cl_inputs.logger_msg}: missing the required default regions' BED file | '{self._default_BED_file.path}'\nExiting..."
                )
                exit(1)
        else:
            self.cl_inputs.logger.info(
                f"{self.cl_inputs.logger_msg}: found default regions' BED file | '{self._default_BED_file.path}'"
            )
    
    def create_benchmarking_file(self) -> None:
        """
        Save SLURM job ids from custom pipeline to a new file, for benchmarking compute/wall time.
        """ 
        # If tracking resources used from SLURM jobs,
        if self.cl_inputs.args.benchmark:
            _todays_date = timestamp(date_only=True)
            
            # create a file to store metrics
            self._benchmarking_file = File(
                path_to_file=Path(self.cl_inputs._input_path.parent)/ "tmp" / f"{_todays_date}_SLURM_job_numbers.csv",
                cl_inputs=self.cl_inputs,
            )
            self._benchmarking_file.check_status()
    
    def create_new_sample_file(self) -> None:
        """
        Confirm if a temporary samples file for today is pre-existing.
        
        Used to create a new file to save samples' needing re-processing.
        """
        _todays_date = timestamp(date_only=True)

        self._new_samples_csv = File(
            path_to_file=Path(self.cl_inputs._input_path.parent) / "tmp" / f"{_todays_date}_samples.csv",
            cl_inputs=self.cl_inputs,
        )
        self._new_samples_csv.check_status()
    
    def find_pickled_samples(self, expect_existing: bool = False) -> None:
        """
        Identify if sample processing is necessary.
        """
        _sample_file_name = self.cl_inputs._input_path.stem
        _sample_file_path = Path(self.cl_inputs._input_path.parent)
        
        self._samples_pickle = File(
            path_to_file=_sample_file_path / f"{_sample_file_name}.pkl",
            cl_inputs=self.cl_inputs,
        )
        self._samples_pickle.check_status(should_file_exist=expect_existing)
    
    def count_inputs(self) -> None:
        """
        Quickly confirm the number of BAM/CRAM rows in the input file.
        """
        try:
            if self.cl_inputs.debug_mode:
                self.cl_inputs.logger.debug(
                    f"{self.cl_inputs.logger_msg}: identifying the number of BAM/CRAM inputs provided.",
                )
            # Creating a .dict file with Picard
            _result = run(
                [
                    "wc",
                    "-l",
                    f"{self.cl_inputs._input_path}",
                ],
                capture_output=True,
                text=True,
                check=True,)
            
            self._total_num_rows = int(_result.stdout.split(" ")[0])
            
            self.cl_inputs.logger.info(
                    f"{self.cl_inputs.logger_msg}: number of potential BAM/CRAM files to process | {self._total_num_rows:,}",
                )
                
        except CalledProcessError as err:
            self.cl_inputs.logger.error(
                f"{self.cl_inputs.logger_msg}: unable to open the input file | {self.cl_inputs._input_path}",
            )
            self.cl_inputs.logger.error(f"{err}\n{err.stderr}\nExiting... ")
            exit(err.returncode)
    
    def load_samples(self) -> None:
        """
        Re-use a previously saved 'PipelineInputManager._all_genomes()' object.
        """
        _all_genomes_pickled = self._samples_pickle.load_pickle()
        if _all_genomes_pickled and isinstance(_all_genomes_pickled, dict):
            self._all_genomes = _all_genomes_pickled
            self._total_num_genomes = len(self._all_genomes)
    
    def save_samples(self) -> None:
        """
        Save the 'PipelineInputManager._all_genomes()' object to a file for future use.
        """
        if self._all_genomes:
            if self.cl_inputs.overwrite:
                _status = "--overwrite=True; "
            else:
                _status = ""
            self.cl_inputs.logger.info(f"{self.cl_inputs.logger_msg}: {_status}saving all_genomes() as a pickle file") 
            self._samples_pickle.save_pickle(obj=self._all_genomes)
    
    def process_samples(self) -> None:
        """
        Determine if the input file needs to be opened, or if the pickled input file can be re-used.
        """
        if self.cl_inputs.overwrite:
            self.find_pickled_samples(expect_existing=True)
        else:
            self.find_pickled_samples(expect_existing=False)
        
        if self._samples_pickle._file._file_exists and not self.cl_inputs.overwrite:
            self.load_samples()
            self.cl_inputs.logger.info(f"{self.cl_inputs.logger_msg}: loaded {self._total_num_genomes:,} samples from pickled file | '{self._samples_pickle.file_path}'")
        else:
            self.process_input_file()
            self.save_samples()
    
    def check_sample(self, index: int, sample_id: str, file: str, manual_review: bool = False) -> Union[Dict[int, List[str]], None]:
        """
        Confirm that all input BAM/CRAM files are pre-existing.
        
        Returns:
            None: indicates that the user-provided BAM/CRAM file provided does not exist.
            Dict[int, List[str]: indicates a valid BAM/CRAM file was provided:
                format = {row#: [sampleID, absolute path to existing BAM/CRAM file(s) from the CSV input]}
        """
        if manual_review and self.cl_inputs.debug_mode:
            get_status = 100
        else:
            get_status = 1000

        if (index % get_status == 0) or (index % self._total_num_rows == 0):
            self.cl_inputs.logger.info(
                f"{self.cl_inputs.logger_msg}: checking input format for {(index)}-of-{self._total_num_rows} rows"
            )

        if isinstance(file, str) and file == "None":
            self.cl_inputs.logger.error(
                f"{self.cl_inputs.logger_msg}: missing input file for sample | '{sample_id}'"
            )
        else:
            # Using lambda function to check if string contains any element from list
            _contains_word = lambda s, l: any(map(lambda x: x in s, l))
            
            valid_file_types = ["bam", "cram"]
            
            if _contains_word(file, valid_file_types):
                file_path = Path(str(file))
                file_parent = Path(file_path.parent)
                file_stem = file_path.stem
                file_ext = file_path.suffix.strip(".")

                try:
                    assert (
                        file_path.exists()
                    ), f"non-existent {file_ext.upper()} provided | '{file}'"
                    
                    return {index: [sample_id, str(file)]}
                
                except AssertionError:
                    if file_ext.lower() == valid_file_types[0]:
                        new_ext = valid_file_types[1]
                    elif file_ext.lower() == valid_file_types[1]:
                        new_ext = valid_file_types[0]
                    else:
                        self.cl_inputs.logger.warning(
                            f"{self.cl_inputs.logger_msg}: missing a valid file extension {valid_file_types} for input file | '{file}'...SKIPPING AHEAD"
                        )
                        return

                    # Determine if only the file extension is bad (e.g., BAM when really CRAM)
                    alternative_input = file_parent / f"{file_stem}.{new_ext}"
                    try:
                        assert (
                            alternative_input.exists()
                        ), f"non-existent {file_ext.upper()} provided | '{file}'"
                        
                        if manual_review and self.cl_inputs.logger.debug:
                            self.cl_inputs.logger.debug(
                                f"{self.cl_inputs.logger_msg}: updating file extension | '{file_path.name}' -> '{alternative_input.name}'\nPress (c) to continue:"
                            )
                            breakpoint()
                        return {index: [sample_id, str(alternative_input)]}
                    
                    except AssertionError as e:
                        self.cl_inputs.logger.warning(e)
                        self.cl_inputs.logger.warning(
                            f"{self.cl_inputs.logger_msg}: unable to find input file with either BAM/CRAM extension for sample | '{sample_id}'"
                        )
                        return
            else:
                self.cl_inputs.logger.warning(
                    f"{self.cl_inputs.logger_msg}: missing a valid file extension {valid_file_types} for input file | '{file}'...SKIPPING AHEAD"
                )
                return
    
    def process_input_file(self) -> None:
        """
        Convert the user-provided .CSV file into dict with each line in the file.
        
        Loop through the user-provided CSV file, and confirm each row's BAM/CRAM file exists.
        
        Note: Used internally by process_samples() only.
        """
        _digits_only = compile(r"\d+")
        try:
            with open(
                str(self.cl_inputs._input_path), mode="r", encoding="utf-8-sig"
            ) as csv_file:
                csv_reader = DictReader(csv_file, fieldnames=["file_path"])
                current_genome_num = 0
                
                for row in csv_reader:
                    if list(row.keys()) == list(row.values()):
                        continue
                    else:
                        current_genome_num += 1

                    # Identify the unique numeric sample identifier
                    input_path = Path(row["file_path"])
                    match = _digits_only.search(input_path.name)
                    if match:
                        lab_id = match.group()
                    else:
                        lab_id = input_path.name.split(".")[0]

                    self._total_num_genomes += 1
                    contents = [lab_id] + list(row.values())
                    
                    # If the user provides headers, skip them
                    if csv_reader.fieldnames and any(
                        i in contents for i in csv_reader.fieldnames
                    ):
                        continue
                    
                    # Uncomment to view original contents:
                    # self._all_genomes[current_genome_num] = contents
                    
                    # Handle relative paths, if entered
                    _abs_file_path = Path(contents[1]).resolve()
                    
                    # Confirm each row contains a valid BAM/CRAM file
                    # NOTE: a row with a non-existent file will be ignored!
                    _valid_input = self.check_sample(
                        index=current_genome_num,
                        sample_id=contents[0],
                        file=str(_abs_file_path),
                        # manual_review=True, # Uncomment to see what files were changed
                        )
                    
                    # If check_samples() works, then save the final output
                    if _valid_input is not None:
                        for k,v in _valid_input.items():
                            self._all_genomes[k] = v
                
                assert (
                    self._all_genomes
                ), f"unable to load in input data from file | '{self.cl_inputs._input_path}'"

        except AssertionError as err:
            self.cl_inputs.logger.exception(f"{err}\nExiting... ")
            exit()

        self.find_unique_samples()
        self.check_num_genomes()

    def find_unique_samples(self) -> None:
        """
        Skip over any duplicate samples provided by the user.
        """
        temp = {val[0]: [key, val[1]] for key, val in self._all_genomes.items()}
        unique_data = {val[0]: [key, val[1]] for key, val in temp.items()}
        new_num_genomes = len(unique_data)

        diff = self._total_num_rows - new_num_genomes
        if diff != 0:
            self.cl_inputs.logger.warning(
                f"{self.cl_inputs.logger_msg}: skipping {diff} duplicated inputs"
            )
            self._total_num_genomes = new_num_genomes
            self._all_genomes = unique_data
    
    def check_num_genomes(self, max_limit: int = 10000) -> None:
        """
        Prevents the user from creating 10,000+ sub-directories within a single output path.
        """
        try:
            assert (self._total_num_genomes < max_limit), f"exceeded max number of genomes to write to a single output directory | '{self._total_num_genomes:,}'>max_limit={max_limit:,}"
        except AssertionError as err:
            self.cl_inputs.logger.exception(f"{err}\nExiting... ")
            exit() 