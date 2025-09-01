#!/usr/bin/python3
"""
description: combine generic command-line inputs with pipeline-specific command-line inputs 

example usage: from pipeline.input import PipelineInputManager

"""

from dataclasses import dataclass, field
from pathlib import Path
from sys import exit
from os import getenv
from subprocess import run, CalledProcessError
from pandas import read_csv
from typing import Dict, List, Union, TYPE_CHECKING
from csv import DictReader
from regex import compile
import pandas as pd
from json import load

if TYPE_CHECKING:
    from helpers.inputs import InputManager
    from helpers.files import File

from helpers.files import TestFile, File
from helpers.wrapper import timestamp
from helpers.utils import check_if_all_same, find_NaN, find_not_NaN, partial_match_case_insensitive, check_if_all_same, iterdir_with_prefix


@dataclass
class PipelineInputManager:
    """
    Save the custom, user-provided inputs for repeated use.
    """
    # required parameters
    cl_inputs: "InputManager"

    # optional parameters
    get_help_with: str = "run_deepvariant"

    # internal parameters
    _all_genomes: Dict[int, List[str]] = field(default_factory=dict, init=False, repr=False)
    _benchmarking_file: Union[None, "File"] = field(default=None, init=False, repr=False)
    _chr_names: List[str] = field(default_factory=list, init=False, repr=False)
    _ckpt_paths: List[str] = field(
        default_factory=list, init=False, repr=False
    )
    _configs: Dict[str, Dict[str, str]] = field(
        default_factory=dict, init=False, repr=False
    )
    _get_help: bool = field(default=False, init=False, repr=False)
    _new_samples_csv: Union[None, "File"] = field(default=None, init=False, repr=False)
    _num_chrs: int = field(default=0, init=False, repr=False)
    _total_num_rows: int = field(default=0, init=False, repr=False) 
    _total_num_genomes: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.track_resources = self.cl_inputs.args.benchmark

    def load_model_configs(self) -> None:
        """
        Iterate through a list of JSON config file(s), and save the values as a list of dictionaries.
        """
        for config in self.cl_inputs.args.model_config:
            if self.cl_inputs.debug_mode:
                self.cl_inputs.logger.info(
                    f"{self.cl_inputs.logger_msg}: loading a new model config file | '{config}'"
                )
            with open(str(config), mode="r") as file:  # type: ignore
                _config_dict = load(file)

            # Confirm the minimum viable parameters are found in the config file
            _required_keys = ["model_type", "model_version", "checkpoint_prefix"]
            _required_keys_str = ",".join(_required_keys)
            assert set(_required_keys).issubset(
                _config_dict.keys()
            ), f"unexpected config file format; missing at least one required parameter ({_required_keys_str}) | '{config}'"

            _variant_caller = _config_dict["model_type"]
            _ckpt_path = _config_dict["checkpoint_prefix"]

            # Determine if user just wants the detailed manual for DeepVariant
            if _variant_caller.lower() == "deepvariant" and "get_help" in _config_dict.keys():
                self._get_help = _config_dict["get_help"]

            # Internally save the contents from multiple config files
            self.cl_inputs.add_to_dict(
                update_dict=self._configs,
                new_key=_variant_caller,
                new_val=_config_dict,
                replace_value=False,
                verbose=self.cl_inputs.debug_mode,
            )
            self.cl_inputs.add_to_dict(
                update_dict=self._configs[_variant_caller],
                new_key="config_path",
                new_val=config,
                replace_value=False,
                verbose=self.cl_inputs.debug_mode,
            )
            self._ckpt_paths.append(_ckpt_path)

    def check_model_configs(self) -> None:
        """
        Confirm that the contents of all config files meet expectations.
        """
        # Check for supported variant callers
        # NOTE: returns a list of matching strings if match is found
        _deepvariant_ckpt = partial_match_case_insensitive(
            "deepvariant", self._ckpt_paths
        )
        _deeptrio_ckpt = partial_match_case_insensitive("deeptrio", self._ckpt_paths)
        _cue_ckpt = partial_match_case_insensitive("cue", self._ckpt_paths)

        # Confirm at least one supported variant caller was provided
        _checkpoints_entered = ",".join(self._ckpt_paths)
        _no_valid_checkpoints = check_if_all_same([_deepvariant_ckpt, _cue_ckpt, _deeptrio_ckpt], None)
        assert (
            _no_valid_checkpoints is False
        ), f"unable to find a supported checkpoint (e.g., DeepVariant, DeepTrio or Cue) | '{_checkpoints_entered}'"

        # Get the expected default checkpoint path (custom bovid-trained WGS AF)
        _default_ckpt_prefix = Path(
            "./tutorial/existing_ckpts/DeepVariant/v1.4.0_withIS_withAF_bovid/model.ckpt-282383"
        ).resolve()

        # Create an empty list to store valid checkpoint paths
        _list_of_ckpt_prefixes = list()

        if _deepvariant_ckpt and len(_deepvariant_ckpt) == 1:

            # Get the value of 'BIN_VERSION_DV', return None if not set
            _dv_version = getenv("BIN_VERSION_DV")

            # Confirm this environment variable exists
            assert (
                _dv_version is not None
            ), f"missing [REQUIRED] environment variable: ($BIN_VERSION_DV); Please double check that this variable is included in your modules.sh file"

            # Do not allow the user to deviate from v1.4.0
            assert (
                _dv_version == "1.4.0"
            ), f"invalid environment variable ($BIN_VERSION_DV); Please edit your modules.sh file to use the expected version of DeepVariant (1.4.0)"
            # NOTE: In future, newer versions may become supported, but as they are untested, we do not encourage deviating from this expectation.

            # Identify the DeepVariant checkpoint prefix entered
            _user_ckpt_prefix = Path(_deepvariant_ckpt[0]).resolve()

            # Confirm that all the expected DeepVariant v1.4 checkpoint files are available
            _checkpoint_files = iterdir_with_prefix(
                absolute_path=_user_ckpt_prefix.parent,
                prefix=_user_ckpt_prefix.name,
                valid_suffixes=[".data-00000-of-00001", ".json", ".index", ".meta",],
                )

            assert (len(_checkpoint_files) == 4), f"unable to find all four DeepVariant checkpoint files | '{_user_ckpt_prefix.parent}'"

            # If all four expected files exist,
            # Save the new Path()
            self.cl_inputs.add_to_dict(
                update_dict=self._configs["deepvariant"],
                new_key="checkpoint_prefix",
                new_val=_user_ckpt_prefix,
                replace_value=True,
                verbose=self.cl_inputs.debug_mode,
            )

            # Determine if using the pipeline's default DeepVariant checkpoint (model.ckpt-282383),
            if _user_ckpt_prefix == _default_ckpt_prefix:

                # If so, make the pop_file config item [REQUIRED]
                assert (
                    "pop_file" in self._configs["deepvariant"].keys()
                ), "missing [REQUIRED] config value: pop_file; Please add a PopVCF to use the custom bovine-trained checkpoint (model.ckpt-282383)"

                # Resolve any relative path entered for pop_file
                _resolved_pop_path = Path(self._configs["deepvariant"]["pop_file"]).resolve()

                # Confirm the PopVCF file is available
                assert (_resolved_pop_path.is_file() is True), f"unable to find the PopVCF file | '{_resolved_pop_path}'"

                self.cl_inputs.add_to_dict(
                    update_dict=self._configs["deepvariant"],
                    new_key="pop_file",
                    new_val=_resolved_pop_path,
                    replace_value=True,
                    verbose=self.cl_inputs.debug_mode,
                )

                _list_of_ckpt_prefixes.append(_user_ckpt_prefix)

            else:
                print("ADD LOGIC FOR DIFFERENT DEEPVARIANT CHECKPOINTS")
                breakpoint()

        if _cue_ckpt:
            print("ADD LOGIC FOR CUE CHECKPOINT")
            breakpoint()

        if _deeptrio_ckpt:
            print("ADD LOGIC FOR DEEP TRIO CHECKPOINT")
            breakpoint()

        # Confirm that a model checkpoint was entered
        _entered_ckpts = ",".join(self._ckpt_paths)
        assert (len(_list_of_ckpt_prefixes) >= 1), f"unable to find at least one valid checkpoint | '{_entered_ckpts}'"

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
        Quickly confirm the number of rows in the input file.
        """
        try:
            if self.cl_inputs.debug_mode:
                self.cl_inputs.logger.debug(
                    f"{self.cl_inputs.logger_msg}: identifying the number of inputs provided.",
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
                    f"{self.cl_inputs.logger_msg}: number of potential files to process | {self._total_num_rows:,}",
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
        Confirm that all input files are pre-existing.
        
        Returns:
            None: indicates that the user-provided file could not be found.
            Dict[int, List[str]: indicates a valid BAM/CRAM file was provided:
                format = {row#: [sampleID, absolute path to an existing, indexed BAM/CRAM file]}
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
            return
        else:
            # Using lambda function to check if string contains any element from list
            _contains_word = lambda s, l: any(map(lambda x: x in s, l))

            valid_file_types = ["bam", "cram"]
            _valid_file_types_str = "/".join([f.upper() for f in valid_file_types])

            if _contains_word(file, valid_file_types):
                # Define the input file components to variables
                file_path = Path(str(file))
                file_parent = Path(file_path.parent)
                file_stem = file_path.stem
                file_ext = file_path.suffix.strip(".")

                try:
                    # Confirm the input file exists already
                    assert (
                        file_path.exists()
                    ), f"non-existent {file_ext.upper()} provided | '{file}'"

                    # Assuming the input file exists,
                    # Confirm user provided a BAM/CRAM input
                    if file_ext.lower() not in valid_file_types:
                        raise FileNotFoundError(f"invalid file type provided | '{file}'")

                    _validated_input = file_path

                except AssertionError:
                    if file_ext.lower() == valid_file_types[0]:
                        new_ext = valid_file_types[1]
                    elif file_ext.lower() == valid_file_types[1]:
                        new_ext = valid_file_types[0]
                    else:
                        self.cl_inputs.logger.warning(
                            f"{self.cl_inputs.logger_msg}: missing a valid file extension ({_valid_file_types_str}) for input file | '{file}'...SKIPPING AHEAD"
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

                        _validated_input = alternative_input

                    except AssertionError as e:
                        self.cl_inputs.logger.warning(e)
                        self.cl_inputs.logger.warning(
                            f"{self.cl_inputs.logger_msg}: unable to find input file with a valid extension ({_valid_file_types_str}) for sample | '{sample_id}'...SKIPPING AHEAD"
                        )
                        return
            else:
                self.cl_inputs.logger.warning(
                    f"{self.cl_inputs.logger_msg}: missing a valid file extension ({_valid_file_types_str}) for input file | '{file}'...SKIPPING AHEAD"
                )
                return

            # Assuming the input file is a BAM/CRAM file,
            # Define the required index file components as variables
            _validated_parent = Path(_validated_input.parent)
            _validated_stem = _validated_input.stem
            _validated_ext = _validated_input.suffix.strip(".")
            if _validated_ext.lower() == valid_file_types[0]:
                index_type = "bai"
            elif _validated_ext.lower().lower() == valid_file_types[1]:
                index_type = "crai"

            _index_file = _validated_parent / f"{_validated_stem}.{_validated_ext}.{index_type}"

            # Confirm the input file's index exists already
            try:
                assert (
                    _index_file.is_file()
                ), f"non-existent {index.upper()} provided | '{file}'" 
            except AssertionError as e:
                self.cl_inputs.logger.warning(e)
                self.cl_inputs.logger.warning(
                    f"{self.cl_inputs.logger_msg}: unable to find the index file for sample | '{sample_id}'"
                )
                print("TO DO: add a samtools sort + samtools index command here?")
                breakpoint()
                return

            # Assuming all inputs are valid, define the inputs to pass to Genome()
            return {index: [sample_id, str(_validated_input)]}

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
                    # print("INPUT PATH:", input_path)
                    
                    _name_contents = input_path.name.split(".")
                    
                    # print("NAME CONTENTS:", _name_contents)
                    # print("LENGTH:", len(_name_contents))
                    # breakpoint()
                    
                    if len(_name_contents) == 2:
                        match = _digits_only.search(input_path.name)
                        if match:
                            lab_id = match.group()
                        else:
                            lab_id = _name_contents[0]
                    else:
                        lab_id = _name_contents[0] 
                    
                    # print("LAB_ID:", lab_id)
                    # breakpoint()

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
            assert (
                self._total_num_genomes < max_limit
            ), f"exceeded max number of genomes to write to a single output directory | '{self._total_num_genomes:,}'>max_limit={max_limit:,}\nPlease split --input-path into multiple files contain fewer than {max_limit:,} samples."
        except AssertionError as err:
            self.cl_inputs.logger.exception(f"{err}\nExiting... ")
            exit(1)

    def check_submission(self,
                         slurm_job_ids: List[Union[str, None]],
                         n_expected: int = 1,
                         ) -> None:
        """
        Check if a batch of SBATCH files were submitted to the SLURM queue successfully.
        """
        nothing_submitted = check_if_all_same(slurm_job_ids, None)
        num_submitted = len(find_not_NaN(slurm_job_ids))
        num_skipped = len(find_NaN(slurm_job_ids))

        try:
            # Confirm at least one SLURM job id was detected
            assert ((num_submitted + num_skipped) == n_expected), f"expected {n_expected} SLURM jobs to be submitted, but received {len(slurm_job_ids)}"

            # DRY RUN MODE: will produce a fake 8-digit SLURM job id
            # if self.cl_inputs.dry_run_mode:
            #     assert (
            #         nothing_submitted is True
            #     ), f"expected nothing to be submitted, but at least one SLURM jobs was submitted"
            # else:
            assert nothing_submitted is False,  f"expected at least one SLURM jobs to be submitted" 
        
        except AssertionError as err:
            self.cl_inputs.logger.error(
                f"{self.cl_inputs.logger_msg}: fatal error encountered, unable to proceed further with pipeline.",
            )
            self.cl_inputs.logger.error(
                f"{self.cl_inputs.logger_msg}: {err}.\nExiting... ")
            exit(1)
