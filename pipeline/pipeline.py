#!/usr/bin/python3
"""
description: iterate through all samples provided by the user

example usage: from pipeline.pipeline import Pipeline

"""
from dataclasses import dataclass, field
from time import sleep
from random import random
from typing import List, Union, TYPE_CHECKING

if TYPE_CHECKING:   
    from input import PipelineInputManager

from pipeline.genome import Genome
from helpers.files import File

@dataclass
class Pipeline:
    """
    Contains the entire cohort for a generic variant calling pipeline.
    """

    # required parameters
    pipeline_inputs: "PipelineInputManager"

    # Optional parameters
    submit_size: int = 1

    # internal parameters
    _job_ids: List[str] = field(default_factory=list, init=False, repr=False)
    _skip_counter: int = field(default=0, init=False, repr=False)
    _starting_row: int = field(default=0, init=False, repr=False)
    _submitted_counter: int = field(default=0, init=False, repr=False)
    _result: Union[str, int, None] = field(default=None, repr=False, init=False)

    # _resubmission_counter: int = field(default=0, init=False, repr=False)
    # _previously_run_samples: List[str] = field(default_factory=list, init=False, repr=False)
    # _rerun_counter: int = field(default=0, repr=False, init=False)
    # _samples_file: Union[None, str] = field(default=None, init=False, repr=False)

    def subdivide_group(self, current_iter: int, subset_size: int) -> bool:
        """
        Controls when to provide logging updates during large variant calling cohorts given to an iterative loop within process_cohort() .

        Args:
            current_iter (int): 1-based value; the current place within a group of samples.
            subset_size (int): defines where to create breaks within the iterator

        Returns:
            bool: if True, both numbers evenly divide
        """
        if current_iter % subset_size == 0:
            return True
        else:
            return False

    def process_genome(
        self,
        genome: "Genome",
        get_help: bool = False,
    ) -> None:
        """
        Submit a SLURM job that takes a BAM/CRAM as input to generate a VCF output.

        Args:
            prior_jobs (Union[List[Union[str, None]], None], optional): _description_. Defaults to None.
        """
        if get_help:
            self.pipeline_inputs.cl_inputs.logger.info(
                f"{self.pipeline_inputs.cl_inputs.logger_msg}: --get-help=True; viewing internal flags within DeepVariant ({self.pipeline_inputs.get_help_with})"
            )
        else:
            _default_output = genome.pipeline_inputs._configs[genome._model_type][
                "default_output"
            ]

            _use_warning = False
            if self.pipeline_inputs.cl_inputs.dry_run_mode:
                if _default_output.file_exists:
                    if self.pipeline_inputs.cl_inputs.overwrite:
                        _info = "--overwrite=True, pretending to re-write the existing"
                    else:
                        _info = "--overwrite=False, pretending to re-write the existing"
                else:
                    _info = "pretending to expect the following"
            else:
                if (
                    _default_output.file_exists
                    and not self.pipeline_inputs.cl_inputs.overwrite
                ):
                    _info = "--overwrite=False, unable to re-write an existing"
                    _use_warning = True
                elif (
                    _default_output.file_exists
                    and self.pipeline_inputs.cl_inputs.overwrite
                ):
                    _info = "--overwrite=True, re-writing the existing"
                else:
                    _info = "expected new"

            if _use_warning:
                self.pipeline_inputs.cl_inputs.logger.warning(
                    f"{self.pipeline_inputs.cl_inputs.logger_msg}: {_info} output file | '{_default_output.file_name}'"
                )
            else:
                self.pipeline_inputs.cl_inputs.logger.info(
                    f"{self.pipeline_inputs.cl_inputs.logger_msg}: {_info} output file | '{_default_output.file_name}'"
                )

        genome.init_science(get_help=get_help)
        genome.init_job()
        self._result = genome.submit_job()

    def process_cohort(self) -> None:
        """
        Loop through all unique Genomes() provided by the user.
        """
        # if self.pipeline_inputs.cl_inputs.args.check_samples:
        #     self._previously_run_samples = [dep for dep in self.pipeline_inputs.cl_inputs.args.check_samples.split(",")]

        # Enable users to start & end the cohort intentionally
        if self.pipeline_inputs.cl_inputs.args.submit_start:
            self._starting_row = self.pipeline_inputs.cl_inputs.args.submit_start - 1
        else:
            self._starting_row = 0

        if self.pipeline_inputs.cl_inputs.debug_mode:
            self._ending_row = self._starting_row + 1
        # elif self.pipeline_inputs.cl_inputs.args.submit_stop >= 1 and self.pipeline_inputs.cl_inputs.args.submit_stop < self.pipeline_inputs._total_num_genomes:
        #     self._ending_row = self.pipeline_inputs.cl_inputs.args.submit_stop + 1
        else:
            self._ending_row = self.pipeline_inputs._total_num_genomes

        if self._ending_row <= self._starting_row:
            self.pipeline_inputs.cl_inputs.logger.error(
                f"{self.pipeline_inputs.cl_inputs.logger_msg}: the value for ending_row must be greater than or equal to '{self._starting_row:,}'.\nExiting... "
            )
            exit(1)

        # print("STARTING ROW:", self._starting_row)
        # print("ENDING ROW:", self._ending_row)
        # breakpoint()

        # How many times will variant calling be performed per-sample?
        _n_variant_callers = len(self.pipeline_inputs._configs.keys())

        # NOTE: This only works for DeepVariant currently!
        _total_jobs = (_n_variant_callers * self.pipeline_inputs._total_num_genomes)
        # self._job_ids = [None] * _total_jobs

        for i, g in enumerate(self.pipeline_inputs._all_genomes.items()):
            # print("I:", i)
            # print("STARTING ROW:", self._starting_row)
            # breakpoint()
            if i < self._starting_row:
                continue

            # Iterate through potentially multiple variant callers (e.g., DeepVariant, and Cue)
            for variant_caller in self.pipeline_inputs._configs.keys():
                _original_logger_msg = self.pipeline_inputs.cl_inputs.logger_msg 

                lab_id = g[1][0]

                # Uncomment if adding --check-samples arg
                # if self._previously_run_samples:
                #     self._skip_counter += 1
                #     if lab_id in self._previously_run_samples:
                #         self.pipeline_inputs.cl_inputs.logger.info(
                #             f"{_updated_logger_msg}: FOUND GENOME '{lab_id}' @ ITERATION '{g[0]}'"
                #         )
                #     continue

                _genome = Genome(
                    sample=g, pipeline_inputs=self.pipeline_inputs,
                )
                _genome.init_genome()

                # Edit the internal logging message
                _updated_logger_msg = f"{_genome._log_msg} - [run_{variant_caller}]"
                _genome._log_msg = _updated_logger_msg

                if self.pipeline_inputs.cl_inputs.debug_mode:
                    self.pipeline_inputs.cl_inputs.logger.debug(
                            f"{_updated_logger_msg}: FOUND GENOME '{lab_id}' @ ITERATION '{g[0]}'"
                        )

                # Create a temporary logger msg
                self.pipeline_inputs.cl_inputs.logger_msg = _updated_logger_msg

                # Check for the expected output file produced by a specific variant caller
                _default_output = _genome.pipeline_inputs._configs[variant_caller][
                    "default_output"
                ]

                # OUTPUT EXISTS!
                if _default_output.file_exists and not self.pipeline_inputs.cl_inputs.overwrite:
                    self.pipeline_inputs.cl_inputs.logger.info(
                        f"{_updated_logger_msg}: found all outputs for sample '{_genome._sample_id}'... SKIPPING AHEAD"
                    )
                    self._job_ids.insert(i, None)
                    self._skip_counter += 1

                # OUTPUT DOES NOT EXIST! OR USER WANTS TO RE-SUBMIT
                elif _default_output.file_exists is False or self.pipeline_inputs.cl_inputs.overwrite: 

                    # Identify the pickled data for a specific variant caller
                    if "deepvariant" in variant_caller.lower():
                        _pickle_dir = _genome._sample_dir
                    elif "cue" in variant_caller.lower():
                        _pickle_dir = _genome._reports_dir
                    else:
                        print("LOOK HERE")
                        breakpoint()

                    _genome_pickle = File(
                        path_to_file=_pickle_dir / f"{_genome._sample_id}.pkl",
                        cl_inputs=self.pipeline_inputs.cl_inputs,
                    )

                    # Save the 'Genome()' obj to a file for future use
                    _genome_pickle._test_file.check_missing()
                    _genome_pickle.write_pickle(obj=_genome)
                    _genome.check_pickle(input=_genome_pickle)

                    if self.submit_size > 50:
                        # Sleep for <1 second before submission to SLURM queue via process_genome()
                        # NOTE: helps to rate limit jobs submission within a second
                        sleep(random())

                    self.process_genome(genome=_genome)

                    # Uncomment to enable per-chr optimization
                    # if _genome._outputs._missing_any_outputs or self.pipeline_inputs.cl_inputs.overwrite:
                    # if _per_chr_jobids:
                    #     self.process_genome(prior_jobs=_per_chr_jobids)
                    # else:
                    #     self.process_genome()

                    if str(self._result).isnumeric() or self._result is None:
                        self._job_ids.insert(i, self._result)
                        self._submitted_counter += 1
                    else:
                        self._job_ids.insert(i, None)
                        self._skip_counter += 1

                    # Revert the logger message back to original
                    self.pipeline_inputs.cl_inputs.logger_msg = _original_logger_msg

            # After submitting variant calling for all model types,
            # determine if reporting a status update
            halt_loop = self.subdivide_group(
                current_iter=(i+1),
                subset_size=self.submit_size,
                )

            if halt_loop:
                if self.pipeline_inputs.cl_inputs.dry_run_mode:
                    _msg1 = "number of samples pretending to submit"
                    _msg2 = "number of samples pretending to skip"
                else:
                    _msg1 = "number of samples submitted"
                    _msg2 = "number of samples skipped"

                self.pipeline_inputs.cl_inputs.logger.info(
                    f"{self.pipeline_inputs.cl_inputs.logger_msg}: {_msg1} | {self._submitted_counter}"
                )
                self.pipeline_inputs.cl_inputs.logger.info(
                    f"{self.pipeline_inputs.cl_inputs.logger_msg}: {_msg2} | {self._skip_counter}"
                )

                # Uncomment for Cue
                # if iteration.inputs.args.check_outputs:
                #     self.pipeline_inputs.cl_inputs.logger.info(
                #         f"{self.pipeline_inputs.cl_inputs.logger_msg}: number of samples to re-run with CUE | {self._resubmission_counter}"
                #     )

                if self._job_ids:

                    # print("I:", i)
                    # print("ITER:", (i+1))
                    # print("JOB IDS:", self._job_ids[0:10])
                    # breakpoint()

                    # print("SUBMIT SIZE:", self.submit_size)
                    # print("SUBMIT START:", self.pipeline_inputs.cl_inputs.args.submit_start)
                    # print("STARTING ROW:", self._starting_row)

                    # print("TOTAL GENOMES:", self.pipeline_inputs._total_num_genomes)
                    # print("ENDING ROW:", self._ending_row)
                    # breakpoint()

                    if (i+1) == self.pipeline_inputs._total_num_genomes:

                        if self.pipeline_inputs.cl_inputs.args.submit_size > 1:
                            total = (
                                self._skip_counter + self._submitted_counter
                            )  # + self._rerun_counter??

                            # print("LOOK OVER HERE!")
                            # print("TOTAL:", total)
                            # print("START:", start)

                            start = total - self.submit_size
                            _current_job_ids = self._job_ids[start : (i + 1)]

                            pass
                        else:
                            # print("OVER HERE!")
                            # print("END:", _end)
                            _end = i + self.submit_size

                            _current_job_ids = self._job_ids[i:_end]
                    else: 
                        # print("NOPE, OVER HERE!")
                        _current_job_ids = self._job_ids[self._starting_row:(i+1)]

                    # print("JOB IDS:", self._job_ids)
                    # breakpoint()
                    # print("CURRENT JOB IDS:", _current_job_ids)
                    # breakpoint()

                    # Confirm that the submit size worked
                    self.pipeline_inputs.check_submission(
                        slurm_job_ids=_current_job_ids, n_expected=(self.submit_size * _n_variant_callers)
                    )

                    if (i+1) < self.pipeline_inputs._total_num_genomes:
                        self.pipeline_inputs.cl_inputs.logger.info(
                            f"{self.pipeline_inputs.cl_inputs.logger_msg}: submit_size={self.submit_size}; SBATCH job ids for current samples | {_current_job_ids}"
                        )

                        self.pipeline_inputs.cl_inputs.logger.info(
                            f"{self.pipeline_inputs.cl_inputs.logger_msg}: iteration={(i+1)}; waiting for you to press (c) to continue..."
                        )
                        print("----------------------------------------------------------------")
                        breakpoint()

        if self._skip_counter == _total_jobs:
            self.pipeline_inputs.cl_inputs.logger.info(
                f"{self.pipeline_inputs.cl_inputs.logger_msg}: there were no SLURM jobs to submit."
            )
        # elif args.check_outputs:
        #     logger.info(
        #         f"{inputs.logger_msg}: {_resubmission_counter} samples need to be re-run."
        #     )
        #     if _resubmission_counter > 0:
        #         logger.info(
        #             f"{inputs.logger_msg}: Please update '--samples' to '{_samples_file}'"
        #         )
        else:

            # Should only print a message if necessary
            self.pipeline_inputs.check_submission(
                slurm_job_ids=self._job_ids,
                n_expected=_total_jobs)

            if self.pipeline_inputs.cl_inputs.debug_mode:
                self.pipeline_inputs.cl_inputs.logger.debug(
                    f"{self.pipeline_inputs.cl_inputs.logger_msg}: SBATCH job ids for all samples | {self._job_ids}"
                )

# def handle_resubmissions(self) -> None:
#     """_summary_"""
#     # if "checking outputs" on a genome-wide basis...
#     if (
#         self.genome.iter.inputs.args.check_outputs
#         and not self.genome.iter.inputs.args.per_chr
#     ):
#         # then, create a new row in a new 'samples' file containing bam_paths for
#         # genomes that still need to be run with Cue
#         self.genome.save_new_inputs()
#         if self.genome.iter._samples_csv:
#             self._samples_file = str(self.genome.iter._samples_csv.file_path)
#             if not self.genome.iter._samples_csv._file._file_exists:
#                 self._rerun_counter = self.genome.iter._samples_csv._rows_added
#             else:
#                 self._rerun_counter = (
#                     self.genome.iter._samples_csv._existing_rows
#                     + self.genome.iter._samples_csv._rows_added
#                 )
#     else:
#         self._samples_file = None

# Process Genome -- Uncomment for Cue
# # if missing raw CUE results completely...
# # Only call svs when creating the default VCF
# if not _default_output._file_exists:
#     if not self.genome.iter.inputs.args.per_chr or (
#         self.genome.iter.inputs.args.per_chr and not self.genome._final_genome
#     ):
#         # NOTE: "Cue"-specific job lines are only included if default VCF is missing
#         self.genome.call_svs(region=self.genome._region)
#         self.genome._run_cue = True
#     else:
#         self.genome._run_cue = False
# elif (
#     _default_output._file_exists
#     and self.pipeline_inputs.cl_inputs.overwrite
# ):
#     self.genome.call_svs()
# else:
#     self.genome._run_cue = False

# # Only rename/sort/index if either final outputs are missing
# if (
#     not self.genome._outputs._indexed_vcf._file_exists
#     or not self.genome._outputs._compressed_vcf._file_exists
#     or self.pipeline_inputs.cl_inputs.overwrite
# ):
#     self.genome.wrangle_svs()
#
# if not self.genome._submitting_jobs:
#     self.handle_resubmissions()

# if self.genome.iter.inputs.args.per_chr and not self.genome._final_genome:
#     if self.genome._submitting_jobs:
#         self._result = self.genome.submit_job()
#         self.genome._outputs._command_line._job_cmd = []
#         self.genome._outputs._command_line._n_new_lines = 0
#     else:
#         if self._rerun_counter == 0:
#             self._skip_counter += 1
# else:
#     # Generate SV metrics files if they are missing
#     if (
#         self.genome._submitting_jobs
#         and self.genome._outputs._missing_any_outputs
#     ) or self.pipeline_inputs.cl_inputs.overwrite:
#         self.genome.process_svs()
#         self.genome.clean_tmp()
#         self._result = self.genome.submit_job(prior_jobs=prior_jobs)
#     elif self.genome._outputs._missing_any_outputs:
#         self.genome.process_svs(stream=True)
#         # self._result = self.genome.submit_job(prior_jobs=prior_jobs)
#     else:
#         if self._rerun_counter == 0:
#             self._skip_counter += 1
