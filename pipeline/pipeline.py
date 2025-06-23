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
    
    # internal parameters
    _job_ids: List[str] = field(default_factory=list, init=False, repr=False)
    _previously_run_samples: List[str] = field(default_factory=list, init=False, repr=False)
    _rerun_counter: int = field(default=0, repr=False, init=False)
    _skip_counter: int = field(default=0, init=False, repr=False)
    _starting_row: int = field(default=0, init=False, repr=False)
    _submitted_counter: int = field(default=0, init=False, repr=False)
    _resubmission_counter: int = field(default=0, init=False, repr=False)
    # _samples_file = None
    
    # _result: Union[str, int, None] = field(default=None, repr=False, init=False)
    # _samples_file: Union[None, str] = field(default=None, init=False, repr=False)

    # def subdivide_group(self, current_index: int, subset_size: int) -> bool:
    #     """
    #     Controls when to provide logging updates during large variant calling cohorts given to an iterative loop within all_genomes() .

    #     Parameters
    #     ----------
    #     current_index : int
    #         represents the current place within a group of samples
    #     subset_size : int
    #         defines where to create breaks within the iterator

    #     Returns
    #     -------
    #     bool
    #         True: when both numbers evenly divide
    #         False: when they do not evenly divide
    #     """
    #     if current_index % subset_size == 0:
    #         return True
    #     else:
    #         return False

    def all_genomes(self) -> None:
        """
        Loop through all unique genomes provided by the user.
        """
        # if self.pipeline_inputs.cl_inputs.args.check_samples:
        #     self._previously_run_samples = [dep for dep in self.pipeline_inputs.cl_inputs.args.check_samples.split(",")]

        # if self.pipeline_inputs.cl_inputs.args.chunk_start:
        #     self._starting_row = self.pipeline_inputs.args.chunk_start - 1

        for i, g in enumerate(self.pipeline_inputs._all_genomes.items()):
            if i < self._starting_row:
                continue
            
            for variant_caller in self.pipeline_inputs.variant_callers.keys():
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
                
                _updated_logger_msg = f"{_genome._log_msg} - [run_{variant_caller}]"
                # _genome_pickle.cl_inputs.logger_msg = _updated_logger_msg
                _genome._log_msg = _updated_logger_msg
                
                if self.pipeline_inputs.cl_inputs.debug_mode:
                    self.pipeline_inputs.cl_inputs.logger.debug(
                            f"{_updated_logger_msg}: FOUND GENOME '{lab_id}' @ ITERATION '{g[0]}'"
                        )
                
                # Create a temporary logger msg
                self.pipeline_inputs.cl_inputs.logger_msg = _updated_logger_msg
                
                # Check for the expected output file produced by a specific variant caller
                _default_output = _genome.pipeline_inputs.variant_callers[variant_caller]["default_output"]
                
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
                        
                    if self.pipeline_inputs.cl_inputs.args.group_size > 50:
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
                    else:
                        self._skip_counter += 1
                    
                    # Revert the logger message back to original
                    self.pipeline_inputs.cl_inputs.logger_msg = _original_logger_msg
                    print("END OF A SINGLE GENOME VARIANT CALLING")
                    breakpoint()
                    
                 

            

    #         ## CLEAN UP ANY INTERMEDIATE FILES NOT CLEANED DURING SBATCH
    #         if args.check_outputs:
    #             clean_files = CleanUp(genome=_genome)
    #             clean_files.remove_lg_intermediates()

    #         if pipe._skip_counter == 0 and pipe._rerun_counter == 0:
    #             _submitted_counter += 1

    #         _skip_counter += pipe._skip_counter
    #         _resubmission_counter += pipe._rerun_counter
    #         total = _skip_counter + pipe._rerun_counter + _submitted_counter

    #         halt_loop = pipe.break_into_chuncks(total, args.chunk_size)
    #         if halt_loop:
    #             iteration.inputs.logger.info(
    #                 f"{inputs.logger_msg}: number of samples submitted | {_submitted_counter}"
    #             )
    #             iteration.inputs.logger.info(
    #                 f"{inputs.logger_msg}: number of samples skipped | {_skip_counter}"
    #             )
    #             if iteration.inputs.args.check_outputs:
    #                 iteration.inputs.logger.info(
    #                     f"{inputs.logger_msg}: number of samples to re-run with CUE | {_resubmission_counter}"
    #                 )

    #             if _job_ids:
    #                 start = total - args.chunk_size
    #                 iteration.inputs.logger.info(
    #                     f"{inputs.logger_msg}: CURRENT CHUNK JOB IDS | {_job_ids[start:total]}"
    #                 )

    #             iteration.inputs.logger.info(
    #                 f"{inputs.logger_msg}: ITR={total}; WAITING HERE... press (c) to continue"
    #             )
    #             print("----------------------------------------------------------------")
    #             breakpoint()

    # if args.chunk_start:
    #     _skip_counter = _skip_counter + (int(args.chunk_start) - 1)

    # if _skip_counter == samples._total_num_genomes:
    #     logger.info(f"{inputs.logger_msg}: There are no SLURM jobs to submit.")
    # elif args.check_outputs:
    #     logger.info(
    #         f"{inputs.logger_msg}: {_resubmission_counter} samples need to be re-run."
    #     )
    #     if _resubmission_counter > 0:
    #         logger.info(
    #             f"{inputs.logger_msg}: Please update '--samples' to '{_samples_file}'"
    #         )
    # else:
    #     CheckJobs(
    #         iter=iteration,
    #         slurm_jobs_list=_job_ids,
    #         benchmarking_file=benchmarking_file,
    #     ).check_submission() 
    #         print("WHERE I LEFT OFF:")
    #         breakpoint()

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

    def process_genome(
        self,
        genome: "Genome",
        # prior_jobs: Union[List[Union[str, None]], None] = None
    ) -> None:
        """
        Submit a SLURM job that takes a BAM/CRAM as input to generate a VCF output.

        Args:
            prior_jobs (Union[List[Union[str, None]], None], optional): _description_. Defaults to None.
        """
        
        _default_output = genome.pipeline_inputs.variant_callers[genome._model_type]["default_output"]
        if self.pipeline_inputs.cl_inputs.overwrite:
            self.pipeline_inputs.cl_inputs.logger.info(
                    f"{self.pipeline_inputs.cl_inputs.logger_msg} --overwrite=True; re-writing the existing output file | '{_default_output.file_name}'"
            )
        
        genome.init_science()        
        genome.init_job()
        self._result = genome.submit_job()
        print("HALT!")
        breakpoint()
        
            # Uncomment for Cue
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