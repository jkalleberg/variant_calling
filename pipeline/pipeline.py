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
                _updated_logger_msg = f"{self.pipeline_inputs.cl_inputs.logger_msg} - [run_{variant_caller}]"
                lab_id = g[1][0]
                self.pipeline_inputs.cl_inputs.logger.info(
                        f"{_updated_logger_msg}: FOUND GENOME '{lab_id}' @ ITERATION '{g[0]}'"
                    )

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
                
                # Check for the expected output file produced by a specific variant caller
                _default_output = _genome.pipeline_inputs.variant_callers[variant_caller]["default_output"]
                
                # if (
                    #     # not _genome._missing_output
                    #     # Uncomment for Cue
                    #     # and not _genome._outputs._missing_any_outputs
                    #     and not self.pipeline_inputs.cl_inputs.args.overwrite
                    # ):
                
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
                
                if _default_output.file_exists and not self.pipeline_inputs.cl_inputs.overwrite:
                    # OUTPUT EXISTS!
                    self.pipeline_inputs.cl_inputs.logger.info(
                        f"{_updated_logger_msg}: found all outputs for sample '{_genome._sample_id}'... SKIPPING AHEAD"
                    )
                    self._skip_counter += 1
                elif _default_output.file_exists is False or self.pipeline_inputs.cl_inputs.overwrite: 
                    # OUTPUT DOES NOT EXIST!
                    
                    # Save the 'Genome()' obj to a file for future use
                    _genome_pickle._test_file.check_missing()
                    _genome_pickle.write_pickle(obj=_genome)
                    _genome.check_pickle(input=_genome_pickle)
                else:
                    print("LOGIC FAILURE")
                    breakpoint()
                    
                if self.pipeline_inputs.cl_inputs.args.group_size > 50:
                    # Sleep for <1 second before submission to SLURM queue via process_genome()
                    # NOTE: helps to rate limit jobs submission within a second
                    sleep(random())
                
                print("WHERE I LEFT OFF:")    
                # print("GENOME:", _genome)
                # breakpoint()
                self.process_genome(genome=_genome)
                breakpoint()
                
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
        if self.pipeline_inputs.cl_inputs.overwrite:
            self.pipeline_inputs.cl_inputs.logger.info(
                    f"{self.pipeline_inputs.cl_inputs.logger_msg} --overwrite=True; re-writing the existing output file | '{self.genome._default_vcf.file}'"
            )
        
        genome.init_science() 
        # self._result = genome.submit_job()
        breakpoint()

        
            # Uncomment for Cue
            # # if missing raw CUE results completely...
            # # Only call svs when creating the default VCF
            # if not self.genome._default_vcf._file_exists:
            #     if not self.genome.iter.inputs.args.per_chr or (
            #         self.genome.iter.inputs.args.per_chr and not self.genome._final_genome
            #     ):
            #         # NOTE: "Cue"-specific job lines are only included if default VCF is missing
            #         self.genome.call_svs(region=self.genome._region)
            #         self.genome._run_cue = True
            #     else:
            #         self.genome._run_cue = False
            # elif (
            #     self.genome._default_vcf._file_exists
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