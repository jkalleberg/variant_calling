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
