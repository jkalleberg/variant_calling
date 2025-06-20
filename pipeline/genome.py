#!/usr/bin/python3
"""
description: define a single unit for the generic variant calling pipeline

example usage: from pipeline.genome import Genome

"""
from dataclasses import dataclass, field
from pathlib import Path
from sys import path, exit
from typing import Dict, List, Union, TYPE_CHECKING
# from os import sched_getaffinity
# from sys import exit

if TYPE_CHECKING:   
    from input import PipelineInputManager
    from helpers.files import File

abs_path = Path(__file__).resolve()
module_path = str(abs_path.parent.parent)
path.append(module_path)

from helpers.utils import count_digits
from helpers.files import File, TestFile
# from pipeline.postprocess_vcf import PostProcessVCF

# from sbatch import SBATCH, SubmitSBATCH
from pipeline.science import Science



@dataclass
class Genome:
    """
    Contains the per-sample input files to give to a generic variant calling pipeline.
    """

    # required parameters
    sample: tuple
    pipeline_inputs: "PipelineInputManager"

    # optional parameters:
    group_name: Union[str, None] = None
    # trio_order: List[str] = field(default_factory=list, init=True)

    # internal parameters
    # _default_output: Union[None, "File"] = field(default=None, init=False, repr=False)
    # _missing_output: bool = field(default=True, init=False, repr=False)
    _model_type: Union[str, None] = field(default="DeepVariant", init=False, repr=False)
    _pickle_file: Union[None, File] = field(default=None, init=False, repr=False)
    _reads_path: Union[Path, None] = field(default=None, init=False, repr=False)
    _variables: Dict[str, str] = field(default_factory=dict, init=False, repr=False)
    
    # _check_dict: Dict[str, str] = field(default_factory=dict, init=False, repr=False)
    # # _chrom: Union[str, None] = field(default=None, init=False, repr=False)
    # _data_dict: Dict[str, Union[str, int, List[str]]] = field(
    #     default_factory=dict, init=False, repr=False
    # )
    # _final_genome: bool = field(default=False, init=False, repr=False)
    # _job_ids: List[Union[str, None]] = field(
    #     default_factory=list, init=False, repr=False
    # )
    # _jobid_found: Union[List[bool], None] = field(default=None, init=False, repr=False)
    
    # _model_dict: Dict[str, Union[str, int, List[str]]] = field(
    #     default_factory=dict, init=False, repr=False
    # )
    # _new_lines: List[str] = field(default_factory=list, init=False, repr=False)
    # _paths_found: List[Union[str, None]] = field(
    #     default_factory=list, init=False, repr=False
    # )
    
    # _region: Union[None, str] = field(default=None, init=False, repr=False)
    # _skip_counter: int = field(default=0, init=False, repr=False)
    # _submitting_jobs: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        
        self._sample_num = str(self.sample[0]).zfill(count_digits(self.pipeline_inputs._total_num_genomes))
        self._sample_id = self.sample[1][0]
        
        if self._sample_id is None:
            self._log_msg = f"{self.pipeline_inputs.cl_inputs.logger_msg}"
            _info = "UH OH!"
            print("HALT!")
            breakpoint()
        elif self.sample[1][1] is not None:
            self._reads_path = Path(self.sample[1][1])
            self._resources = self.pipeline_inputs.cl_inputs.resource_dict
            _info = "samples"
        else:
            _info = "trios"
        
        self._log_msg = f"{self.pipeline_inputs.cl_inputs.logger_msg} - [{self._sample_num}-of-{self.pipeline_inputs._total_num_genomes} {_info}]"
        
        # print(f"SAMPLE #: {self._sample_num}\nSAMPLE ID: {self._sample_id}")
        # print("LOG MSG:", self._log_msg)
        # breakpoint()
    
    def set_outputs(self, verbose: bool = False, format: str = "vcf") -> None:
        """
        Defines the expected file(s).
        """
        if "deepvariant" in self._model_type.lower():
            if "g." in format:
                _extension = "g.vcf.gz"
            else:
                _extension = "vcf.gz"
            
            _output = self._sample_dir / f"{self._sample_id}.{_extension}"
            
        elif "cue" in self._model_type.lower():
            _extension = "vcf"
            _output = self._reports_dir / f"svs.{_extension}"
        else:
            print("PESKY USER, WHY DO YOU WANT TO USE A DIFFERENT MODEL TYPE?")
            breakpoint()

        _default_output = File(
            path_to_file=_output,
            cl_inputs=self.pipeline_inputs.cl_inputs,
        )
        if self.pipeline_inputs.cl_inputs.overwrite:
            _default_output.check_status(should_file_exist=True)
        else:
            _default_output.check_status()

        if _default_output.file_exists:
            # Uncomment if adding the "check_outputs" flag
            # # If only "checking outputs", skip the remaining steps
            # if (
            #     "check_outputs" in self.pipeline_inputs.cl_inputs.args
            #     and self.pipeline_inputs.cl_inputs.args.check_outputs
            # ):
            #     return
            
            if verbose or self.pipeline_inputs.cl_inputs.debug_mode:
                self.pipeline_inputs.cl_inputs.logger.debug(
                    f"{self._log_msg}: found the default {_extension.upper()} file | '{_default_output.file_name}'"
                )
        
        # Uncomment for per-chr parallelization
        # elif (
        #         self.pipeline_inputs.cl_inputs.args.per_chr
        #         and len(self._paths_found) == self._num_chrs
        #     ):
        #         self.pipeline_inputs.cl_inputs.logger.info(
        #             f"{self._log_msg}: missing the 'bcftools concat' output file | '{_default_output.file_name}'"
        #         )
        else:
            self.pipeline_inputs.cl_inputs.logger.info(
                f"{self._log_msg}: missing the default {_extension.upper()} file | '{_default_output.file_name}'"
            )
        
        self.pipeline_inputs.cl_inputs.add_to_dict(
            update_dict=self.pipeline_inputs.variant_callers[self._model_type],
            new_key="default_output",
            new_val=_default_output,
        )
    
    def get_status(self) -> None:
        """
        Print info about the current genome.
        """
        if self._reads_path is not None:
            self.pipeline_inputs.cl_inputs.logger.info(
                f"{self._log_msg}: sample_id='{self._sample_id}' | raw_data='{self._reads_path}'"
            )
        
        # Uncomment to enable DeepTrio
        # else:
        #     self.parents = [x for x in self.trio_order if x != self._sample_id]
        #     self.pipeline_inputs.cl_inputs.logger.info(
        #         f"{self._log_msg}: child_id='{self._sample_id}' | parent_ids={self.parents}"
        #     )
